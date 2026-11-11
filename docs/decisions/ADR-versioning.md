# ADR — Inventory Versioning and Branching (Iceberg-native)

**ID:** ADR-versioning
**Status:** Accepted
**Date:** 2026-11-10
**Builds on:** ADR-nsi-minimal (replaces its "Bitemporal identity + branching" deferral)
**Serves user stories:** RQM-02, RQM-03, RQM-11, RQM-12, RQM-16
**Implementation:** `tools/schema.sql`, `tools/ingest_nsi.py`, `tools/ingest_source.py`, `tools/versioning.py`, `tools/rqm_version.py`
**Tests / demo:** `test/test_versioning.py`, `notebooks/02_versioning_demo.ipynb`

## Context

ADR-nsi-minimal stores one current state per table: `building_id` (= NSI `fd_id`) is the identity, and `ingested_at` was "the entire version history". The loads appended, so loading a second, modified dataset duplicated every building. The user stories need more:

- **RQM-02** — a structure keeps its identity when its location, attributes, or geometry change, and earlier states are not overwritten.
- **RQM-03** — project inventories stay linked to the inventory they came from; versions can be compared, and an analysis can name and reproduce the inventory it used.
- **RQM-11** — several people edit in parallel; conflicts are resolved logically and only changes are stored.
- **RQM-12 / RQM-16** — each edit records who made it, when, and why.

The hybrid exploration (`feature/hybrid_data_model`) answered this with five tables: `asset`, `asset_revision`, `inventory_branch`, `revision_log`, and a bitemporal `versioning` registry. ADR-nsi-minimal deferred that bundle as over-normalized. The tables join with no payload of their own, and they reimplement snapshots and branches that the storage layer already has.

## Decision

**Use Apache Iceberg's own snapshots, tags, and branches for versioning. Add two small tables for attribution and per-building change history.**

### 1. Loads are full-replacement MERGE upserts

`load_buildings` and `load_geometries` `MERGE` the input into the target branch instead of appending:

| Input row vs. table | Action |
|---|---|
| New `building_id` | insert |
| Same `building_id`, any compared column differs (NULL-safe) | update, with a fresh `ingested_at` |
| Same `building_id`, identical | **no write** |
| `building_id` missing from the input | delete from the branch head (earlier versions keep it) |

The input is the complete inventory **of its source** for the branch. Since [ADR-multi-source](ADR-multi-source.md), the delete is scoped to `buildings.source_ref`, so a load never touches other sources' rows. Geometries follow the same rules, keyed on the stable `geometry_id`:

- Only the point and CRS are compared; `is_selected_for_export` is never overwritten, so a selection migration survives a reload.
- Only the loading source's own geometries (`source_ref` = `<source>` or `<source>-<subcode>`, e.g. `NSI-P`) of its own buildings are deleted when they are missing from the input.
- Geometries from any source are deleted when their building is gone.

Identity is unchanged: `building_id` = `fd_id` for NSI (a stable hash of the native id for other sources), and `geometry_id` = hash of the natural key, so the same structure has the same ids in every version (RQM-02).

`buildings` and `geometries` use **merge-on-read** writes (`write.{delete,update,merge}.mode`). A version stores only the rows that changed, plus Iceberg v3 deletion vectors that mask the rows they replace. On the 45,622-row sample, v2 (205 changed buildings) adds 165 records and one deletion vector to `buildings`, not a 45,597-row copy (RQM-11, minimal footprint).

### 2. A version is a tag on both tables plus a registry row

`commit_version(name, editor, reason, ...)` records a branch head as a named version:

1. Resolve the **parent**: by default, the nearest registered version in the branch's snapshot history. That is the previous version on `main`, or the version a branch was created from.
2. Write the per-building **changes** relative to the parent into `inventory_changes`.
3. Create an Iceberg **tag** named after the version on `buildings` and `geometries`, pointing at the head snapshots.
4. Insert the **registry** row into `inventory_versions` — **last**.

A version exists only once its registry row exists. Because the tables commit independently, a crash between steps leaves at most a tag with no registry row. `check_consistency` reports it, and the next commit of the same name replaces it.

Any version can be read with plain SQL, `SELECT … FROM rqm.inventory.buildings VERSION AS OF 'nsi-2025'`, or with `read_version` (RQM-03).

### 3. New tables

| Table | Grain | Purpose |
|---|---|---|
| `inventory_versions` | one row per version | `version_name` (= tag), `parent_version`, `merged_version`, `branch`, `kind` (release / edit / merge), `source_ref` (the source a release loaded; ADR-multi-source), `source_uri` + `source_sha256`, `editor`, `reason`, both snapshot ids, `n_buildings` / `n_added` / `n_removed` / `n_changed`, `created_at` |
| `inventory_changes` | one row per (version, building) changed relative to the parent | `change_kind` (added / removed / modified) and `changed_columns` (buildings columns, plus `geom` when the selected computation geometry changed) |

Root versions (no parent) have no `inventory_changes` rows; every building in them counts as added. `building_history(building_id)` joins the change log with the registry to answer "who changed this building, when, and why" (RQM-16).

### 4. Diffs

`diff_versions(a, b)` is a FULL OUTER JOIN of the two versions' `buildings` on `building_id`. Each side also carries the WKB of the building's selected computation geometry. Every column is compared NULL-safe, and `ingested_at` is ignored. The result has one row per added, removed, or modified building, with the changed columns. `diff_summary` counts results by kind and by column; `compare_buildings` shows old and new values side by side.

### 5. Branches and merges

A project inventory is an Iceberg **branch** with the same name on both tables, created from a version (`create_branch`). Writes go through `rqm.inventory.<table>.branch_<name>`: `ingest_nsi --branch`, `ingest_source --branch`, or Spark SQL `UPDATE` / `DELETE` / `MERGE`. Edits on a branch become a version when they are committed with `branch=<name>`.

`merge_branch(branch, version=...)` requires the branch head to be a committed version. It then works as follows:

- **Fast-forward** when the target has not moved since the branch was created: both tables' refs move, and nothing is rewritten.
- **Three-way** otherwise. Changes since the common ancestor snapshot are computed on both sides, building by building, over attributes and every geometry row.
  - A building changed on both sides is a **conflict**, unless both sides ended in the identical state.
  - Any conflict raises `MergeConflictError`, which lists what each side changed, and nothing is written.
  - Without conflicts, the branch's changes are replayed onto the target with one MERGE per table.

The merge is committed as `kind='merge'`: its parent is the target's previous version, `merged_version` is the branch version, and its change log is what the merge brought in. The documented way to resolve a conflict is to re-apply the edit on a branch created from the current target version, which then fast-forwards.

Merges never resolve conflicts automatically, and conflicts are detected per building rather than per column. Both choices are deliberately conservative.

### 6. Pinned exports (run reproducibility)

`export_version(version, out_dir)` reads the version at its recorded snapshot ids and writes parquet plus `_rqm_version.json`. The manifest holds the version, catalog, namespace, both snapshot ids, provenance (editor, reason, source, source hash), row count, and filter. `export_version(..., sources=[...])` exports only the chosen sources, and the manifest records them along with the row count per source (ADR-multi-source). Re-exporting from a manifest reproduces the data however far the inventory has moved on (RQM-03). The demo notebook checks this byte for byte.

### 7. Catalog: JDBC (SQLite by default) replaces Hadoop

The Hadoop catalog commits by renaming files in the warehouse. That is unsafe on object stores and when two writers commit at once, and branches with parallel editors make concurrent commits a normal case. The default is now Iceberg's **JDBC catalog** on a SQLite file (`<warehouse>/catalog.db`): no server, and every commit is a database transaction.

- **Shared catalog:** a Postgres catalog for multi-user editing only changes `--catalog-prop uri=jdbc:postgresql://...`.
- **Hadoop:** `--catalog-type hadoop` still works.
- **Namespace:** all tables moved into the `inventory` namespace (`rqm.inventory.buildings`), because REST catalogs reject tables at the catalog root. Moving to a REST catalog later is configuration only.
- **Migrating an existing Hadoop warehouse:** register its tables in the new catalog with `CALL <catalog>.system.register_table('<catalog>.inventory.<table>', '<path to metadata.json>')`, or reload from source.

## Consistency rules

`check_consistency` reports, and `test/test_versioning.py` asserts:

1. Every registered version is a tag on both `buildings` and `geometries`, at the snapshot ids in its registry row.
2. No tag lacks a registry row. A tag without one is an incomplete commit and is replaced by the next commit of that name.
3. Version names are unique, are not branch names, and match `[A-Za-z0-9][A-Za-z0-9_.-]*` (not starting with `branch_`).
4. `parent_version` and `merged_version` resolve, and parent chains have no cycles.
5. `inventory_changes` counts per version equal the registry's `n_added` / `n_removed` / `n_changed`.
6. ADR-nsi-minimal rules 1, 2, and 7 hold, and every geometry has a building, **at every version**, not only at the head. Since ADR-multi-source, rule 7 applies to NSI rows, every other row's `building_id` must be the hash of `(source_ref, source_record_id)`, and every `source_ref` must be registered in `sources`.

Tags never expire by default (Iceberg tags have no `max-ref-age` unless one is set). `expire_snapshots` keeps any snapshot a tag or branch references, so all versions survive routine maintenance. Do not set a `max-ref-age` on version tags.

## Worked example (from the demo notebook)

| Version | Branch | Kind | Parent | Merged | Changes | Editor / reason |
|---|---|---|---|---|---|---|
| `nsi-sample-v1` | main | release | — | — | +45,622 | analyst-a / initial load |
| `nsi-sample-v2` | main | release | v1 | — | +15 −40 ~150 | analyst-b / field survey |
| `calibration-1` | calibration | edit | v2 | — | ~381 | analyst-c / FFE calibration |
| `nsi-sample-v3` | main | merge (fast-forward) | v2 | calibration-1 | ~381 | lead-reviewer |
| `buyout-1` | buyout-scenario | edit | v3 | — | −5 | planner |
| `nsi-sample-v4` | main | edit | v3 | — | ~4,315 | valuation-team |
| `nsi-sample-v5` | main | merge (three-way) | v4 | buyout-1 | −5 | lead-reviewer |

A further branch, `review-x`, edits the `found_ht` of one building while main (`nsi-sample-v6`) changes its `val_struct`. Merging it raises `MergeConflictError` and writes nothing. The edit is re-applied on a branch from v6 and fast-forwarded as `nsi-sample-v7`.

## What is chosen vs. deferred

**Chosen:** stable identity via existing ids; MERGE loads; named versions (tags + registry); per-building change log with attribution; diffs; branches with fast-forward and conflict-refusing three-way merges; pinned exports; JDBC catalog default; `inventory` namespace.

**Deferred:**
- **Valid time (bitemporality).** Record time is captured: `created_at` and snapshot commit times. When a change became true in the real world is not. An optional `valid_from` on `inventory_versions` (or on change rows) can be added later without a schema break.
- **Multi-table atomic commits.** These need a catalog with multi-table transactions, such as Nessie once it supports Iceberg v3 or a REST catalog that implements them. Until then, the registry row is the commit marker.
- **Column-level merge and automatic conflict resolution.** Any building changed differently on both sides conflicts, even when the changed columns don't overlap.
- **GIS-native editing (RQM-12).** Today the workflow is: export a version, edit it in QGIS or ArcGIS, and reload the edited file onto a branch with `--editor` / `--reason`. A plugin is out of scope.
- **Hybrid identity tables** (`asset`, `asset_revision`): still not needed. NSI keeps `building_id` = `fd_id`, and since ADR-multi-source, other sources get deterministic hashes of their native ids.

## Alternatives considered

- **Hybrid five-table bundle** (`asset`, `asset_revision`, `inventory_branch`, `revision_log`, bitemporal `versioning`) — the previous proposal in this ADR. Rejected: it reimplements snapshots, branches, and time travel in SQL tables that Iceberg already provides, and it needs a projection view to read any non-head state. Its useful parts are kept: `inventory_versions` records attribution and lineage, and `inventory_changes` plays the role of `revision_log`.
- **Row-level history columns** (`valid_from` / `valid_to`, SCD2) on `buildings` and `geometries`. Rejected: every query has to filter on validity, the tables grow with every change, and branches would need yet another key column.
- **Project Nessie** (catalog-wide git-like branches, multi-table commits, commit authors). This is the best conceptual fit, but Nessie's documentation lists Iceberg table spec v3 as unsupported, and the native `GEOMETRY` column requires v3. Its server also wouldn't start in the devcontainer. Revisit when Nessie supports v3.
- **REST catalog** (Apache Polaris, Lakekeeper). This is the production direction (auth, multi-user, cloud), but it needs a server plus Postgres and does not add cross-table branches. The namespace change keeps the move a configuration change.
- **Keep the Hadoop catalog.** Rejected as the default because of its rename-based commits; it remains available.

## Impact on other ADRs

- **ADR-nsi-minimal:** the "Bitemporal identity + branching" re-entry row is resolved by this ADR. `ingested_at` now means "last written", and history lives in Iceberg snapshots plus the registry.
- **ADR-multi-source:** several inventories share the versioned tables. Loads are scoped to their source, `inventory_versions.source_ref` records the source a release loaded, and versions taken before the multi-source migration lack the new `buildings` columns. `table_at` conforms them (NULL or missing `source_ref` → `NSI`), so diffs across the migration show no spurious changes. Diffs, merges, and branches key on `building_id` and are unchanged.
- **Future domains** (hazard links, loss realizations, exports): pin inputs by version name or snapshot ids, as `export_version` does, rather than reading the head.
