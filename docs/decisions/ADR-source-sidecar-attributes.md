# ADR — Per-Source Sidecar Attributes and Canonical-Field Lineage

**ID:** ADR-source-sidecar-attributes
**Status:** Accepted
**Date:** 2026-11-24
**Builds on:** [ADR-multi-source](ADR-multi-source.md) (amends §3 and §4), [ADR-versioning](ADR-versioning.md)
**Implementation:** `tools/schema.sql`, `tools/sources.py`, `tools/ingest_source.py`, `tools/ingest_nsi.py`, `tools/versioning.py`, `tools/rqm_version.py`, `tools/sidecar.py`
**Tests / demo:** `test/test_sources.py`, `test/test_multi_source.py`, `test/test_sidecar.py`, `notebooks/03_multi_source_demo.ipynb` §8 (synthetic Milliman-shaped portfolio from `tools/synthetic_sources.py`, spec `test/data/sources/milliman-2026q1.yaml`)
**Plan:** [source-sidecar-attributes](../plans/source-sidecar-attributes.md)

## Context

ADR-multi-source maps every source onto the NSI column set. Two things were missing:

1. **Source fields with no `buildings` column were dropped.** A commercial portfolio's policy term, agent, premium, or construction class had nowhere to go.
2. **Canonical values could not be traced.** `sources.spec_sha256` named only the spec *last* used to load a source, and the spec itself lived outside the warehouse. "Which source column populated `val_struct` for this building?" could not be answered from SQL.

Both had to be solved without a JSON blob on `buildings`, without per-source `buildings` tables (rejected in ADR-multi-source), and without changing identity, scoped loads, or versioning of `buildings` / `geometries`.

## Decision

Three new tables and one new column:

| Table | Grain | Versioned | Purpose |
|---|---|---|---|
| `source_attributes` | ≤ 1 row per building | tagged + branched | Source-native extra fields as `MAP<STRING, STRING>` |
| `building_lineage` | 1 row per building | tagged + branched | The version and spec of the last load that changed the building |
| `source_specs` | 1 row per distinct spec | main only, append-only | The canonical spec document, by hash |
| `sources.sidecar_schema` | column | — | Sidecar key → declared type |

### 1. Spec grammar: `sidecar:`

```yaml
sidecar:
  fields:                                       # output key -> rule
    policy_term:    {from: POLICY_TERM}
    premium_usd:    {from: PREMIUM, type: double}   # type documents how to read it back
    deductible_pct: DEDUCTIBLE_PCT                  # shorthand for {from: ...}
```

- A rule is a pure passthrough: `from` plus an optional `type` (`string` | `double` | `bigint` | `boolean` | `timestamp` | `date`; default `string`). No `expr`, `map`, or `default`: transformations belong in `fields:`.
- Keys match `[A-Za-z_][A-Za-z0-9_]{0,63}` and may not collide (case-insensitively) with any `buildings` column, including the computed `building_id`, `source_ref`, `source_record_id`, `ingested_at`. A key already mapped in `fields:` gets its own message.
- A sidecar rule **may** read an input column that `fields:` also reads (e.g. keep the raw occupancy code behind a crosswalk). The collision rule is on output keys only.
- Values are stored as `CAST(col AS STRING)`. A NULL value **drops the key** from that building's map ("present means the source had a value"), and a building with no non-NULL value gets no row.
- Input columns are checked at load time with the rest of the spec's columns.
- NSI's built-in spec has no `sidecar:` block, but nothing special-cases NSI.

### 2. Canonical spec hash

`spec_sha256` = SHA-256 of the spec's **canonical JSON**: parsed YAML, map keys stringified and sorted recursively, compact separators, UTF-8 (`sources.canonical_document`). Reordering keys, reindenting, comments, and quoting keep the hash; any changed value changes it. This replaces the former file-bytes hash. `source_specs.spec_document` stores that canonical JSON, so `sha2(spec_document, 256) = spec_sha256` is a warehouse-checkable invariant (`versioning.check_spec_registry`). The authored YAML's location is kept as the informational `spec_uri` (repo-relative inside the repository).

### 3. Load lifecycle

`ingest_source.load_source` (and `ingest_nsi.load_buildings` for NSI) runs, after validation:

1. `register_spec`: insert into `source_specs` unless the hash is present (first `spec_uri` sticks);
2. `register_source` (now also writes `sidecar_schema`);
3. MERGE `buildings` (unchanged);
4. MERGE `source_attributes` (`merge_source_attributes`);
5. MERGE `building_lineage` (`merge_building_lineage`);
6. MERGE `geometries` (unchanged).

Step 4 is scoped exactly like `buildings`:

```sql
MERGE INTO source_attributes t USING <staged> s ON t.building_id = s.building_id
WHEN MATCHED AND (
    NOT (to_json(array_sort(map_entries(t.attributes)))
         <=> to_json(array_sort(map_entries(s.attributes))))
    OR NOT (t.source_record_id <=> s.source_record_id)
    OR NOT (t.source_ref <=> s.source_ref)
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.source_ref = '<S>' THEN DELETE
```

- Spark cannot compare `MAP` columns, so the change predicate compares the JSON of the **sorted** entries; key order in the spec never causes a rewrite.
- A spec without `sidecar:` only deletes the source's rows, so removing the block cleans up.
### 4. Lineage points at the last load that changed the building

A lineage row names the version and the spec of the **last load that added or changed the building** — not of the last load of its source. A reload rewrites only the lineage of buildings whose values it changed, so a reload costs lineage writes in proportion to what changed, like `buildings` itself.

- Before step 3, the load records the `buildings` snapshot at the branch head (`ingest_source.buildings_snapshot`). After it, `changed_buildings` anti-joins the source's buildings at the head against that snapshot on `building_id` and every compared column (`BUILDING_COMPARE_COLUMNS`, the same columns as the `buildings` MERGE): what remains was added or modified by this load.
- Step 5 upserts lineage for those buildings, plus any building of the source that has no lineage row yet (e.g. non-NSI buildings loaded before the migration). It then deletes lineage rows of the source whose building is gone. Unchanged buildings keep their row byte for byte, and other sources are never touched.
- **A new spec reaches only the buildings whose values it changes.** A spec edit that leaves a building's values as they were (e.g. `{from: X}` → `{expr: "X * 1.0"}`) does not move its lineage: the stored values are still the ones the earlier spec produced. `sources.spec_sha256` still names the spec last used, and `source_specs` registers every spec used, so the change remains visible at the source level.
- "Changed" is judged on the `buildings` columns, which are what lineage traces. A change to the computation geometry alone, without `x` / `y` moving, does not count. In practice `x` / `y` follow the computation point for every source.
- Consequently, a building's lineage version is the version whose `inventory_changes` recorded it as `added` or `modified` by a load (or the root load that first brought it in), unless an edit changed it later.

### 5. `source_version_name`

The version of the last load that changed the building:

- `load_source(..., version=)` / `load_buildings(..., version=)` record it directly; both CLIs pass `--version`.
- A load without a version writes NULL, and the **next `commit_version` on that branch stamps** NULL rows with the version it commits (`settle_sidecars`). It also re-stamps names that are not registered versions (a load given `--version` that was then committed under another name, or never committed), so a failed or renamed commit cannot leave dangling names. So the column always names the version that recorded the load, and nothing needs to be passed through for ad-hoc loads.
- Edits never rewrite lineage: it points at the load that produced the canonical mapping, not the latest edit. A later load that changes the building again (for example, reverting the edit to the source's values) moves it.

### 6. Versioning

- `VERSIONED_TABLES` is now `buildings`, `geometries`, `source_attributes`, `building_lineage`. Every version is a tag on all four; every branch exists on all four; `create_branch`, `drop_branch`, fast-forward merges, `head_snapshots`, `version_at`, and `check_consistency`'s tag checks cover all four. `inventory_versions` gains `source_attributes_snapshot_id` and `building_lineage_snapshot_id`.
- **Sidecar rows follow their buildings.** Before tagging, `commit_version` deletes sidecar and lineage rows whose building is gone from the branch head (an edit that deletes a building need not know about the sidecar tables).
- **Three-way merges** replay the branch's sidecar and lineage changes since the merge base, except for buildings the target changed in *any* versioned table, including sidecar- or lineage-only changes (e.g. a reload on the target, or the migration backfill). For those buildings the target keeps both its `source_attributes` and its `building_lineage` row, so a building's attributes and lineage always come from the same side. Conflict detection is unchanged: it looks at `buildings` and `geometries` only, so that differing lineage alone (e.g. the same source reloaded on both sides) never blocks a merge.
- `source_specs` is not versioned, like `sources`: every version resolves its lineage against `main`.
- **Invariants** (`check_inventory_invariants`, at every version): sidecar and lineage `building_id`s are unique, resolve in `buildings`, and carry the building's `source_ref`; every lineage `spec_sha256` resolves in `source_specs`; every non-NULL `source_version_name` is a registered version.
- **Diffs.** `DIFF_COLUMNS` and `inventory_changes` are unchanged. `diff_sidecar_versions` (CLI: `rqm_version.py diff --include-sidecar --include-lineage`) reports per building the changed attribute keys or lineage columns.

### 7. Migration

`apply_schema` creates the three tables and adds the new columns, then `versioning.prepare_sidecar_tables` (idempotent, every run):

- gives each sidecar table an **empty root snapshot** (an empty append), so every branch and version can point somewhere;
- creates every `buildings` branch on the sidecar tables at that root (Iceberg refuses writes to a missing branch);
- tags every version registered before the tables existed at the root and fills in its snapshot ids. Older versions therefore read an empty sidecar and an empty lineage — truthfully: nothing was recorded then — instead of carrying NULL snapshot ids through every reader.
- **Backfill**, when `building_lineage` is created in a populated warehouse: NSI buildings on every branch get lineage pointing at the built-in NSI spec (registered in `source_specs`; the NSI `sources` row is re-registered with its canonical hash) and, following §4, at the last NSI release in the branch's history (releases with `source_ref = 'NSI'`, or NULL from before multi-source support) whose `inventory_changes` recorded the building as added or modified, else at the earliest such release (a root load, which records no change rows). Non-NSI sources are **not** backfilled: their specs are not in the warehouse, and their old `sources.spec_sha256` is a file hash. They get exact lineage on their next load. Every building having a lineage row is therefore not an invariant.

### 8. Exports

`export_version` writes, beside the data parquet and `_rqm_version.json`, for the exported buildings only:

- `_source_attributes.parquet` — with `include_sidecar` (default: when an exported source declares a sidecar);
- `_building_lineage.parquet` and `_source_specs.json` (the specs the exported lineage references, with the canonical document and its parsed form) — with `include_lineage` (default on).

The files are `_`-prefixed (rather than the plan's unprefixed names) so Spark, pandas, and pyarrow readers of the export directory keep skipping everything but the data, exactly as they skip the manifest. The manifest gains `n_source_attributes_rows`, `sidecar_schema_by_source`, `n_lineage_rows`, `n_specs`, and `specs_by_sha256` (hash → source). CLI: `rqm_version.py export … --[no-]include-sidecar --[no-]include-lineage`.

### 9. Reading

`tools/sidecar.py` answers one question per function, at the head of a branch or at a version:

- `buildings_with_sidecar`: `buildings` LEFT JOIN `source_attributes` (NULL `attributes` for buildings without a sidecar);
- `lineage_for`: the lineage row with its spec document attached;
- `source_of(building_id, field)`: the spec's rule for that `buildings` column (`from` / `expr` / `map` / `default`), the input columns it reads, and a note for derived or unmapped fields. Parsed documents are cached by hash (content-addressed, so never stale).

`rqm_version.py specs list | show <sha-prefix>` prints the registry; `sources show` prints `sidecar_schema` and the source's specs.

## Decisions on the plan's open questions

1. **MAP vs EAV for `source_attributes`:** MAP. Switch criteria stay as in the plan (§12): > ~50 extras per row, predicate-heavy queries on single keys, or the `to_json` comparison proving too expensive.
2. **Pre-migration `source_version_name`:** reconstructed from history: the last NSI release in the branch's ancestry that added or modified the building (`inventory_changes`), else the earliest NSI release in it (the root load), NULL when there is none. No `backfilled` flag column: NULL already means "no recorded version", and backfilled rows are identifiable as NSI rows whose `ingested_at` is the migration time.
3. **Field-level lineage view:** not added; derivable later from `building_lineage ⨝ source_specs`.
4. **Spec document format:** canonical JSON only; the authored YAML is pointed at by `spec_uri`.

## Consequences

- Every load does two more scoped MERGEs plus an anti-join of the source's buildings against the pre-load snapshot (a read, no writes for unchanged buildings). Lineage writes scale with what a reload changed, not with the size of the source.
- Every `apply_schema` (each CLI run) issues a few metadata queries to keep the sidecar refs in line with `buildings`.
- `sources.spec_sha256` values change once, to canonical hashes, on each source's next load.
- An unversioned load followed by an edit commit is stamped with the edit's version name (it is the version that recorded the load's state).

## Alternatives considered

- **JSON column on `buildings`.** Rejected in the plan: widens every read and every MERGE comparison, and mixes typed canonical columns with untyped extras.
- **EAV rows** (`building_id, attr_name, attr_value`). Deferred (§ open question 1).
- **NULL snapshot ids for pre-sidecar versions.** Rejected in favor of tagging the empty root: no reader, branch, or merge needs a NULL path.
- **Lineage of the last load of the source** (the plan's §7 MERGE, which rewrote a row whenever the version differed). Rejected after implementation: every versioned reload rewrote every lineage row of its source — for NSI, a nationwide merge-on-read pass even when a handful of buildings changed.
- **Hashing the YAML file bytes.** Rejected: cosmetic edits would register new specs and churn lineage.

## Out of scope

- Change-log rows for sidecar or lineage edits (`inventory_changes` is unchanged).
- Redaction of restricted sources' sidecar or lineage rows (ADR-restricted-sources): the tables are keyed and scoped by `source_ref`, which is where a filter would attach.
