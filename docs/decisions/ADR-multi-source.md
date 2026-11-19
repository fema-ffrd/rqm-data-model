# ADR — Multi-Source Building Inventories (side by side)

**ID:** ADR-multi-source
**Status:** Accepted
**Date:** 2026-11-11
**Builds on:** [ADR-nsi-minimal](ADR-nsi-minimal.md) (amends validation rule 7), [ADR-versioning](ADR-versioning.md)
**Serves user stories:** RQM-04 (national, local, commercial, or project-specific inventories; keep each source's native identifier)
**Implementation:** `tools/schema.sql`, `tools/sources.py`, `tools/sources/nsi.yaml`, `tools/ingest_source.py`, `tools/ingest_nsi.py`, `tools/versioning.py`, `tools/rqm_version.py`
**Tests / demo:** `test/test_sources.py`, `test/test_multi_source.py`, `notebooks/03_multi_source_demo.ipynb` (synthetic sources from `tools/synthetic_sources.py`, specs in `test/data/sources/`)
**Plan:** [multi-source-inventory-plan](../plans/multi-source-inventory-plan.md)

## Context

Until this ADR every building was an NSI row: `building_id = fd_id`, and a load was a full-replacement `MERGE` that deleted every building not in its input. Loading any other inventory would have wiped NSI. There was also no way to map another source's column names, codes, units, CRS, or ids onto the NSI field set.

RQM-04 asks for inventories from national, local, commercial, and project-specific sources, each keeping its native identifier.

## Decision

**Sources sit side by side in `buildings`, scoped by `source_ref`. The NSI field set is the shared column schema, and each source maps onto it through one declarative YAML spec.** There is no cross-source matching or conflation.

### 1. Identity

| Column | Meaning |
|---|---|
| `buildings.source_ref` | The inventory the row came from (FK → `sources.source_ref`). `NSI` for the National Structure Inventory. |
| `buildings.source_record_id` | The source's native id, as a string (NSI: the `fd_id`). |
| `buildings.building_id` | NSI: the `fd_id`, as before. Every other source: `xxhash64(source_ref, source_record_id)` with the sign bit set (`ingest_nsi.source_building_id_expr`). |

- **No collisions with NSI.** Hashed ids are always negative and NSI's `fd_id`s are positive, so the two can never collide.
- **Collision guard.** Before writing, a load refuses any hashed id that collides with a different `(source_ref, source_record_id)` (`assert_no_building_id_collisions`, mirroring the `geometry_id` guard).
- **Stable across releases.** A `source_ref` names a dataset *lineage* (`COUNTY-ASSESSOR`), not a release, and stays the same across releases. The release label goes in the version name and `inventory_versions.source_uri`. So a structure keeps its `building_id` across reloads, and diffs between releases show modifications rather than delete-plus-insert (RQM-02).
- **Who may use native ids.** Only the built-in NSI spec may use native ids. Every other spec gets hashed ids, and `NSI` is a reserved `source_ref`.

### 2. Scoped loads

A load is a full replacement **of its own source**:

```sql
MERGE INTO buildings t USING <load> s ON t.building_id = s.building_id
WHEN MATCHED AND (<any compared column differs>) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.source_ref = '<source>' THEN DELETE
```

- **Reloads are isolated.** Reloading one source never touches another source's rows.
- **Geometries** follow the same rules (ADR-versioning §1). After the upsert, a delete pass removes two kinds of rows:
  - the source's own geometries (`geometries.source_ref` = `<source>` or `<source>-<subcode>`) for buildings of that source that the load no longer produces;
  - every geometry whose building is gone, as before.
- **`geometries.source_ref` values.** `geometries.source_ref` equals the building's `source_ref`, or `<source_ref>-<subcode>` when the spec names a subcode column. NSI keeps `NSI-P` / `NSI-H`, so every existing `geometry_id` (and the pinned test id) is unchanged.
- **Polygon sources** get two geometry rows per building:
  - a `role='footprint'`, `purpose='display'` row;
  - a `role='point'`, `purpose='computation'`, selected row from `ST_PointOnSurface`.

  So ADR-nsi-minimal rules 1–3 hold for every source.
- **CRS and coordinates.**
  - Input in any EPSG CRS is transformed to EPSG:3857 (`ST_Transform`).
  - WKB points already in EPSG:3857 (NSI) pass through byte for byte, so reloads compare equal.
  - `x` / `y` hold WGS84 lon / lat, which is what NSI actually publishes. Unless a spec maps them, they are derived from the computation point.

### 3. The `sources` registry

| Column | Notes |
|---|---|
| `source_ref` | PK; `[A-Z0-9][A-Z0-9_.-]*`, up to 64 characters |
| `source_name` | Human-readable name |
| `source_kind` | Enum `source_kind`: `national_inventory`, `local_inventory`, `commercial_inventory`, `project_inventory`, `field_survey`, `remote_sensing`, `other` |
| `description` | Free text |
| `spec_sha256` | SHA-256 of the YAML spec last used to load the source |
| `updated_at` | When the row was last written |

The registry row is upserted from the spec on every load, before the data `MERGE`s, so a building never references an unregistered source. It is written on `main` and never branched, and rows are never deleted, so every version's `source_ref`s resolve. `inventory_versions.source_ref` records which source a release loaded; it is NULL for edits and merges.

### 4. The mapping spec

One YAML file per source (`tools/sources.py`; demo specs in `test/data/sources/`):

```yaml
source_ref: COUNTY-ASSESSOR
source_name: Example County Assessor Parcels
source_kind: local_inventory
input:
  format: gpkg               # parquet | geoparquet | csv | gpkg
  layer: parcels             # gpkg only
  crs: EPSG:2276
  geometry: {kind: polygon, column: geom}     # or {kind: point, lon: LON, lat: LAT}
id: {column: PARCEL_ID}
fields:
  occtype:    {from: LANDUSE, map: {A1: RES1, F1: COM1}, default: RES1}
  found_ht:   {expr: "FFE_IN / 12.0"}
  found_type: {from: FOUND, map: {SLAB: S, CRAWL: C}}
  num_story:  STORIES        # shorthand for {from: STORIES}
```

**Field rules.**
- A field rule is exactly one of:
  - `from`: a rename;
  - `expr`: a Spark SQL expression, e.g. a unit conversion;
  - `from` + `map`: a code crosswalk, with input codes trimmed and compared as strings.
- Any rule can add a `default`, which replaces NULL. A rule with only a `default` is a constant.
- Each value is cast to its `buildings` column type, and unmapped columns are NULL.
- `expr` runs in Spark SQL, so a spec is trusted configuration, like code.

**Validation in pure Python.** `parse_spec` runs before Spark starts and reports every problem at once:
- Unknown keys are rejected, so a typo can't silently drop a field.
- Targets must be mappable `buildings` columns. The computed columns `building_id`, `source_ref`, `source_record_id`, and `ingested_at` can't be mapped.
- `occtype` must be mapped or defaulted (rule 5).
- Map values and defaults must be valid enum codes for `occtype`, `st_damcat`, and `found_type`. NSI's qualified occupancies such as `RES1-1SNB` are accepted. Other columns get a basic type check.
- `source_kind`, the EPSG CRS, the geometry kind (`point` | `polygon`), and the geometry columns (for each format) must be valid.
- An id column must be declared.

**Validation at load time** (`ingest_source.validate_input`): the load stops before writing if:
- a column the spec names is missing from the input;
- ids are NULL or duplicated;
- a row has no geometry, or a geometry of the wrong type (rule 3);
- a row maps to a NULL `occtype`.

**Reported, not refused:**
- Crosswalk gaps: input codes no `map` covers (`crosswalk_gaps`).
- Rows without `cbfips`, which land in the NULL-state partition of `buildings`.

**NSI is a source like any other.** `tools/sources/nsi.yaml` is the built-in identity mapping (native `fd_id` ids, `subcode: source`). `ingest_nsi.load_buildings` / `load_geometries` keep their signatures and NSI behavior and delegate to the generic engine with it.

**Readers.**
- Parquet and CSV are read through Spark, GeoParquet through Sedona.
- GeoPackage is read through Sedona's `geopackage` reader. If that reader fails, the layer is staged to parquet through `pyogrio`.

### 5. Migration of existing warehouses

`apply_schema` adds missing columns to existing tables idempotently (`migrate_table`: `ALTER TABLE ADD COLUMN … AFTER …`, in DDL position).

**No column defaults.** Iceberg cannot add a required column to a populated table, and Spark 4.1 rejects column defaults (*"setting default values in Spark is currently unsupported"*). So migrated columns are nullable and existing rows read NULL. Iceberg also reads tags with the schema of their snapshot, so pre-migration versions have no `source_ref` column at all.

**Reads conform instead.** Readers fill in both cases. `versioning.table_at` passes every `buildings` read through `ingest_nsi.conform_buildings`:
- a NULL or missing `source_ref` reads as `NSI`;
- `source_record_id` reads as the `fd_id` string.

Loads compare the same way, so reloading NSI onto a migrated warehouse rewrites nothing, and a diff across the migration is empty.

When the migration adds `buildings.source_ref`, it also registers the built-in NSI source, so `check_consistency` holds for pre-migration versions.

### 6. Versioning, invariants, and exports

**Versioning keys on `building_id`.** Diffs, merges, and branches all key on `building_id`, so they work across sources unchanged. `DIFF_COLUMNS` picks up `source_ref` / `source_record_id` automatically.

**Invariants.** `check_inventory_invariants` adds these checks at every version:
- every `source_ref` is non-null and registered in `sources`;
- rule 7 (`building_id = fd_id`) applies to NSI rows only;
- every other row's `building_id` equals the hash of `(source_ref, source_record_id)`.

**Exports.** `export_version(..., sources=[...])` (CLI: `rqm_version.py export … --source REF`, repeatable) keeps only the chosen sources. The manifest records the filter, the version's `source_ref`, and `rows_by_source`.

### 7. Double counting

Side by side means the same physical structure can appear once per source: an NSI point and an assessor parcel for the same house are two `buildings` rows. Any aggregate across sources double counts. Queries and exports should select sources explicitly; the demo notebook shows the naive sum next to the per-source totals. Matching structures across sources is a possible follow-up, not part of this decision.

## Consequences

- **Rule 7 now covers NSI rows only.** ADR-nsi-minimal validation rule 7 is amended to that scope; other sources follow §1.
- **New CLIs.** `ingest_source.py --spec SPEC.yaml --input FILE [--version …]` loads any source. `rqm_version.py sources list | show <ref>` inspects the registry and per-version counts.
- **Schema migration on every run.** `rqm_version.py` runs the schema migration before every command, so an older warehouse is upgraded on first use.
- **Identity tables still deferred.** The hybrid-branch identity tables (`asset`, `asset_revision`) remain unnecessary: non-NSI ids are deterministic hashes of the native ids.
- **cbfips is not derived.** Sources without `cbfips` are allowed, but their rows land in the NULL-state partition. Spatially joining census blocks is left to a follow-up. The synthetic demo sources carry `cbfips`.

## Alternatives considered

- **Conflate into one row per structure** (match sources, then merge their attributes by precedence). Rejected for now: it needs matching rules, per-attribute precedence, and provenance per value. That is a much larger design, and side-by-side loading is a prerequisite for it anyway.
- **A separate `buildings_<source>` table per source.** Rejected: every query, diff, merge, and export would have to union the tables, and versions would have to tag a growing set of tables.
- **Sequence-assigned ids for non-NSI sources.** Rejected: ids would depend on load order, and Iceberg has no sequences. The hash depends only on the source lineage and its native id.
- **Mapping in Python code per source.** Rejected in favor of declarative YAML. A spec can be validated before Spark starts, diffed in review, hashed into the registry, and written by people who don't write Spark code.
- **Iceberg column defaults for the migration.** Spark 4.1 doesn't support them (verified), so readers conform NULLs instead (§5).

## Out of scope

- **Restricted sources.** [ADR-restricted-sources](ADR-restricted-sources.md) stays *Proposed*: access classification, redaction, provenance tables, and export contracts. The `source_ref` column and the `sources` registry are where that work attaches. That ADR sketches a `source_registry` with steward and classification columns and a wider `source_kind` enum (`class_default`, `tribal`, `proprietary`, `agency_restricted`, …). If it is accepted, it should extend `sources` (add the columns, extend the enum) rather than add a second registry.
- **Cross-source matching and conflation** (§7).
