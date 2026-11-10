# Risk Quantification Methodology (RQM) Data Model — NSI Minimal — Data Dictionary

**Version:** 0.3.0 &nbsp;|&nbsp; **Companion:** `fema-ffrd/inland-consequences` &nbsp;|&nbsp; **Query engine:** Apache Sedona (Spark 4.1)

NSI-minimal edition: a focused, loadable schema for a single National Structure Inventory parquet ingest, with first-class multiple-geometry support (ADR-geometry) preserved so additional footprint sources can be attached beside each NSI point without a schema break. Component decomposition, attribute uncertainty, hazard links, DDFs, loss realizations, export contracts, provenance registry, and valid-time (bitemporal) history are deferred; re-entry points are documented in ADR-nsi-minimal. Inventory versions, branches, and the per-building change log follow ADR-versioning. The previous full-coverage dictionary is preserved at `data-dictionary-full.yaml`.

## Contents

- **Domain A — Inventory (NSI structures + first-class geometry)**
  - [`buildings`](#buildings)
  - [`geometries`](#geometries)
  - [`inventory_versions`](#inventory_versions)
  - [`inventory_changes`](#inventory_changes)

## Enumerations

- **`occtype`**: `RES1`, `RES2`, `RES3A`, `RES3B`, `RES3C`, `RES3D`, `RES3E`, `RES3F`, `RES4`, `RES5`, `RES6`, `COM1`, `COM2`, `COM3`, `COM4`, `COM5`, `COM6`, `COM7`, `COM8`, `COM9`, `COM10`, `IND1`, `IND2`, `IND3`, `IND4`, `IND5`, `IND6`, `AGR1`, `GOV1`, `GOV2`, `EDU1`, `EDU2`, `REL1`
- **`st_damcat`**: `RES`, `COM`, `IND`, `PUB`, `AGR`
- **`found_type`**: `S`, `C`, `B`, `P`, `F`
- **`geometry_role`**: `point`, `footprint`
- **`geometry_purpose`**: `computation`, `hazard_sampling`, `display`, `qc`
- **`version_kind`**: `release`, `edit`, `merge`
- **`change_kind`**: `added`, `removed`, `modified`

## Domain A — Inventory (NSI structures + first-class geometry)

### `buildings`
**Storage:** Iceberg (via Apache Sedona) &nbsp;|&nbsp; **Grain:** One row per NSI structure.

Flat NSI inventory projection. Every NSI parquet column maps 1:1 to a column on this table, using NSI's native field names (occtype, num_story, val_struct, pop2amu65, …) so queries line up with the raw source. Geometry lives in the `geometries` table (ADR-geometry preserved). Component decomposition, attribute uncertainty specs, hazard links, DDFs, loss realizations, export contracts, provenance registry, and valid-time (bitemporal) history are deferred; re-entry points are documented in ADR-nsi-minimal. The primary key `building_id` is set to the NSI `fd_id` at ingest so ids are stable across re-ingests and versions; `fd_id` is kept as the external natural key. Loads MERGE a full replacement into a branch; earlier states stay readable through version tags (ADR-versioning). Physical layout: partitioned by state (Iceberg `truncate(2, cbfips)`) and sorted by `cbfips`; merge-on-read writes.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `building_id` | bigint | PK | NOT NULL | Stable primary key; equals the NSI fd_id (logical identity; unique, asserted in smoke tests). |
| `fd_id` | bigint |  |  | NSI structure id (external natural key; source of building_id; logically UNIQUE where not null). |
| `bid` | varchar(32) |  |  | NSI building id (UBID — Open Location Code plus extents, e.g. 8645XMJC+QG9-2-2-3-1). |
| `usastrucid` | varchar(32) |  |  | FEMA USA Structures id (joins to USAS footprint source). |
| `ftprntid` | varchar(64) |  |  | Footprint id as reported by NSI (joins to geometries.source_ref when the footprint source is attached). |
| `ftprntsrc` | varchar(32) |  |  | Footprint source tag (MSBF |
| `source` | varchar(32) |  |  | NSI per-structure source code (e.g., P, H); geometries.source_ref is NSI-<source>. |
| `occtype` | varchar(16) |  | NOT NULL | Occupancy classification code (enum occtype; e.g. |
| `st_damcat` | varchar(16) |  |  | Structure damage category (enum st_damcat; RES |
| `bldgtype` | varchar(16) |  |  | Construction class code (e.g. |
| `num_story` | smallint |  |  | Story count. |
| `sqft` | double |  |  | Reported structure area |
| `ftprntsqft` | double |  |  | Footprint-derived area |
| `bldheight` | real |  |  | Building height |
| `med_yr_blt` | smallint |  |  | Median year built for the structure (or its block/cohort). |
| `found_type` | varchar(16) |  |  | Foundation type code (enum found_type; S slab |
| `found_ht` | real |  |  | First-floor height above adjacent grade |
| `val_struct` | double |  |  | Structure replacement value |
| `val_cont` | double |  |  | Contents replacement value |
| `val_vehic` | double |  |  | Vehicle value attributed to structure |
| `ornl_low` | double |  |  | ORNL low-value estimate |
| `ornl_med` | double |  |  | ORNL median-value estimate |
| `ornl_hgh` | double |  |  | ORNL high-value estimate |
| `resunits` | real |  |  | Residential units contained in structure. |
| `novehprob` | real |  |  | Probability that the household has no vehicle. |
| `vehperunit` | real |  |  | Vehicles per residential unit. |
| `pop2amu65` | real |  |  | 2 AM population |
| `pop2amo65` | real |  |  | 2 AM population |
| `pop2pmu65` | real |  |  | 2 PM population |
| `pop2pmo65` | real |  |  | 2 PM population |
| `u65disable` | real |  |  | Under-65 disabled population attributed to structure. |
| `o65disable` | real |  |  | 65-and-over disabled population attributed to structure. |
| `students` | real |  |  | Student population attributed to structure. |
| `firmzone` | varchar(16) |  |  | FIRM (Flood Insurance Rate Map) flood zone code. |
| `zone_sub` | varchar(16) |  |  | FIRM zone subtype / qualifier. |
| `static_bfe` | real |  |  | Static base flood elevation |
| `ground_elv` | real |  |  | Ground elevation at structure |
| `grnd_elv_m` | real |  |  | Ground elevation at structure |
| `pctlowclr` | real |  |  | NSI pctlowclr indicator (percent low-clearance). |
| `fullrep` | real |  |  | NSI fullrep indicator (full-replacement flag or share). |
| `depindex` | real |  |  | NSI dependency index. |
| `creprcnt` | real |  |  | NSI community-resilience percent indicator. |
| `crerank` | real |  |  | NSI community-resilience rank (fractional, 0-1). |
| `cbfips` | varchar(15) |  |  | Census block GEOID (15-digit FIPS) for community-level aggregation. |
| `x` | double |  |  | NSI structure X coordinate (EPSG:3857). Denormalized from `shape` for query convenience. |
| `y` | double |  |  | NSI structure Y coordinate (EPSG:3857). Denormalized from `shape` for query convenience. |
| `ingested_at` | timestamp |  | NOT NULL | When this row was last written: its insert, or the last load or edit that changed it. Version history lives in Iceberg snapshots + inventory_versions. |

### `geometries`
**Storage:** Iceberg (via Apache Sedona) &nbsp;|&nbsp; **Grain:** One row per (building, representation, alternate) geometric record.

First-class geometry registry (ADR-geometry preserved). Each row holds one geometric representation of a building. The NSI load produces exactly one row per structure with role='point', purpose='computation', is_selected_for_export=true, geom from the parquet `shape` column stored as a native Iceberg v3 GEOMETRY with a column-level CRS of EPSG:3857. Alternate footprints (MSBF, USAS) land as additional rows with role='footprint' and may flip the selected flag via a separate selection migration. Logical invariant: exactly one row per (building_id, purpose='computation') has is_selected_for_export=true at any time — enforced by the ingest script and smoke tests (Iceberg does not enforce UNIQUE). 3D roles, component-level geometry, and valid-time history are deferred (see ADR-nsi-minimal re-entry map). Versioned together with buildings (same tag per version). A reload never overwrites is_selected_for_export, deletes only missing NSI rows, and deletes rows from any source whose building is gone. Physical layout: unpartitioned by default (configurable via TableLayout in tools/ingest_nsi.py); merge-on-read writes.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `geometry_id` | bigint | PK | NOT NULL | Stable primary key: xxhash64 of the natural key (source_ref, building_id, role, purpose, alt_group_key), so a geometry keeps its id across re-ingests and selection changes. The ingest refuses a write whose id collides with a different natural key. |
| `building_id` | bigint | FK → `buildings.building_id` | NOT NULL | Parent structure (logical FK; cascade behavior enforced by the ingest contract). |
| `role` | varchar(16) |  | NOT NULL | Geometry role (enum geometry_role): point | footprint. 3D roles deferred. |
| `purpose` | varchar(16) |  | NOT NULL | Why this representation exists (enum geometry_purpose): computation | hazard_sampling | display | qc. |
| `is_selected_for_export` | boolean |  | NOT NULL | Exactly one TRUE per (building_id |
| `alt_group_key` | varchar(64) |  |  | Groups competing alternates at the same (building_id |
| `source_ref` | varchar(64) |  |  | Source dataset identifier (e.g., NSI-2024.1, MSBF-2022-10, USAS-2024). |
| `crs` | varchar(16) |  | NOT NULL | EPSG string for this row's geometry; must equal the geom column CRS (EPSG:3857), so alternates are reprojected on load. |
| `geom` | geometry |  | NOT NULL | Native Iceberg v3 GEOMETRY(EPSG:3857); Point for role='point', Polygon/MultiPolygon for role='footprint'. Sedona reads it via ST_GeomFromWKB(spark_st_asbinary(geom)). |
| `ingested_at` | timestamp |  | NOT NULL | When this row was last written (insert, or a load that changed its point or CRS). |

### `inventory_versions`
**Storage:** Iceberg (via Apache Sedona) &nbsp;|&nbsp; **Grain:** One row per named inventory version.

Version registry (ADR-versioning). Each row names an immutable state of the inventory: `version_name` is also an Iceberg tag on `buildings` and `geometries` pointing at the recorded snapshots, so the version reads as `SELECT … FROM rqm.inventory.buildings VERSION AS OF '<version_name>'`. Records who committed it, why, from which input, on which branch, its lineage (parent and merged versions), and change counts relative to the parent. Written last when a version is committed, so a version exists only once registered; a tag without a row is an incomplete commit.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `version_name` | varchar(100) | PK | NOT NULL | Version name; also the Iceberg tag name. Letters, digits, ".", "_", "-". |
| `parent_version` | varchar(100) | FK → `inventory_versions.version_name` |  | Version this one was derived from (NULL for a root load). For merges |
| `merged_version` | varchar(100) | FK → `inventory_versions.version_name` |  | For kind=merge: the branch version merged in. |
| `branch` | varchar(100) |  | NOT NULL | Iceberg branch the version was committed on (main or a project branch). |
| `kind` | varchar(16) |  | NOT NULL | Enum version_kind: release, edit, or merge. |
| `source_uri` | text |  |  | Input file loaded for the version (NULL for in-place edits and merges). |
| `source_sha256` | varchar(64) |  |  | SHA-256 of the input file. |
| `editor` | varchar(100) |  | NOT NULL | Who committed the version (RQM-16). |
| `reason` | text |  |  | Why the version was committed (RQM-16). |
| `buildings_snapshot_id` | bigint |  | NOT NULL | Iceberg snapshot of buildings tagged with version_name. |
| `geometries_snapshot_id` | bigint |  | NOT NULL | Iceberg snapshot of geometries tagged with version_name. |
| `n_buildings` | bigint |  | NOT NULL | Buildings in the version. |
| `n_added` | bigint |  | NOT NULL | Buildings added relative to parent_version (all of them for a root version). |
| `n_removed` | bigint |  | NOT NULL | Buildings removed relative to parent_version. |
| `n_changed` | bigint |  | NOT NULL | Buildings whose attributes or selected computation geometry changed relative to parent_version. |
| `created_at` | timestamp |  | NOT NULL | When the version was committed (record time). |

### `inventory_changes`
**Storage:** Iceberg (via Apache Sedona) &nbsp;|&nbsp; **Grain:** One row per (version, building) added, removed, or modified relative to the version's parent.

Per-building change log (ADR-versioning), attributed through inventory_versions (editor, reason, created_at). Answers "which versions changed building X, how, and who did it" without scanning table history. Root versions have no rows; every building in them counts as added. Counts per version equal the registry's n_added / n_removed / n_changed.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `version_name` | varchar(100) | PK | NOT NULL | Version that made the change. |
| `building_id` | bigint | PK | NOT NULL | Changed building (logical FK; a removed building is absent from the head but present in earlier versions). |
| `change_kind` | varchar(16) |  | NOT NULL | Enum change_kind: added, removed, or modified. |
| `changed_columns` | varchar[] |  |  | For modified rows: the buildings columns that changed, plus geom when the selected computation geometry changed. |
