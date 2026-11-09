# Risk Quantification Methodology (RQM) Data Model — NSI Minimal — Data Dictionary

**Version:** 0.2.0 &nbsp;|&nbsp; **Companion:** `fema-ffrd/inland-consequences` &nbsp;|&nbsp; **Query engine:** Apache Sedona (Spark 3.x)

NSI-minimal edition: a focused, loadable schema for a single National Structure Inventory parquet ingest, with first-class multiple-geometry support (ADR-geometry) preserved so additional footprint sources can be attached beside each NSI point without a schema break. Component decomposition, attribute uncertainty, hazard links, DDFs, loss realizations, export contracts, provenance registry, and bitemporal versioning are deferred; re-entry points are documented in ADR-nsi-minimal. The previous full-coverage dictionary is preserved at `data-dictionary-full.yaml`.

## Contents

- **Domain A — Inventory (NSI structures + first-class geometry)**
  - [`buildings`](#buildings)
  - [`geometries`](#geometries)

## Enumerations

- **`occtype`**: `RES1`, `RES2`, `RES3A`, `RES3B`, `RES3C`, `RES3D`, `RES3E`, `RES3F`, `RES4`, `RES5`, `RES6`, `COM1`, `COM2`, `COM3`, `COM4`, `COM5`, `COM6`, `COM7`, `COM8`, `COM9`, `COM10`, `IND1`, `IND2`, `IND3`, `IND4`, `IND5`, `IND6`, `AGR1`, `GOV1`, `GOV2`, `EDU1`, `EDU2`, `REL1`
- **`st_damcat`**: `RES`, `COM`, `IND`, `PUB`, `AGR`
- **`found_type`**: `S`, `C`, `B`, `P`, `F`
- **`geometry_role`**: `point`, `footprint`
- **`geometry_purpose`**: `computation`, `hazard_sampling`, `display`, `qc`

## Domain A — Inventory (NSI structures + first-class geometry)

### `buildings`
**Storage:** Iceberg (via Apache Sedona) &nbsp;|&nbsp; **Grain:** One row per NSI structure.

Flat NSI inventory projection. Every NSI parquet column maps 1:1 to a column on this table, using NSI's native field names (occtype, num_story, val_struct, pop2amu65, …) so queries line up with the raw source. Geometry lives in the `geometries` table (ADR-geometry preserved). Component decomposition, attribute uncertainty specs, hazard links, DDFs, loss realizations, export contracts, provenance registry, and bitemporal versioning are all deferred; re-entry points are documented in ADR-nsi-minimal. The primary key `building_id` is a surrogate generated at ingest; `fd_id` is the external natural key.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `building_id` | bigint | PK | NOT NULL | Surrogate primary key (generated at ingest; logical identity). |
| `fd_id` | bigint |  |  | NSI structure id (external natural key; logically UNIQUE where not null). |
| `bid` | bigint |  |  | NSI block id. |
| `usastrucid` | varchar(32) |  |  | FEMA USA Structures id (joins to USAS footprint source). |
| `ftprntid` | varchar(64) |  |  | Footprint id as reported by NSI (joins to geometries.source_ref when the footprint source is attached). |
| `ftprntsrc` | varchar(32) |  |  | Footprint source tag (MSBF |
| `source` | varchar(32) |  |  | NSI release tag (e.g. |
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
| `crerank` | integer |  |  | NSI community-resilience rank. |
| `cbfips` | varchar(15) |  |  | Census block GEOID (15-digit FIPS) for community-level aggregation. |
| `ingested_at` | timestamp |  | NOT NULL | Row insertion timestamp (serves as the v0.1 version history until bitemporal versioning returns). |

### `geometries`
**Storage:** Iceberg (via Apache Sedona) &nbsp;|&nbsp; **Grain:** One row per (building, representation, alternate) geometric record.

First-class geometry registry (ADR-geometry preserved). Each row holds one geometric representation of a building. The NSI load produces exactly one row per structure with role='point', purpose='computation', is_selected_for_export=true, geom from the parquet `shape` column (EPSG:3857). Alternate footprints (MSBF, USAS) land as additional rows with role='footprint' and may flip the selected flag via a separate selection migration. Logical invariant: exactly one row per (building_id, purpose='computation') has is_selected_for_export=true at any time — enforced by the ingest script and smoke tests (Iceberg does not enforce UNIQUE). 3D roles, component-level geometry, and bitemporal validity are deferred (see ADR-nsi-minimal re-entry map).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `geometry_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `building_id` | bigint | FK → `buildings.building_id` | NOT NULL | Parent structure (logical FK; cascade behavior enforced by the ingest contract). |
| `role` | varchar(16) |  | NOT NULL | Geometry role (enum geometry_role): point | footprint. 3D roles deferred. |
| `purpose` | varchar(16) |  | NOT NULL | Why this representation exists (enum geometry_purpose): computation | hazard_sampling | display | qc. |
| `is_selected_for_export` | boolean |  | NOT NULL | Exactly one TRUE per (building_id |
| `alt_group_key` | varchar(64) |  |  | Groups competing alternates at the same (building_id |
| `source_ref` | varchar(64) |  |  | Source dataset identifier (e.g., NSI-2024.1, MSBF-2022-10, USAS-2024). |
| `crs` | varchar(16) |  | NOT NULL | EPSG string for this row's geometry (e.g. |
| `geom` | geometry |  | NOT NULL | Sedona geometry value; Point for role='point' |
| `ingested_at` | timestamp |  | NOT NULL | Row insertion timestamp. |
