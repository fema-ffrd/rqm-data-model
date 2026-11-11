# ADR: NSI-Minimal Schema (v0.1)

**ID:** ADR-nsi-minimal
**Status:** Accepted
**Date:** 2026-11-09
**Scope:** Inventory + geometry only; all other RQM domains deferred.
**Serves user stories:** Ingest-and-query of the National Structure Inventory (NSI) with first-class multi-geometry attach for future building-footprint sources.
**Supersedes:** n/a (new decision on `main`; see "Relation to the hybrid branch" below).

---

## Context

A prior exploration on the `feature/hybrid_data_model` branch built the full RQM "wish list": 35 Postgres/Iceberg tables across seven domains (A Inventory, B Attribute Uncertainty, C Hazard Linkage, D Depth-Damage Functions, E Loss Realizations, F Provenance & Versioning, G Export Contracts). That exploration produced eleven ADRs documenting real decisions about uncertainty specs, the generics vocabulary, bitemporal versioning, the W3C PROV-O profile, export contracts, and more. It is preserved for reference at `data-dictionary/data-dictionary-full.yaml` and `diagrams/rqm-erd-full.mmd`.

Two critiques of that exploration drove this ADR:

1. **Too broad to prototype.** Seven domains and 35 tables is more surface area than any single engineer can stand up or load real data into on a reasonable timeline. The paper schema was difficult to validate against anything concrete.
2. **Over-normalized.** Subtype/supertype splits where only one subclass is populated (`generics` + `generic_building_component`; `uncertainty_spec` + three subtype tables); identity/branching tables that join with no payload (`asset` + `asset_revision` + `inventory_branch` + `revision_log` + `versioning`); a five-table W3C PROV-O profile for a single-source batch load.

The actionable goal: **ingest the real NSI parquet (`test/data/nsi.parquet`, 46 columns, EPSG:3857) into a schema that supports real test cases**, while preserving the one piece of the hybrid design that is genuinely load-bearing for future work — first-class multiple geometries per structure so a point from NSI and a polygon footprint from Microsoft Building Footprints or FEMA USA Structures can co-exist without a schema break.

## Decision

**Two tables. One FK edge. Every NSI field mapped.**

### Tables

- **`buildings`** — one row per NSI structure. Columns map every NSI parquet field one-to-one, using NSI's native field names (`occtype`, `num_story`, `val_struct`, `pop2amu65`, `firmzone`, …) so analyst queries line up with the raw source. `building_id` is the primary key and is set to the NSI `fd_id` at ingest, so ids are stable across re-ingests; `fd_id` is kept as the external natural key.
- **`geometries`** — first-class geometry registry. The NSI load produces exactly one row per structure with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`, `geom` from the parquet `shape` column stored as a native Iceberg v3 `GEOMETRY(3857)`. Alternates (MSBF, USAS footprints) land as additional rows and may flip the selected flag via a separate selection migration.

### Engine and storage

- **Storage tier:** Apache Iceberg format-version 3 tables in the `inventory` namespace (`rqm.inventory.buildings`). `geometries.geom` uses the native v3 `GEOMETRY` type with a column-level CRS of EPSG:3857. The default catalog is Iceberg's JDBC catalog on a SQLite file in the warehouse; see [ADR-versioning](ADR-versioning.md) §7.
- **Query engine:** Apache Sedona 1.9 on PySpark 4.1 (local mode for development and tests; the same `SparkSettings` target a cluster and a REST/Glue catalog). Spark 4.1 + Iceberg 1.12 is the first open-source stack that reads and writes v3 geometry columns. Sedona provides the `ST_*` function family; because Sedona shadows Spark's built-in geometry functions and only accepts its own geometry UDT, the ingest registers Spark's built-ins as `spark_st_*` and Sedona reads `geom` via `ST_GeomFromWKB(spark_st_asbinary(geom))`.
- **Physical layout:** `buildings` is partitioned by state (`truncate(2, cbfips)`) and sorted by `cbfips`; `geometries` is unpartitioned. Layouts are configurable per table at creation (`TableLayout` in `tools/ingest_nsi.py`).
- **Primary/foreign keys** are logical; Iceberg does not enforce them. Invariants are enforced by the ingest script and the smoke-test suite.

### Lookups

Occupancy, damage category, foundation type, and FIRM zone are inlined as `varchar` with enum declarations in the YAML `enums:` block. No lookup tables in v0.1. Adding a lookup table later is additive, not a schema break.

### Identity and versioning

`building_id` is the structure identity; for NSI rows it equals the NSI `fd_id`, so a structure keeps its id across re-ingests. Other inventories load beside NSI with hashed ids ([ADR-multi-source](ADR-multi-source.md)). `geometry_id` is likewise deterministic: the xxhash64 of the row's natural key `(source_ref, building_id, role, purpose, alt_group_key)`, so a geometry keeps its id across re-ingests and selection migrations, and the ingest refuses a write whose id collides with a different natural key. Versioning is defined by [ADR-versioning](ADR-versioning.md): loads are MERGE upserts, named versions are Iceberg tags recorded in `inventory_versions`, and per-building changes are recorded in `inventory_changes`. `ingested_at` records when a row was last written. The hybrid branch's `asset` / `asset_revision` / `inventory_branch` / `revision_log` / `versioning` split is not needed while these ids are stable.

## Validation rules

Logical invariants enforced by the ingest script (`tools/ingest_nsi.py`) and asserted by the smoke tests (`test/test_nsi_ingest.py`):

1. Every `buildings` row has at least one `geometries` row with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`.
2. For every `(building_id, purpose='computation')` group, exactly one row has `is_selected_for_export=true`.
3. Geometry shape matches `role`: Point for `role='point'`, Polygon or MultiPolygon for `role='footprint'`.
4. `crs` is a well-formed EPSG string (the NSI load sets `EPSG:3857`).
5. `buildings.occtype` is non-null for every row (NSI always publishes it).
6. The ingest's row count equals the NSI parquet's row count; the `geometries` row count equals the same number when only the NSI point source is loaded.
7. `buildings.building_id` is unique, and for NSI rows (`source_ref = 'NSI'`) it equals `fd_id`. *Amended by [ADR-multi-source](ADR-multi-source.md):* rows from every other source have `building_id = xxhash64(source_ref, source_record_id)` with the sign bit set.

## Alternatives considered

- **Single flat table combining `buildings` and the NSI point geometry.** Rejected: loses the ability to attach a footprint without a migration; the ADR-geometry principle is the one piece of the hybrid design worth preserving.
- **Three or four tables splitting population exposure and FIRM regulatory fields into child tables (`building_exposure`, `building_regulatory`).** Rejected in favor of the flat `buildings` table to minimize joins and match the NSI parquet shape 1:1. Readability is recovered via column grouping in the data-dictionary description, not via physical splits.
- **DuckDB-only stack.** Rejected: loses PostGIS/Sedona-style spatial semantics on multi-million-row workloads and ties us to a single-node engine. Sedona-on-Spark scales out when the inventory grows beyond a county-sized test case.
- **Keep the subtype hierarchies (`generics`, `uncertainty_spec`) because they are the "right" model.** Rejected: subtypes with zero or one populated children are pure overhead until a second subtype actually lands. Re-entry is additive.

## Deferrals and re-entry map

Nothing from the hybrid exploration is lost — the full dictionary and ERD sit beside the new ones. Each deferred concept has a concrete re-entry point so reintroducing it is additive, not a rewrite:

| Deferred concept | Hybrid-branch tables | Re-entry point in this schema |
|---|---|---|
| Attribute uncertainty | `uncertainty_spec` (+ 3 subtables), `uncertainty_ref`, `uncertainty_correlation`, `distribution_registry` | Add `uncertainty_ref` with `consumer_type='buildings'`, `consumer_id=buildings.building_id`. No change to `buildings`. |
| Components | `building_components`, `component_connection` | FK on `buildings.building_id`; connections key on component-pair as in the hybrid ADR-citygml-profile. |
| Generics vocabulary | `generics`, `generic_building_component` | Reintroduce when a second vocabulary kind actually lands; inline enums remain fine until then. |
| Hazard links | `events`, `hazard_links` | Standalone `events`; `hazard_links` FKs on `buildings.building_id` and references the hazard-grid URI. |
| Depth-damage functions | `ddf_library`, `ddf_uncertainty` | No inventory dependency; keyed on `occtype` + `found_type` enums that already exist here. |
| Loss realizations | `loss_realizations`, `mv_loss_summary` | Iceberg tables keyed on `building_id` + `event_id` + `realization_id`. No schema change to the core. |
| Provenance registry | `prov_agent`, `prov_activity`, `source_registry`, `source_observation`, `adopted_value_link` | **Partly resolved by [ADR-multi-source](ADR-multi-source.md):** the `sources` registry and `buildings.source_ref` / `source_record_id` record which inventory every row came from. Still deferred: per-value provenance and adoption ([ADR-restricted-sources](ADR-restricted-sources.md), *Proposed*). |
| Bitemporal identity + branching | `asset`, `asset_revision`, `inventory_branch`, `revision_log`, `versioning` | **Resolved by [ADR-versioning](ADR-versioning.md):** Iceberg tags and branches plus `inventory_versions` / `inventory_changes`. Non-NSI id schemes are resolved by [ADR-multi-source](ADR-multi-source.md) (hashed ids, no identity tables). Still deferred: valid time. |
| Export contracts | `export_contract`, `export_product`, `export_lossiness_report` | Return as a separate `exports` domain; discriminator enum re-adds `'buildings'` and `'geometries'`. |
| 3D geometry roles + component-level geometry | `geometries.role` 3D values, `geometries.component_id` | Role enum extends to include `envelope_3d` / `detailed_3d`; add an optional `component_id` FK once `building_components` lands. |

## Relation to the hybrid branch

The hybrid exploration's eleven ADRs are not part of this branch (they live on `feature/hybrid_data_model`). This ADR is the first in a fresh ADR series on `main`. When any deferred concept returns, the hybrid ADR for that concept (e.g., ADR-geometry, ADR-uncertainty, ADR-ddf-uncertainty) should be ported into this branch's `docs/decisions/` directory alongside the schema change that reintroduces it.
