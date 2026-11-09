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

The actionable goal: **ingest the real NSI parquet (`test/data/nsi.parquet`, 44 columns, EPSG:3857) into a schema that supports real test cases**, while preserving the one piece of the hybrid design that is genuinely load-bearing for future work — first-class multiple geometries per structure so a point from NSI and a polygon footprint from Microsoft Building Footprints or FEMA USA Structures can co-exist without a schema break.

## Decision

**Two tables. One FK edge. Every NSI field mapped.**

### Tables

- **`buildings`** — one row per NSI structure. Columns map every NSI parquet field one-to-one, using NSI's native field names (`occtype`, `num_story`, `val_struct`, `pop2amu65`, `firmzone`, …) so analyst queries line up with the raw source. Surrogate `building_id` is the primary key; `fd_id` is the external natural key.
- **`geometries`** — first-class geometry registry. The NSI load produces exactly one row per structure with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`, `geom` from the parquet `shape` column (EPSG:3857). Alternates (MSBF, USAS footprints) land as additional rows and may flip the selected flag via a separate selection migration.

### Engine and storage

- **Storage tier:** Apache Iceberg tables.
- **Query engine:** Apache Sedona (PySpark 3.5+) in local mode for development and tests. Sedona provides the geometry UDT and the `ST_*` function family.
- **Primary/foreign keys** are logical; Iceberg does not enforce them. Invariants are enforced by the ingest script and the smoke-test suite.

### Lookups

Occupancy, damage category, foundation type, and FIRM zone are inlined as `varchar` with enum declarations in the YAML `enums:` block. No lookup tables in v0.1. Adding a lookup table later is additive, not a schema break.

### Identity and versioning

`building_id` is the only identity. `ingested_at` is the entire version history. The hybrid branch's `asset` / `asset_revision` / `inventory_branch` / `revision_log` / `versioning` split is deferred.

## Validation rules

Logical invariants enforced by the ingest script (`tools/ingest_nsi.py`) and asserted by the smoke tests (`test/test_nsi_ingest.py`):

1. Every `buildings` row has at least one `geometries` row with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`.
2. For every `(building_id, purpose='computation')` group, exactly one row has `is_selected_for_export=true`.
3. Geometry shape matches `role`: Point for `role='point'`, Polygon or MultiPolygon for `role='footprint'`.
4. `crs` is a well-formed EPSG string (the NSI load sets `EPSG:3857`).
5. `buildings.occtype` is non-null for every row (NSI always publishes it).
6. The ingest's row count equals the NSI parquet's row count; the `geometries` row count equals the same number when only the NSI point source is loaded.

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
| Provenance registry | `prov_agent`, `prov_activity`, `source_registry`, `source_observation`, `adopted_value_link` | Soft references via `buildings.source` and `geometries.source_ref` seed the future registry. |
| Bitemporal identity + branching | `asset`, `asset_revision`, `inventory_branch`, `revision_log`, `versioning` | Add `asset_id` to `buildings`; demote `building_id` to a current-head projection; add the five supporting tables as a bundle. `ingested_at` is replaced by the bitemporal quadruple. |
| Export contracts | `export_contract`, `export_product`, `export_lossiness_report` | Return as a separate `exports` domain; discriminator enum re-adds `'buildings'` and `'geometries'`. |
| 3D geometry roles + component-level geometry | `geometries.role` 3D values, `geometries.component_id` | Role enum extends to include `envelope_3d` / `detailed_3d`; add an optional `component_id` FK once `building_components` lands. |

## Relation to the hybrid branch

The hybrid exploration's eleven ADRs are not part of this branch (they live on `feature/hybrid_data_model`). This ADR is the first in a fresh ADR series on `main`. When any deferred concept returns, the hybrid ADR for that concept (e.g., ADR-geometry, ADR-uncertainty, ADR-ddf-uncertainty) should be ported into this branch's `docs/decisions/` directory alongside the schema change that reintroduces it.
