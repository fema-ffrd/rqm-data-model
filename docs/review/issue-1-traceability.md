# RQM Data Model — Issue #1 Traceability Matrix

**Date:** 2026-09-08

Rows: every issue item from the [gap register](gap-register.md).
Columns: **ADR** → **Primary entity / field** → **User stories served** → **Key validation rule (Prompt 11 or runtime)**.

Runtime rules are the ones cataloged in [consistency-report.md](consistency-report.md) §"Validation rules deferred to runtime / ingest-time"; static rules are enforced today by `preview_dict.py`.

## Body questions

| Issue | ADR | Primary entity / field | User stories | Key validation rule |
|---|---|---|---|---|
| ISSUE-Q01 | [ADR-provenance](../decisions/ADR-provenance.md) | `source_observation`, `prov_activity`, `prov_agent`, `adopted_value_link` | RQM-04, RQM-07, RQM-09, RQM-16 | Runtime: every adopted value has ≥ 1 supporting observation OR `is_default_or_assumed=true` with non-null `assumption_rationale` |
| ISSUE-Q02 | [ADR-uncertainty](../decisions/ADR-uncertainty.md) | `uncertainty_spec` + `uncertainty_ref` + `uncertainty_correlation` | RQM-08, RQM-14, RQM-15, RQM-17 | Runtime: every `uncertainty_spec` has exactly one row in exactly one subclass table (kind discriminator); categorical PMF sums to 1.0 ± 1e-6 |
| ISSUE-Q03 | [ADR-versioning](../decisions/ADR-versioning.md) | `asset`, `asset_revision`, `inventory_branch`, `revision_log`, `versioning` (bitemporal) | RQM-02, RQM-03, RQM-11, RQM-12, RQM-16 | Runtime: exactly one `asset_revision` per `(asset_id, branch_id)` has `is_current=true`; `record_from` non-null when referenced by an `asset_revision` |
| ISSUE-Q04 | [ADR-geometry](../decisions/ADR-geometry.md) | `geometries.asset_id` + optional `component_id`; `role`, `crs`, `is_selected_for_export` | RQM-01, RQM-05, RQM-13, RQM-18 | Runtime: exactly one `geometries` row per `(asset_id, purpose='computation')` has `is_selected_for_export=true`; component_id's asset must equal geometry's asset |
| ISSUE-Q05 | [ADR-exports](../decisions/ADR-exports.md) | `export_contract`, `export_product`, `export_lossiness_report` | RQM-06, RQM-09 | Runtime: `export_product.product_kind` == bound contract's `product_kind`; `seed_root` non-null iff `pre_sampled_realizations` |
| ISSUE-Q06 | [ADR-exports](../decisions/ADR-exports.md) | `export_contract.required_fields[*].imputation_policy` + existing `prov_activity(activity_type='impute')` | RQM-06, RQM-09 | Runtime: every imputed value has matching `prov_activity` + `adopted_value_link(is_default_or_assumed=true)` with non-null `assumption_rationale` |
| ISSUE-Q07 | [ADR-restricted-sources](../decisions/ADR-restricted-sources.md) | `source_registry` (classification + steward); `source_observation.is_redacted` | RQM-04, RQM-07, RQM-10 | Runtime: `access_classification='restricted'` requires `is_redacted=true` + `observed_value=null`; public export contracts must declare a non-restricted fallback |
| ISSUE-Q08 | [ADR-performance](../decisions/ADR-performance.md) | *(no schema)* — [tools/bench/](../../tools/bench/), [perf-benchmark.md](perf-benchmark.md) | RQM-03, RQM-06, RQM-09, RQM-11 | Static: measured cells in `perf-benchmark.md` must be JSON-backed by files under `tools/bench/results/` |

## Reviewer comments

| Issue | ADR | Primary entity / field | User stories | Key validation rule |
|---|---|---|---|---|
| ISSUE-R01 | [ADR-uncertainty](../decisions/ADR-uncertainty.md) | `uncertainty_spec` (superclass) + `uncertainty_ref` (polymorphic bridge) | RQM-08, RQM-14, RQM-15, RQM-17 | Runtime: `uncertainty_ref.consumer_type` ∈ `uncertainty_consumer_type` enum; `consumer_id` resolves in the table named by `consumer_type` |
| ISSUE-R02 | [ADR-versioning](../decisions/ADR-versioning.md) | `revision_log` (editor, reason, diff, timestamp) | RQM-11, RQM-12, RQM-16 | Static: FK to `versioning.version_id` + FK to `prov_agent.agent_id`; `reason` non-null. Runtime: no revision overwrites a prior; every version bump inserts a new row |
| ISSUE-R03 | [ADR-distribution-registry](../decisions/ADR-distribution-registry.md) | `distribution_registry.family_name` (varchar PK) + `parameter_contract` (jsonb) | RQM-08, RQM-14, RQM-15, RQM-17 | Runtime: consumer's `parameters` jsonb satisfies referenced family's `parameter_contract`; `sampler_ref` non-null for active rows |
| ISSUE-R04 | [ADR-ddf-uncertainty](../decisions/ADR-ddf-uncertainty.md) | `ddf_uncertainty.damage_mean` + `spread_spec_id` | RQM-08, RQM-13, RQM-14, RQM-15, RQM-17 | Runtime: when `spread_spec_id` set, `\|p10/p50/p90 − spec.ppf(q)\| ≤ 1e-3` (stale-cache); simultaneous direct FK + `uncertainty_ref` binding rejected |
| ISSUE-R05 | [ADR-citygml-profile](../decisions/ADR-citygml-profile.md) | `component_connection` (`from`, `to`, `connection_kind`) | RQM-05, RQM-10, RQM-13, RQM-18 | Runtime: endpoints belong to same `building_id`; `from != to`; `adjacent_to` unique per unordered `{from, to}` pair |
| ISSUE-R06 | [ADR-geometry](../decisions/ADR-geometry.md) | `geometries.component_id` (optional FK) | RQM-01, RQM-05, RQM-13, RQM-18 | Runtime: component_id → `building_components.building_id → buildings.asset_id` must equal `geometries.asset_id` (coherent semantical-geometrical rule) |
| ISSUE-R07 | [ADR-generics](../decisions/ADR-generics.md) | `generics` (superclass, `kind` discriminator) + `generic_building_component` (subclass) | RQM-05, RQM-10, RQM-13 | Static: SQL supertype/subtable identity name match. Runtime: `building_components.generic_id` → `generics.kind='building_component'`; deferred kinds non-referenceable |

## User story coverage roll-up

| User story | Served by |
|---|---|
| RQM-01 Multiple geometry representations | ISSUE-Q04, ISSUE-R06 (ADR-geometry) |
| RQM-02 Stable asset identity | ISSUE-Q03 (ADR-versioning) |
| RQM-03 Inventory versioning + branching | ISSUE-Q03 (ADR-versioning), ISSUE-Q08 (ADR-performance workloads) |
| RQM-04 Multiple source inventories | ISSUE-Q01 (ADR-provenance), ISSUE-Q07 (ADR-restricted-sources) |
| RQM-05 Asset components + relationships | ISSUE-Q04/R06 (ADR-geometry), ISSUE-R05 (ADR-citygml-profile), ISSUE-R07 (ADR-generics) |
| RQM-06 Calculation-ready exports | ISSUE-Q05/Q06 (ADR-exports), ISSUE-Q08 (ADR-performance) |
| RQM-07 Data + assumption traceability | ISSUE-Q01 (ADR-provenance), ISSUE-Q07 (ADR-restricted-sources) |
| RQM-08 Attribute uncertainty | ISSUE-Q02/R01 (ADR-uncertainty), ISSUE-R03 (ADR-distribution-registry), ISSUE-R04 (ADR-ddf-uncertainty) |
| RQM-09 Asset-to-result linkage | ISSUE-Q01 (ADR-provenance), ISSUE-Q05/Q06 (ADR-exports), ISSUE-Q07 (ADR-restricted-sources), ISSUE-Q08 (ADR-performance) |
| RQM-10 Infrastructure + hazard extensibility | ISSUE-Q07 (ADR-restricted-sources), ISSUE-R05 (ADR-citygml-profile), ISSUE-R07 (ADR-generics) |
| RQM-11 Multi-user revisions | ISSUE-Q03/R02 (ADR-versioning), ISSUE-Q08 (ADR-performance workloads) |
| RQM-12 GIS-based revision tracking | ISSUE-Q03/R02 (ADR-versioning) |
| RQM-13 Asset hierarchy + structure | ISSUE-Q04/R06 (ADR-geometry), ISSUE-R04 (ADR-ddf-uncertainty), ISSUE-R05 (ADR-citygml-profile), ISSUE-R07 (ADR-generics) |
| RQM-14 Event-derived uncertainty | ISSUE-Q02/R01 (ADR-uncertainty), ISSUE-R03 (ADR-distribution-registry), ISSUE-R04 (ADR-ddf-uncertainty) |
| RQM-15 Attribute uncertainty modularity | ISSUE-Q02/R01 (ADR-uncertainty), ISSUE-R03 (ADR-distribution-registry), ISSUE-R04 (ADR-ddf-uncertainty) |
| RQM-16 Revision attribution | ISSUE-Q01 (ADR-provenance), ISSUE-Q03/R02 (ADR-versioning) |
| RQM-17 Uncertainty hierarchy + structure | ISSUE-Q02/R01 (ADR-uncertainty), ISSUE-R03 (ADR-distribution-registry), ISSUE-R04 (ADR-ddf-uncertainty) |
| RQM-18 Alternative schema export | ISSUE-Q04/R06 (ADR-geometry), ISSUE-R05 (ADR-citygml-profile) |

Every user story RQM-01…RQM-18 is now served by at least one ADR.
