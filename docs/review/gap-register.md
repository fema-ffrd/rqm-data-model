# RQM Data Model — Gap Register

> Read-only output of Prompt 0 (bootstrap). No schema changes here. Establishes what exists today, extracts every distinct item raised in [issue #1](https://github.com/fema-ffrd/rqm-data-model/issues/1), and maps each item to the affected entities and RQM user stories. Prompts 1–10 make the changes; Prompt 11 synchronizes; Prompt 12 posts the response.

**Snapshot date:** 2026-09-03
**Sources evaluated:** [README.md](../../README.md), [data-dictionary.yaml](../../data-dictionary/data-dictionary.yaml) (385 lines, 15 tables), [data-dictionary.md](../../data-dictionary/data-dictionary.md) (308 lines), [diagrams/rqm-erd.mmd](../../diagrams/rqm-erd.mmd) (189 lines), [docs/user-stories/rqm-user-stories.md](../user-stories/rqm-user-stories.md) (RQM-01…RQM-18), [docs/building-standards-options/rqm-building-standards-options.md](../building-standards-options/rqm-building-standards-options.md), issue #1 body + reviewer comment.

---

## 1. Component Inventory

Every construct the prompt pack calls out, and whether it exists in the current model. Labels: **Implemented** (in YAML + ERD), **Documented only** (README/docs but no schema), **Partial** (some fields but incomplete), **Not found** (no representation).

| # | Construct | Status | Where |
|---|---|---|---|
| 1 | Building identity (stable, source-agnostic) | Partial | `buildings.building_id` is a surrogate PK, but there is no explicit identity-vs-revision split. `fd_id` co-exists as a source id without governance. |
| 2 | Geometry (as fields on building) | Partial | `buildings.geom` is a single Point column. No footprint, no LOD, no alternative representations, no separate geometry entity. |
| 3 | Geometry as its own class | Not found | No `geometries` table; geometry is a column, not an entity. |
| 4 | Component-level geometry (polymorphic) | Not found | Components carry no geometry today. |
| 5 | Building components | Implemented | `building_components` table with `component_type`, `generic_id`, `replacement_cost`. |
| 6 | Component-to-component connections (roof→wall→foundation) | Not found | No component-relationship / adjacency table. |
| 7 | Generics vocabulary | Implemented (building-only) | `generics` table (category/subtype/schema_ref) — currently coupled to `building_components` only; no cross-domain generalization. |
| 8 | Attribute uncertainty specs | Implemented (flat) | `attribute_distributions` with `dist_family` (varchar), `parameters` (jsonb), `conditioning` (jsonb). Bound to `building_id` (+ optional `component_id`). No shared/superclass form; each row is independent. |
| 9 | Uncertainty as generalized class shareable across consumers | Not found | No `uncertainty_spec` supertype; buildings, components, DDF percentiles, and future consumers each carry uncertainty independently. |
| 10 | Continuous vs categorical children | Partial | Continuous families exist in the `dist_family` enum; categorical exists as a separate table `foundation_pmf` (not typed as a categorical uncertainty spec). Not unified. |
| 11 | Distribution registry / compute backend binding | Not found | `dist_family` is a bare string enum; no reference to a versioned compute package/registry to make sampling identical across users. |
| 12 | Attribute correlation / conditioning | Partial | `attribute_distributions.conditioning` (jsonb) allows freeform dependencies (e.g., FFH conditioned on foundation) but the pattern is not schema-enforced and no sampler contract is defined. |
| 13 | Foundation PMF (shuffled) | Implemented | `foundation_pmf` with `probability`, `shuffle_policy`, `version_id`. |
| 14 | Hazard events registry | Implemented | `events` table + `versioning` link. |
| 15 | Hazard linkage (grid URI + depth-in-structure uncertainty) | Implemented | `hazard_links` with `depth_grid_uri`, `depth_dist_family`, `depth_parameters`, `grid_version`. |
| 16 | DDF library | Implemented | `ddf_library` with occupancy/foundation/peril mapping. |
| 17 | DDF uncertainty as true distribution (per-depth increment) | Partial | `ddf_uncertainty` currently stores derived percentiles (`damage_p10/p50/p90`) plus an optional `dist_family`/`parameters` per depth row. Present but the distribution form is optional; the percentile shortcut is the primary artifact and go-consequences-style per-increment specs are not the intended primary. |
| 18 | Loss realizations (ensemble scale) | Implemented | `loss_realizations` on Iceberg, partitioned by `event_id`/`aep`, seed-reproducible. |
| 19 | Loss summary (aggregates) | Implemented | `mv_loss_summary` materialized view. |
| 20 | Run catalog + manifests + run logs | Implemented | `run_catalog`, `manifests`, `run_logs`. |
| 21 | Versioning registry | Implemented (single-time) | `versioning` (entity_type/entity_ref/semver/parent_version_id/author/created_at). No valid-time (bitemporal). No branch/fork. No diff/reason logging. No editor identity captured on the versioned row itself beyond `author`. |
| 22 | Bitemporal (valid time + record time) revisions | Not found | Only one timestamp (`created_at`) on `versioning`; no `valid_from/valid_to`. |
| 23 | Branch / fork of an inventory | Not found | No branch identifier; no fork lineage beyond `parent_version_id`. |
| 24 | Change diff / reason / editor per revision | Not found | No `change_log` / `revision_reason` / `diff` table. |
| 25 | Provenance at attribute-value grain (source observation → adopted value) | Not found | `attribute_distributions.source` is free text. No `source_observation` table, no derivation activity, no adopted-vs-observation split, no W3C PROV-O-style entity/activity/agent triple. |
| 26 | Provenance at geometry grain | Not found | Geometry has no source/method/quality field. |
| 27 | Restricted-source classification | Not found | No `access_class` / `restricted_source_ref` construct; no redaction policy. |
| 28 | Versioned export contract | Not found | `run_catalog` implies a run's config but there is no explicit "export contract" object with required fields, units, imputation rules, or sampling policy. |
| 29 | Export product per run type (deterministic snapshot vs distribution-param vs pre-sampled) | Not found | Only run_type enum (`loss|sensitivity|calibration`); no export-product typology. |
| 30 | Imputation responsibility boundary | Not found | Not documented; no governed export compiler entity. |
| 31 | Lossiness report emitted per export | Not found | No structure for discarded alternatives/distributions/geometries. |
| 32 | Validation basin / pilot linkage | Not found | Not represented. |
| 33 | Performance/benchmark thresholds | Not found | No documented scale assumption or benchmark harness. |
| 34 | Post-processing / raster-adjustment tracking | Not found | No representation of downstream engine-output adjustments. |
| 35 | Cross-domain generics superclass | Not found | `generics` is building-scoped; no shared base for future infrastructure/uncertainty subclasses. |
| 36 | Polymorphic FK / supertype pattern (for structure/element refs) | Not found | Current polymorphism is by nullable FK (`attribute_distributions.component_id` optional). No explicit supertype table for future asset types. |
| 37 | Component/LOD validation rules (footprint consistency, orphan checks, LOD promotion) | Not found | No validation rules beyond FK integrity in `preview_dict.py`. |
| 38 | CityJSON / 3DCityDB export path (RQM18) | Documented only | Building-standards-options doc calls it a design goal; no schema hooks (e.g., role tags on geometry, semantic-geometric coherence). |

**Summary:** the model implements the *ensemble* half of RQM (attribute distributions, hazard linkage, DDF percentiles, realizations, run/manifest) and a lightweight versioning table. The *inventory-management* half — stable identity split from revisions, geometry as a class with LOD/multiple representations, component connections, cross-consumer uncertainty superclass, provenance at value grain, bitemporal + branch versioning, restricted-source handling, export contracts, and validation — is largely absent.

---

## 2. Issue-Item Register (stable IDs)

Two source streams. `ISSUE-Q##` = the 8 questions in the issue body (from @zherbz). `ISSUE-R##` = the 7 numbered reviewer comments (rjp3k / Passarelli), anchored to line-level ERD comments. All 15 items get a stable id here and will be referenced by every downstream ADR and by [issue-1-responses.md](issue-1-responses.md) (Prompt 12).

### Body questions

| ID | Question | Resolution prompt in pack |
|---|---|---|
| ISSUE-Q01 | What provenance grain is required: dataset, building, attribute, geometry, transformation activity, or all of these? | Prompt 1 |
| ISSUE-Q02 | Which uncertainty models are building-specific vs regional/class-level, and which correlations must be preserved? | Prompt 2 |
| ISSUE-Q03 | Does inventory versioning require full snapshots, branches, valid-time history, audit history, or a defined subset? | Prompt 3 |
| ISSUE-Q04 | Are footprints required for computation, QC, or future hazard sampling? Which use case would justify detailed 3D? | Prompt 4 |
| ISSUE-Q05 | Should exports contain sampled realizations, distribution parameters, deterministic central estimates, or separate products for different run types? | Prompt 5 |
| ISSUE-Q06 | Which imputations belong in the canonical export compiler vs a specific loss engine? | Prompt 5 |
| ISSUE-Q07 | How should restricted source references be retained without exposing protected attributes? | Prompt 6 |
| ISSUE-Q08 | At representative inventory scale, what export latency, query latency, and storage growth are acceptable? | Prompt 7 |

### Reviewer comments (rjp3k)

Line numbers below refer to the pre-rename `pfrl-erd.mmd` at commit `247fc34`. Anchors on the current [rqm-erd.mmd](../../diagrams/rqm-erd.mmd) are re-derived by content.

| ID | Comment | Current anchor (rqm-erd.mmd) | Resolution prompt |
|---|---|---|---|
| ISSUE-R01 | Uncertainty should be more general — a class with continuous/categorical children — referenced by buildings, building components, DDFs, and future extensions (e.g., building value), instead of independent relationships. | `attribute_distributions` block ~L38–47 | Prompt 2 |
| ISSUE-R02 | Versioning should log author, diff, reasoning, date per change; use-case: GIS Reviewer 1 moves 50 buildings, Reviewer 2 edits 30 attributes in calibration. | `versioning` block ~L148–154 | Prompt 3 |
| ISSUE-R03 | `dist_family` stored as a string — should reference a specific package/registry to make compute representation of distributions consistent across users. | `attribute_distributions.dist_family` ~L43 | Prompt 2A |
| ISSUE-R04 | `ddf_uncertainty` should be a true distribution, potentially with per-depth-increment assignments (see go-consequences occtypes.json). Current representation shows only derived values. | `ddf_uncertainty` block ~L89–97 | Prompt 2B |
| ISSUE-R05 | Component extensions — how are connections between components captured (roof on wall on foundation)? Gesture at future linkages as CityGML handles them. | `building_components` block ~L23–29 | Prompt 8 |
| ISSUE-R06 | Geometry as its own class — multiple LOD and multiple representations per building; component-level independent geometry (HVAC/foundation 3D); CityGML coherent semantical-geometrical modeling. | `buildings.geom` ~L11–22 | Prompt 4 |
| ISSUE-R07 | Generics — handle across domains beyond buildings as a general class with subclasses (future infrastructure, uncertainty classes). | `generics` block ~L30–35 | Prompt 8A |

**Deferred from this pass:** Prompt 9 (Copilot PR review — cardinality + polymorphic FK) and Prompt 10 (Minton workflow-interaction comments) are removed from scope at this stage. The source material for both is not visible on issue #1 as of 2026-09-03, and the operator has authorized proceeding without them rather than blocking on external inputs. They can be re-introduced later when the source comments are surfaced.

---

## 3. Cross-Reference: Issue Item → Entities Affected → User Stories

| ID | Entities that must change | User stories served |
|---|---|---|
| ISSUE-Q01 | New: `source_observation`, `derivation_activity`, `agent`, `adopted_value_link`. Extend: `attribute_distributions`, `hazard_links`, `ddf_uncertainty`. | RQM-04, RQM-07, RQM-09, RQM-16 |
| ISSUE-Q02 / ISSUE-R01 | Refactor: `attribute_distributions` → `uncertainty_spec` (superclass) + `uncertainty_spec_continuous` / `uncertainty_spec_categorical`. Redirect `foundation_pmf` and `ddf_uncertainty` references to reuse the same superclass. New polymorphic reference table (`uncertainty_ref`) so any consumer can point at a spec. | RQM-08, RQM-14, RQM-15, RQM-17 |
| ISSUE-R03 | New: `distribution_registry` (family → parameterization contract → compute backend/version). Change: `dist_family` from bare varchar to FK/reference. | RQM-08, RQM-15 |
| ISSUE-R04 | Refactor: `ddf_uncertainty` to allow a per-depth-increment `uncertainty_spec_id`; retain derived percentiles as an emitted view, not the primary. | RQM-08, RQM-14, RQM-15 |
| ISSUE-Q03 / ISSUE-R02 | Refactor: `versioning` → bitemporal (add `valid_from`, `valid_to`, `record_from`, `record_to`). New: `revision_log` (author, diff, reason, date), `inventory_branch`. Split identity (`asset`) from revision (`asset_revision`) so branches don't fork identity. | RQM-02, RQM-03, RQM-11, RQM-12, RQM-16 |
| ISSUE-Q04 / ISSUE-R06 | New: `geometries` table (polymorphic parent: building or component), with `lod`, `role`, `crs`, `selection_rule`, `source_ref`. Move `buildings.geom` into it and keep a view for compatibility. | RQM-01, RQM-05, RQM-13, RQM-18 |
| ISSUE-R05 | New: `component_connection` (component_a, component_b, relation_type). Validation: components in a connection must exist and belong to the same building (or a documented cross-building case). | RQM-05, RQM-13 |
| ISSUE-Q05 / ISSUE-Q06 | New: `export_contract` (versioned), `export_product` (per run type), `run_manifest_binding` (contract version + engine version + seeds), `lossiness_report`. Boundary rule: imputation happens once in the compiler. | RQM-06, RQM-09 |
| ISSUE-Q07 | New: `source_ref` with `access_class`, `steward`, `redaction_flag`; redaction behavior on `export_product`. | RQM-04, RQM-07, RQM-09 |
| ISSUE-Q08 | Not schema — new: `tools/bench/` synthetic generator + harness; `docs/review/perf-benchmark.md` result template; documented pass/fail thresholds (pending stakeholder confirmation). | RQM-06 (indirectly) |
| ISSUE-R07 | Refactor: `generics` → `generic_base` + subclasses (`generic_building`, and hook points for `generic_infrastructure`, `generic_uncertainty`). Keep the same inheritance/polymorphism pattern chosen for `uncertainty_spec` and `geometries`. | RQM-10 (extensibility) |

---

## 4. What Is Unclear or Missing (before proceeding to Prompts 1–10)

The following need operator input during the subsequent prompts. Prompts 9 and 10 are removed from this pass (see §2). Remaining items are soft asks that shape fidelity but do not block progress.

1. **Representative scale numbers for Prompt 7** (building count, attributes/building, geometries/building, revisions, realizations per run). We will propose defaults (e.g., 5M buildings, ~30 attributes, ~3 geometries, ~10 revisions/1% of buildings, ~1000 realizations) and mark them "proposed pending team confirmation" in the ADR.
2. **Restricted-source examples.** Prompt 6 asks us not to store restricted data. Absent real examples (commercial parcel, PII-adjacent occupancy), the ADR will be generic — still correct, but less specific.
3. **`ddf_uncertainty` current-vs-target reading.** ISSUE-R04 says current representation stores only derived values. The actual YAML already carries an *optional* `dist_family`/`parameters` per depth row — so the model *can* express a per-increment distribution today; what's missing is (a) making that the primary form and (b) requiring reference to the shared uncertainty superclass. Prompt 2B will document the ambiguity and pick the target form.
4. **User-story oracle.** All 18 stories exist (RQM-01…RQM-18), but the file says "A later validated version will show how the proposed architecture supports each story." Some stories (RQM-11 multi-user revisions, RQM-12 GIS-based tracking, RQM-18 alt-schema export) are not yet reflected in any table. We treat them as target stories that changes in Prompts 3, 4, and 5 must serve.

---

## 5. Locked Invariants (carried into every subsequent prompt)

Restated from the pack for reference:

- YAML is the single source of truth; Markdown + Mermaid are regenerated.
- Stable asset identity survives geometry, attribute, and location changes.
- Observation ≠ adopted value; provenance links them.
- Valid time (real-world) ≠ record time (knowledge); bitemporality applies only where changes actually occur.
- Run snapshots are immutable.
- Deterministic value is a valid degenerate form of an uncertainty spec.
- Imputation happens once, in the governed export compiler — never re-imputed by an engine.
- Restricted-source data is referenced, classified, and masked — never embedded in this repo.

---

## 6. Next Step

Proceed sequentially through Prompts 1 → 2 → 2A → 2B → 3 → 4 → 5 → 6 → 7 → 8 → 8A → 11 → 12. Prompts 9 and 10 are removed from this pass (source material not surfaced) and can be re-introduced later.
