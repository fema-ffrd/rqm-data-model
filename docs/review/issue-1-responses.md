# RQM Data Model — Response to Issue #1

**Date:** 2026-09-08
**Scope:** per-item resolution of every distinct question and reviewer comment tracked in the [gap register](gap-register.md), ordered to match the register's ID sequence (`ISSUE-Q01…Q08`, then `ISSUE-R01…R07`).

**Format per item:** decision + ADR link + schema/diagram changes + user stories served + deferrals + status.

Status legend:
- **Resolved** — decision made, schema in YAML, ERD + Markdown synchronized, static checks pass. Runtime data-content rules documented for future ingest validation.
- **Partially resolved — pending team input** — decision made and encoded, but a value (threshold, taxonomy, etc.) needs USACE / FEMA / team confirmation before ratification.
- **Deferred with rationale** — explicit hand-off; scope declared but implementation is a follow-up.

---

## ISSUE-Q01 — Provenance grain

**Status:** Resolved.
**Decision:** W3C PROV-O-inspired profile at attribute-value + geometry grain; dataset grain via source rollup.
**ADR:** [ADR-provenance](../decisions/ADR-provenance.md).
**Schema:** new tables `prov_agent`, `prov_activity`, `source_observation`, `adopted_value_link`. `adopted_value_link` uses a polymorphic parent (`adopted_entity_type`, `adopted_entity_id`) with a discriminator column so it reaches every adopted value in the model.
**User stories served:** RQM-04, RQM-07, RQM-09, RQM-16.
**Deferred:** full PROV-O ontology; multi-step activity graphs (only terminal adopted-value ↔ observation link retained).

## ISSUE-Q02 — Building-specific vs regional/class-level uncertainty; correlations

**Status:** Resolved.
**Decision:** Generalized `uncertainty_spec` superclass with typed children; scope enum (`building_specific | class_level | regional | shared`); first-class `uncertainty_correlation`.
**ADR:** [ADR-uncertainty](../decisions/ADR-uncertainty.md).
**Schema:** new `uncertainty_spec` + `uncertainty_spec_continuous` + `uncertainty_spec_categorical` + `uncertainty_spec_deterministic` + `uncertainty_ref` (polymorphic bridge) + `uncertainty_correlation`. Replaces the former `attribute_distributions` and `foundation_pmf` tables.
**User stories served:** RQM-08, RQM-14, RQM-15, RQM-17.
**Deferred:** full copula / multivariate joint modeling (correlation-kind `correlation` records Pearson/Spearman ρ; rank-preserving copulas are not modeled).

## ISSUE-Q03 — Inventory versioning: snapshots / branches / bitemporality

**Status:** Resolved.
**Decision:** Bitemporal `versioning` + explicit `asset` identity + per-branch `asset_revision` + `revision_log` (editor, reason, diff) + named `inventory_branch`.
**ADR:** [ADR-versioning](../decisions/ADR-versioning.md).
**Schema:** `asset`, `inventory_branch`, `asset_revision`, `revision_log`; `versioning` extended to bitemporal (`valid_from`, `valid_to`, `record_from`, `record_to`).
**User stories served:** RQM-02, RQM-03, RQM-11, RQM-12, RQM-16.
**Deferred:** full git-style three-way merge semantics; minimal branch/fork + overall-diff model instead.

## ISSUE-Q04 — Geometry requirements (footprints, 3D)

**Status:** Resolved.
**Decision:** Geometry becomes its own class attached to `asset_id` with optional `component_id`; roles `point | footprint | envelope_3d | detailed_3d`; alternates at the same role via `alt_group_key`; explicit `is_selected_for_export` selection rule.
**ADR:** [ADR-geometry](../decisions/ADR-geometry.md).
**Schema:** new `geometries` table; `buildings.geom` removed. New enums `geometry_role`, `geometry_purpose`. ERD updated.
**User stories served:** RQM-01, RQM-05, RQM-13, RQM-18.
**Deferred:** `envelope_3d` / `detailed_3d` population; component-level 3D population; positional uncertainty on geometry; CityGML LOD0–LOD4 numeric taxonomy.

## ISSUE-Q05 — Export contents (sampled / parameters / deterministic; per run type)

**Status:** Resolved.
**Decision:** Versioned `export_contract` object; three product kinds (`deterministic`, `distribution_parameters`, `pre_sampled_realizations`); machine-readable lossiness report per product.
**ADR:** [ADR-exports](../decisions/ADR-exports.md).
**Schema:** new tables `export_contract`, `export_product`, `export_lossiness_report`; six new enums including `export_product_kind` and `lossiness_reason`. Domain G added to metadata + rendering.
**User stories served:** RQM-06, RQM-09.
**Deferred:** JSON-Schema formalization of contract shapes; multi-tenant contract governance; cross-contract diff tooling; delta/incremental exports; streaming exports; engine-specific validators.

## ISSUE-Q06 — Imputation responsibility

**Status:** Resolved.
**Decision:** Locked invariant — imputation happens once, in the governed export compiler. Loss engines consume products as-is. Mechanism reuses the existing `prov_activity(activity_type='impute')` + `adopted_value_link(is_default_or_assumed=true)` bridge. Single documented exception: contract-declared `transform_ref`.
**ADR:** [ADR-exports](../decisions/ADR-exports.md).
**Schema:** no new tables (uses existing provenance profile); `export_contract.required_fields[*].imputation_policy` names the fill rule per field.
**User stories served:** RQM-06, RQM-09.
**Deferred:** engine-specific transform registries beyond the single declared exception.

## ISSUE-Q07 — Restricted-source handling

**Status:** Resolved.
**Decision:** `source_registry` as single policy surface. Every `source_observation.source_ref` becomes a real FK. Classification (`public | internal | restricted`) and stewardship mandatory. Restricted observations stored as redacted stubs (`is_redacted=true`, `observed_value=null`). Public contracts must declare non-restricted fallback.
**ADR:** [ADR-restricted-sources](../decisions/ADR-restricted-sources.md).
**Schema:** new `source_registry` table; `source_observation.source_ref` now FK; new `is_redacted` column; enums `access_classification`, `source_kind`; `lossiness_entity_type` gains `source_observation`.
**User stories served:** RQM-04, RQM-07, RQM-10.
**Deferred:** fine-grained restricted taxonomy (tribal / PII / proprietary sub-tags — free text in `restricted_reason` today); Postgres row-level security policies; classification-change audit log; automated policy-URI parsing.

## ISSUE-Q08 — Performance & scale acceptability

**Status:** Partially resolved — pending team input on threshold ratification.
**Decision:** Three scale tiers (Small 100K / Medium 1M / Large 10M); eight-query workload catalog tied to user stories; proposed thresholds per tier; storage-overhead cap ≤ 3× vs flat baseline (Medium/Large) and revision-cost cap ≤ 25% of canonical; benchmark harness scaffolded; measured-cell CI check prevents fabricated numbers.
**ADR:** [ADR-performance](../decisions/ADR-performance.md).
**Files:** [tools/bench/](../../tools/bench/) (`generate_synthetic.py`, `run_queries.py`, `measure_storage.py`, `requirements.txt`, results README + .gitignore); [docs/review/perf-benchmark.md](perf-benchmark.md) results template with every measured cell blank.
**Schema:** none.
**User stories served (via workloads):** RQM-03, RQM-06, RQM-09, RQM-11.
**Deferred:** actual benchmark run; threshold ratification; alternative hardware profiles; concurrency-under-load; CI-integrated regression gating; component-level 3D cost.
**Team input needed:** ratify the proposed thresholds; confirm the representative-scale assumptions; commission a first measured run.

## ISSUE-R01 — Uncertainty as a generalized, shareable class

**Status:** Resolved.
**Decision:** Same resolution as ISSUE-Q02 — the ADR-uncertainty superclass + subclass + polymorphic reference design directly implements this reviewer ask. Consumers (buildings, building_components, ddf_uncertainty, hazard_links, and a `virtual` value for future extensions like building value) all reach `uncertainty_spec` via `uncertainty_ref`.
**ADR:** [ADR-uncertainty](../decisions/ADR-uncertainty.md).
**Schema:** see ISSUE-Q02.
**User stories served:** RQM-08, RQM-14, RQM-15, RQM-17.

## ISSUE-R02 — Versioning log (author, diff, reason, date)

**Status:** Resolved.
**Decision:** `revision_log` records editor `agent_id`, structured `change_kind`, required free-text `reason`, structured `diff` jsonb, and `created_at` per change. The reviewer's motivating example (Reviewer 1 moves 50 buildings, Reviewer 2 edits 30 attributes in calibration) is worked in the ADR.
**ADR:** [ADR-versioning](../decisions/ADR-versioning.md).
**Schema:** `revision_log` + `change_kind` enum + FK to `versioning.version_id` + FK to `prov_agent.agent_id`.
**User stories served:** RQM-11, RQM-12, RQM-16.
**Deferred:** structured diff schema formalization (recommended shape `[{path, before, after}]` documented; not strictly enforced in v0.1).

## ISSUE-R03 — Distribution family registry with compute backend

**Status:** Resolved.
**Decision:** `distribution_registry` as the single source of truth for every parametric family. `family_name` is the natural varchar PK (readability at reference sites — parallels `source_registry.source_ref`). Declares `parameter_contract` (jsonb array) + canonical `sampler_ref` / `pdf_ref` / `cdf_ref` / `ppf_ref`. Three consumer sites gain FKs into it: `uncertainty_spec_continuous.dist_family`, `hazard_links.depth_dist_family`, (originally `ddf_uncertainty.dist_family`, superseded in ISSUE-R04).
**ADR:** [ADR-distribution-registry](../decisions/ADR-distribution-registry.md).
**Schema:** `distribution_registry` + enums `distribution_family_kind`, `distribution_support_type`, `sampler_backend`, `distribution_family_status`.
**User stories served:** RQM-08, RQM-14, RQM-15, RQM-17.
**Deferred:** user-defined / custom family registration workflow; GPU / vectorized sampler variants; multivariate parametric families; PDF/CDF/PPF consistency checks; rich constraint DSL (JSON-Schema); empirical ensemble registry.

## ISSUE-R04 — DDF uncertainty as a true distribution

**Status:** Resolved.
**Decision:** Each `ddf_uncertainty` row publishes a canonical `damage_mean` inline plus an optional `spread_spec_id` FK into `uncertainty_spec` describing the spread at that depth. Percentile columns (`damage_p10/p50/p90`) become a rendered cache when the spec is set (stale-cache check ≤ 1e-3), authoritative when the spec is null (legacy USACE / FIA / HAZUS envelopes). Point DDFs (both null) legal, flagged diagnostic. The inline `dist_family`/`parameters` columns from ADR-distribution-registry are superseded here — the registry contract now applies indirectly via `uncertainty_spec_continuous.dist_family`. go-consequences per-depth-increment pattern is served by one row per depth, each with its own spec.
**ADR:** [ADR-ddf-uncertainty](../decisions/ADR-ddf-uncertainty.md).
**Schema:** `ddf_uncertainty` reshaped (added `spread_spec_id`; removed inline `dist_family`/`parameters`; percentile column semantics rewritten).
**User stories served:** RQM-08, RQM-13, RQM-14, RQM-15, RQM-17.
**Deferred:** uncertainty on `damage_mean` itself; depth-parametric single-spec DDFs (parameters as functions of depth); DDF ensemble registry; cross-depth spread correlation within a realization; categorical DDF spread.

## ISSUE-R05 — Component-to-component connections

**Status:** Resolved.
**Decision:** New `component_connection` table with four `connection_kind` values (`supports`, `contains`, `adjacent_to`, `attached_to`) — the minimum needed for the reviewer's example (roof → wall → foundation). Both endpoints must belong to the same building; symmetric-kind pairs stored once. Also produces the ADOPT-vs-DEFER CityGML concept profile as its companion resolution.
**ADR:** [ADR-citygml-profile](../decisions/ADR-citygml-profile.md).
**Schema:** `component_connection` + `connection_kind` enum + FKs to `building_components` and `versioning`.
**User stories served:** RQM-05, RQM-10, RQM-13, RQM-18.
**Deferred:** cross-building topology; richer connection vocabulary (`bolted_to`, `welded_to`, load-fraction weights); full topological graph queries; ordering-consistency rules across component types; bidirectional connection semantics at the enum layer.

## ISSUE-R06 — Geometry as its own class (incl. component-level)

**Status:** Resolved (see ISSUE-Q04).
**Decision:** Same as ISSUE-Q04. `geometries.component_id` (optional) provides the component-level attach point without a polymorphic parent — the semantic hierarchy (asset → component) is a two-level FK, not a polymorphic `(owner_type, owner_id)` bridge.
**ADR:** [ADR-geometry](../decisions/ADR-geometry.md).
**Schema:** see ISSUE-Q04.
**User stories served:** RQM-01, RQM-05, RQM-13, RQM-18.

## ISSUE-R07 — Generics generalization across domains

**Status:** Resolved.
**Decision:** `generics` refactored into a SQL supertype/subtable pattern (identical to `uncertainty_spec`) with `kind` discriminator. Concrete subclass `generic_building_component` populated in v0.1; three additional kinds (`infrastructure_component`, `uncertainty_class`, `future_asset_kind`) reserved in the enum but non-referenceable until their subclass tables land. One coherent generalization pattern across the model.
**ADR:** [ADR-generics](../decisions/ADR-generics.md).
**Schema:** `generics` extended (added `kind`, `description`, `version_id`, `created_at`); new `generic_building_component` subclass; enums `generic_kind`, `damage_relevance`.
**User stories served:** RQM-05, RQM-10, RQM-13.
**Deferred:** `generic_infrastructure_component` and `generic_uncertainty_class` subclass tables; domain-specific `schema_ref` validators; cross-generic relationships; ordering constraints across generic kinds; `generic_kind` extension governance.

---

## Cross-cutting deliverables

- **Artifact synchronization + validation.** [Consistency report](consistency-report.md) documents the drift fix (`preview_dict.py` DOMAIN_TITLES lacked `exports`), five new static checks added, and the full inventory of ~40 runtime data-content rules deferred to a future ingest-time validator.
- **Traceability matrix.** [issue-1-traceability.md](issue-1-traceability.md).
- **GitHub-issue-1 comment.** [issue-1-github-comment.md](issue-1-github-comment.md) — paste-ready; does not auto-close.

## Not addressed in this pass

Two register-listed sources of feedback (Prompts 9 and 10 in the original pack) were removed from scope during Prompt 0 bootstrap:

- **Copilot PR-review comments on relationship cardinality / polymorphic FK correctness** (would have been Prompt 9). Source material not surfaced on issue #1 as of the session date.
- **Minton workflow-interaction comments** (would have been Prompt 10). Source material not surfaced.

Re-introduce these when the source comments are visible on GitHub.
