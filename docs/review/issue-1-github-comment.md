# Paste-ready GitHub comment for issue #1

> **Do not auto-close the issue.** Items marked *Pending team input* or *Deferred* remain open.

---

Resolution status for the 15 items tracked in [gap-register.md](docs/review/gap-register.md). Full per-item write-ups in [issue-1-responses.md](docs/review/issue-1-responses.md); traceability matrix in [issue-1-traceability.md](docs/review/issue-1-traceability.md); artifact synchronization and validation state in [consistency-report.md](docs/review/consistency-report.md).

### Body questions

- **ISSUE-Q01 — Provenance grain.** ✅ **Resolved.** W3C PROV-O-inspired profile at attribute-value + geometry grain. → [ADR-provenance](docs/decisions/ADR-provenance.md). Serves RQM-04, RQM-07, RQM-09, RQM-16.
- **ISSUE-Q02 — Building-specific vs class/regional uncertainty; correlations.** ✅ **Resolved.** Generalized `uncertainty_spec` superclass + subclass + polymorphic `uncertainty_ref` + first-class `uncertainty_correlation`. → [ADR-uncertainty](docs/decisions/ADR-uncertainty.md). Serves RQM-08, RQM-14, RQM-15, RQM-17.
- **ISSUE-Q03 — Inventory versioning / branching / bitemporality.** ✅ **Resolved.** `asset` identity split from `asset_revision`; bitemporal `versioning`; named `inventory_branch`; `revision_log` with editor + reason + diff. → [ADR-versioning](docs/decisions/ADR-versioning.md). Serves RQM-02, RQM-03, RQM-11, RQM-12, RQM-16.
- **ISSUE-Q04 — Geometry requirements (footprint / 3D justification).** ✅ **Resolved.** Geometry promoted to `geometries` class attached to `asset_id`; footprint + point required today; 3D roles reserved but not populated until a named use case commits. → [ADR-geometry](docs/decisions/ADR-geometry.md). Serves RQM-01, RQM-05, RQM-13, RQM-18.
- **ISSUE-Q05 — Export contents (sampled / parameters / deterministic).** ✅ **Resolved.** Versioned `export_contract` + three product kinds + machine-readable `export_lossiness_report`. → [ADR-exports](docs/decisions/ADR-exports.md). Serves RQM-06, RQM-09.
- **ISSUE-Q06 — Imputation responsibility.** ✅ **Resolved.** Locked invariant: imputation happens once, in the governed export compiler. → [ADR-exports](docs/decisions/ADR-exports.md). Serves RQM-06, RQM-09.
- **ISSUE-Q07 — Restricted-source handling.** ✅ **Resolved.** `source_registry` as single policy surface; restricted observations stored as redacted stubs; public export contracts must declare non-restricted fallback. No raw restricted values in this repo, at any access level. → [ADR-restricted-sources](docs/decisions/ADR-restricted-sources.md). Serves RQM-04, RQM-07, RQM-10.
- **ISSUE-Q08 — Performance & scale.** 🟡 **Partially resolved — pending team input.** Three scale tiers, eight-query workload catalog, proposed thresholds, benchmark harness scaffolded at [`tools/bench/`](tools/bench/) with a measured-cell CI check. **Team input needed to ratify thresholds and commission a first measured run.** → [ADR-performance](docs/decisions/ADR-performance.md). Serves RQM-03, RQM-06, RQM-09, RQM-11.

### Reviewer comments (rjp3k)

- **ISSUE-R01 — Uncertainty as a general class.** ✅ **Resolved** via ADR-uncertainty (see Q02). Consumers reach `uncertainty_spec` through `uncertainty_ref` polymorphically.
- **ISSUE-R02 — Versioning log (author, diff, reason, date).** ✅ **Resolved.** `revision_log` captures each of those + structured `change_kind` enum + `diff` jsonb. → [ADR-versioning](docs/decisions/ADR-versioning.md).
- **ISSUE-R03 — Distribution family registry with compute backend.** ✅ **Resolved.** `distribution_registry` with `parameter_contract` + `sampler_ref`/`pdf_ref`/`cdf_ref`/`ppf_ref`. Three consumer sites gain FKs into it. → [ADR-distribution-registry](docs/decisions/ADR-distribution-registry.md).
- **ISSUE-R04 — DDF uncertainty as a true distribution.** ✅ **Resolved.** `ddf_uncertainty` reshaped: canonical `damage_mean` inline + optional `spread_spec_id` FK into `uncertainty_spec` for per-depth spread; percentile columns become rendered cache when spec is set. Point DDFs remain legal. → [ADR-ddf-uncertainty](docs/decisions/ADR-ddf-uncertainty.md).
- **ISSUE-R05 — Component-to-component connections.** ✅ **Resolved.** `component_connection` table with four kinds (`supports`, `contains`, `adjacent_to`, `attached_to`) + validation rules that pay off as LOD increases. Companion ADOPT/DEFER CityGML concept profile in the same ADR. → [ADR-citygml-profile](docs/decisions/ADR-citygml-profile.md).
- **ISSUE-R06 — Geometry as its own class + component-level.** ✅ **Resolved** via ADR-geometry (see Q04). Component-level attach is a real FK on `geometries.component_id`, not a polymorphic bridge — the semantic hierarchy is two levels, so a two-level FK is enforceable at the DB.
- **ISSUE-R07 — Generics generalization.** ✅ **Resolved.** `generics` refactored to SQL supertype/subtable with `kind` discriminator (same pattern as `uncertainty_spec`). One populated subclass today; three reserved kinds for future infrastructure / uncertainty class / future asset kinds. → [ADR-generics](docs/decisions/ADR-generics.md).

### Cross-cutting state

- **35 tables / 303 columns.** Validate clean under [`preview_dict.py`](data-dictionary/preview_dict.py).
- **YAML ↔ ERD:** perfect set match (35/35).
- **YAML ↔ Markdown:** regenerated; Domain G now renders (silent-drop drift fixed).
- **~40 runtime data-content rules** deferred to a future ingest-time validator, cataloged verbatim in [consistency-report.md](docs/review/consistency-report.md).

### Explicit deferrals (each with rationale in its ADR)

Full copula modeling; `envelope_3d` / `detailed_3d` geometry population; component-level 3D population; positional uncertainty on geometry; CityGML LOD0–LOD4 numeric taxonomy; user-defined distribution families; JSON-Schema formalization of export-contract shapes; multi-tenant contract governance; delta / streaming exports; fine-grained restricted taxonomy; Postgres RLS; classification-change audit log; benchmark thresholds pending ratification; benchmark first-run; `generic_infrastructure_component` / `generic_uncertainty_class` subclass tables; cross-building topology; richer connection vocabulary; load-fraction weights; automatic CityJSON exporter.

### Not addressed in this pass

The original prompt pack included two additional resolution steps (relationship cardinality / polymorphic FK correctness, and workflow-interaction / consequence-storage questions). Source material for both was not surfaced on this issue as of 2026-09-03 and they were removed from scope during bootstrap. Re-open on request when the source comments are visible.

### What remains open

- Ratify the ADR-performance thresholds (team confirmation).
- Commission a first benchmark run and populate [perf-benchmark.md](docs/review/perf-benchmark.md).
- (Optional) re-scope the two removed items above.

Leaving this issue open. Closing individual items will happen as they are ratified.
