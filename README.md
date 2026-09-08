# Risk Quantification Methodology (RQM) Data Model

> Re-architecting deterministic building-loss inputs into first-class, **versioned distribution** objects for probabilistic flood risk assessment.

**Version:** 0.1.0 &nbsp;|&nbsp; **License:** [MIT](LICENSE) &nbsp;|&nbsp; **Companion:** `fema-ffrd/inland-consequences` (SPHERE core schemas)

## Overview

Traditional flood-loss estimation treats each building input — flood depth, foundation
type, first-floor height (FFH), and the depth-damage function (DDF) — as a single
deterministic value. The RQM data model instead stores each uncertain input as a typed,
versioned **distribution specification**. A sampling engine draws per-realization values
from those specifications, propagating uncertainty end-to-end into Monte Carlo loss
ensembles with reproducible provenance.

This repository is the schema/data-model deliverable called for in the *Risk Assessment
Maturity Roadmap* (Recommended Immediate Start Activity #1: "Develop data model/schema for
Risk Assessment"). It realizes the roadmap's building-inventory design pillars —
**Reproducible Models, Versioning, Components, Generics,** and **Parametric Distributions** —
as a concrete, validated schema.

### Design goals

1. **Inventory / component coverage** — model structures and their damageable sub-components.
2. **Uncertainty quantification** — replace scalar attributes with distribution objects.
3. **Versioning / provenance** — every distribution, DDF, and run is reproducible and auditable.

## Storage-tier architecture

Storage tiers are inherited from the FFRD data model, each chosen for a distinct workload:

| Tier | Technology | Role |
|---|---|---|
| **Relational** | PostgreSQL | Inventory, distribution specs, DDF/event registries, results metadata, provenance |
| **Lakehouse** | Apache Iceberg | Ensemble-scale realization draws & aggregates (schema evolution, time-travel) |
| **Gridded** | Icechunk / Zarr | N-dimensional gridded hazard fields, referenced by versioned URI |

## Data model

The model is organized into seven domains (A–G). Tables are defined once in
[data-dictionary/data-dictionary.yaml](data-dictionary/data-dictionary.yaml) (the single
source of truth) and rendered to
[data-dictionary/data-dictionary.md](data-dictionary/data-dictionary.md).

| Domain | Purpose | Key tables |
|---|---|---|
| **A — Inventory** | Structure, component, connection & geometry coverage | `buildings`, `building_components`, `component_connection`, `generics`, `generic_building_component`, `geometries` |
| **B — Attribute Uncertainty** | Generalized uncertainty superclass + typed children, shared via a polymorphic reference, with a compute contract for every parametric family | `uncertainty_spec`, `uncertainty_spec_continuous`, `uncertainty_spec_categorical`, `uncertainty_spec_deterministic`, `uncertainty_ref`, `uncertainty_correlation`, `distribution_registry` |
| **C — Hazard Linkage** | Events and probabilistic flood depth | `events`, `hazard_links` |
| **D — Depth-Damage Functions** | Versioned, probabilistic DDFs | `ddf_library`, `ddf_uncertainty` |
| **E — Realization & Loss Results** | Ensemble-scale draws & summaries | `loss_realizations`, `mv_loss_summary` |
| **F — Provenance & Versioning** | Reproducibility, lineage, bitemporal branching | `run_catalog`, `manifests`, `run_logs`, `versioning` (bitemporal), `asset`, `asset_revision`, `inventory_branch`, `revision_log`, `prov_agent`, `prov_activity`, `source_registry`, `source_observation`, `adopted_value_link` |
| **G — Exports & Contracts** | Versioned export contract, emitted-product manifest, machine-readable lossiness report | `export_contract`, `export_product`, `export_lossiness_report` |

### How the pieces fit together

- **`buildings`** anchors the inventory. Immutable/base attributes live here; *uncertain*
  attributes do not — they are described by shareable **`uncertainty_spec`** rows and
  bound to buildings/components via **`uncertainty_ref`**, resolved per draw. Geometry
  is not a column here — it lives in **`geometries`**.
- **`building_components`** decomposes a structure into modular sub-assemblies (finish,
  foundation, structure, contents, inventory), typed by the extensible **`generics`**
  vocabulary so non-building inventories can be added without a schema break.
- **`component_connection`** captures typed edges between components — a roof `supports`
  wall, envelope `contains` HVAC, HVAC `attached_to` structure. The four kinds
  (`supports` / `contains` / `adjacent_to` / `attached_to`) are the minimum shape needed
  to answer "does this roof have a wall to sit on?" without inventing a graph. Both
  endpoints must belong to the same building; cross-building topology is deferred. See
  [ADR-citygml-profile](docs/decisions/ADR-citygml-profile.md) for the ADOPT-vs-DEFER
  CityGML concept profile and the RQM-18 CityJSON export path.
- **`generics` / `generic_building_component`** is the generalized controlled-vocabulary
  registry. `generics` is a superclass with a `kind` discriminator and one subclass
  today (`generic_building_component`, holding `damage_relevance` and typical cost
  share). Three additional kinds — `infrastructure_component`, `uncertainty_class`,
  `future_asset_kind` — are reserved in the enum with no subclass yet; consumers may
  not point at them until a subclass table lands. Same SQL supertype/subtable pattern
  as `uncertainty_spec`, so the model has **one coherent generalization pattern** across
  domains. See [ADR-generics](docs/decisions/ADR-generics.md).
- **`geometries`** makes geometry its own class: each asset can carry multiple
  representations (point, footprint, reserved 3D roles) *and* multiple alternates at the
  same role (competing footprints from different sources). Geometries attach to
  `asset_id` — a `geometry_move` never forces a non-geometric revision. An optional
  `component_id` lets future component-level 3D (HVAC, foundation) land in the same
  table without redesign. The flat export resolves a single representation via
  `is_selected_for_export` per `(asset_id, purpose='computation')`. See
  [ADR-geometry](docs/decisions/ADR-geometry.md).
- **`uncertainty_spec`** is a generalized, shareable definition of an uncertainty
  (continuous / categorical / deterministic children). One spec may be referenced by many
  consumers (class-level / regional / shared) or by exactly one (building-specific).
  Correlations between specs (e.g., FFH conditioned on foundation class) live in
  **`uncertainty_correlation`**. The former `attribute_distributions` and `foundation_pmf`
  tables collapsed into this design; see [ADR-uncertainty](docs/decisions/ADR-uncertainty.md).
- **`distribution_registry`** is the single source of truth for every parametric
  distribution family the model supports (lognormal, truncated_normal, beta, empirical, …).
  It declares each family's parameter contract (required parameter names + JSON types +
  constraints) and pins the canonical sampler / PDF / CDF / PPF callables. The `dist_family`
  columns on `uncertainty_spec_continuous` and `hazard_links` are foreign keys into this
  registry; `ddf_uncertainty` reaches the same contract indirectly through its
  `spread_spec_id → uncertainty_spec_continuous.dist_family` chain (ADR-ddf-uncertainty).
  A spec's `parameters` jsonb is validated against the family's contract rather than
  trusting a free-text label. See
  [ADR-distribution-registry](docs/decisions/ADR-distribution-registry.md).
- **`events`** is the canonical registry of hazard scenarios; its `event_id` is referenced
  by `run_catalog`, `hazard_links`, and `loss_realizations`.
- **`hazard_links`** ties a structure to a versioned gridded depth surface (Icechunk/Zarr)
  and its depth-in-structure uncertainty, rather than copying a scalar depth into the row.
- **`ddf_library` / `ddf_uncertainty`** make the DDF itself a distribution: each depth
  row publishes a canonical mean plus a reference to an `uncertainty_spec` for the
  spread, and a realization draws a DDF percentile instead of using a single mean
  curve. Legacy percentile-only envelopes (USACE / FIA / HAZUS) remain ingestable
  by leaving the spec reference null. See
  [ADR-ddf-uncertainty](docs/decisions/ADR-ddf-uncertainty.md).
- **`loss_realizations`** (Iceberg) holds one row per building × event/AEP × Monte Carlo
  draw, each reproducible from its `seed` plus version pointers; **`mv_loss_summary`**
  pre-computes central tendency and upper prediction limits.
- **`versioning`** is the central **bitemporal** lineage registry — every versioned entity
  carries a real-world valid-time interval and a knowledge-time record-time interval, so a
  wrong assertion can be corrected without erasing the fact that we once believed it.
- **`asset`** is the stable identity of a real-world thing that survives every geometry /
  attribute / location change. `buildings.asset_id` is the projection anchor; downstream FKs
  to `building_id` logically resolve through it. **`asset_revision`** materializes revisions
  on a specific **`inventory_branch`** (main, calibration, scenario) so two reviewers can
  edit the same asset in parallel without conflict.
- **`revision_log`** records who edited a version, why, and what changed (structured diff) —
  complementary to `adopted_value_link` (which records the *sources* of a value). See
  [ADR-versioning](docs/decisions/ADR-versioning.md).
- **`source_registry`** is the single policy surface for every source referenced by the
  model. Every `source_observation.source_ref` FKs into it. Classification (`public` /
  `internal` / `restricted`) and stewardship (a real `prov_agent`) are mandatory.
  Restricted sources are stored as reference-only: the observation exists as a
  redacted stub (`is_redacted=true`, `observed_value=null`) so provenance can still
  trace an adopted derivative back to its source dataset, but the raw restricted value
  never lives in this repository. Public export contracts must declare a non-restricted
  fallback for any field they emit — otherwise the compile fails. See
  [ADR-restricted-sources](docs/decisions/ADR-restricted-sources.md).
- **`export_contract` / `export_product` / `export_lossiness_report`** draw the boundary
  between canonical richness and the flat-engine input the current consequence engine
  expects. A contract pins required fields, selection rules for alternates, imputation
  policy, and (for sampled products) the sampling policy. A product emits one of three
  kinds — deterministic, distribution-parameters, or pre-sampled realizations — and
  carries its own reproducibility five-tuple `(inventory_snapshot_id, contract
  version_id, compiler_version, source_checksum, seed_root)`. Anything the contract
  dropped (unselected alternates, imputed values, redacted restricted-derived fields)
  lands in the lossiness report. **Imputation happens once, in the governed compiler;
  loss engines never re-impute.** See [ADR-exports](docs/decisions/ADR-exports.md).

### Entity-relationship diagram

The ERD is maintained in [diagrams/rqm-erd.mmd](diagrams/rqm-erd.mmd) (Mermaid). It is an
abridged view — it shows keys and relationships, not every column. Refer to the data
dictionary for the authoritative column list. GitHub renders `.mmd` files automatically;
locally you can preview it with any Mermaid-capable viewer.

## Repository structure

```
rqm-data-model/
├── data-dictionary/
│   ├── data-dictionary.yaml   # single source of truth (tables, columns, enums, FKs)
│   ├── data-dictionary.md     # generated — do not edit by hand
│   ├── preview_dict.py        # validates schema + renders the Markdown
│   └── requirements.txt       # generator dependencies (PyYAML)
├── diagrams/
│   └── rqm-erd.mmd            # Mermaid entity-relationship diagram
├── docs/
│   ├── Risk_Assessment_Maturity_Roadmap - DRAFT.pdf     # source roadmap
│   ├── decisions/             # ADRs (one per resolved issue cluster)
│   ├── review/                # gap register, consistency report, issue-1 responses / traceability / GitHub comment, perf-benchmark
│   ├── user-stories/          # RQM-01…RQM-18 acceptance oracle
│   └── building-standards-options/
├── tools/
│   └── bench/                 # ADR-performance harness scaffold + results template
└── LICENSE
```

## Regenerating the data dictionary

The YAML is authoritative; the Markdown is generated. After editing the YAML, regenerate and
validate in one step:

```powershell
pip install -r data-dictionary/requirements.txt
python data-dictionary/preview_dict.py
```

The generator validates that every table has a description + grain, every column has a
type + description, every domain is declared and renders, every PostgreSQL table has a
primary key, every foreign key points to an existing `table.column`, and the SQL
supertype/subtable identity-name discipline holds. It then writes `data-dictionary.md`.
It exits non-zero on any validation failure, so it is safe to run in CI. Runtime
data-content rules (parameter contracts, stale-cache checks, restricted-source guards,
export-contract binding, LOD-promotion, etc.) are cataloged in
[docs/review/consistency-report.md](docs/review/consistency-report.md) for a future
ingest-time validator. Expected output:

```
OK: validated 35 tables, 303 columns. Wrote data-dictionary.md.
```

## Architecture Decision Records (ADRs)

The [`docs/decisions/`](docs/decisions/) directory holds one **Architecture Decision
Record** per resolved design question. Eleven ADRs cover the schema as it stands at v0.1.
The YAML says *what* the schema is; the ADRs say *why* it is that way — and why the
alternatives were rejected.

### Why they are checked in (they are load-bearing)

- The [roadmap table below](#roadmap-alignment--maturity-status) cites an ADR per row as
  the authoritative source for that decision.
- Many `data-dictionary.yaml` column descriptions reference an ADR by name (e.g.,
  "canonical mean — uncertainty on `damage_mean` itself is out of scope
  (ADR-ddf-uncertainty §Deferred)").
- The response-to-issue-#1 files
  ([issue-1-responses.md](docs/review/issue-1-responses.md),
  [issue-1-traceability.md](docs/review/issue-1-traceability.md)) link every issue item
  to its ADR.
- The ~40 runtime data-content rules cataloged in
  [consistency-report.md](docs/review/consistency-report.md) are grouped by ADR — the
  rule text is in the report, the rationale is in the ADR.
- ADRs cross-reference each other via `Extends:` and `Impact on other ADRs`; they form
  a graph of decisions.

Removing them would leave the schema without its rationale — every future edit would
have to relitigate decisions like "why is geometry a separate class?" or "why is
`buildings.geom` gone?" from scratch.

### How to read one

Every ADR follows the same section structure so a reader can jump straight to what they need:

| Section | What it tells you |
|---|---|
| **Header** (ID, Status, Date, Extends, Resolves, Serves user stories, Locks) | Snapshot: which issue items and RQM-01…RQM-18 stories this ADR serves; which invariants it locks in |
| **Context** | The problem as it stood before the decision — often quoting reviewer comments verbatim |
| **Decision** | What was chosen: schema changes, enum additions, semantics |
| **Validation rules** | Numbered rules for the future ingest-time validator — mirrored in [consistency-report.md](docs/review/consistency-report.md) |
| **Worked examples** | Concrete accepted and rejected cases |
| **Claim discipline** | Every claim labeled *research-recommends* / *specification-allows* / *implementation-demonstrates* / *project-requires* |
| **Alternatives considered** | Rejected paths and why — so future contributors don't relitigate them |
| **Impact on other ADRs** | Cross-references; changes here often ripple |
| **Deferrals** | Explicit hand-offs — scope declared, implementation deferred |

### When to write a new ADR vs. edit an existing one

- **New ADR** for any resolved design question that locks in invariants — anything the
  schema will pay for going forward.
- **Edit an existing ADR** only for clarifying prose, typo fixes, or cross-reference
  updates. Never rewrite a decision in place — that becomes a new ADR that `Extends:`
  or partially supersedes the earlier one (ADR-ddf-uncertainty partially supersedes the
  inline `dist_family`/`parameters` on `ddf_uncertainty` from ADR-distribution-registry;
  see its header).
- **Never delete an ADR.** A reversed decision becomes `Status: Superseded` with a link
  to the successor — not a missing file.

## Roadmap alignment & maturity status

The model maps to the workstreams of the *Risk Assessment Maturity Roadmap* and to the
RQM-01…RQM-18 user stories (see [docs/user-stories/](docs/user-stories/rqm-user-stories.md)).
Coverage of each concept by the current schema:

| Roadmap concept | Status | Realized by | ADR |
|---|---|---|---|
| Reproducible models | ✅ Covered | `run_catalog`, `manifests`, `run_logs`, `loss_realizations.seed`, and the export five-tuple `(inventory_snapshot_id, contract version_id, compiler_version, source_checksum, seed_root)` on `export_product` | [ADR-exports](docs/decisions/ADR-exports.md) |
| Versioning (bitemporal, branched) | ✅ Covered | `versioning` (bitemporal), `asset`, `asset_revision`, `inventory_branch`, `revision_log` | [ADR-versioning](docs/decisions/ADR-versioning.md) |
| Components | ✅ Covered | `building_components`, `component_connection` (typed edges: `supports` / `contains` / `adjacent_to` / `attached_to`), `generic_building_component` (class-level defaults) | [ADR-citygml-profile](docs/decisions/ADR-citygml-profile.md) |
| Generics (extensible vocabulary) | ✅ Covered | `generics` superclass with `kind` discriminator + `generic_building_component` subclass; three reserved kinds (`infrastructure_component`, `uncertainty_class`, `future_asset_kind`) declared for extension | [ADR-generics](docs/decisions/ADR-generics.md) |
| Uncertainty / parametric distributions | ✅ Covered | `uncertainty_spec` (+ continuous / categorical / deterministic subtypes), `uncertainty_ref` (polymorphic bridge), `uncertainty_correlation`, `distribution_registry` (family contracts + compute backends), `ddf_uncertainty.spread_spec_id`, `hazard_links.depth_dist_family` | [ADR-uncertainty](docs/decisions/ADR-uncertainty.md), [ADR-distribution-registry](docs/decisions/ADR-distribution-registry.md) |
| DDF library & assignment | ✅ Covered | `ddf_library`, `ddf_uncertainty` (canonical `damage_mean` + optional `spread_spec_id`; percentiles as rendered cache when spec set) | [ADR-ddf-uncertainty](docs/decisions/ADR-ddf-uncertainty.md) |
| Hazard / flood-depth linkage | ✅ Covered | `hazard_links`, `events` (+ Icechunk-versioned depth surfaces) | — |
| Geometry / multiple representations | ✅ Covered | `geometries` (role: `point` / `footprint` / reserved 3D) with `alt_group_key` for alternates, `is_selected_for_export` selection rule, optional `component_id` for component-level attach; CityGML coherent semantical-geometrical principle at a lightweight level | [ADR-geometry](docs/decisions/ADR-geometry.md) |
| Provenance / traceability (attribute + geometry grain) | ✅ Covered | `prov_agent`, `prov_activity`, `source_observation`, `adopted_value_link` (polymorphic parent to every adopted value); observation ≠ adopted value invariant | [ADR-provenance](docs/decisions/ADR-provenance.md) |
| Restricted-source handling | ✅ Covered | `source_registry` (classification + steward, mandatory); `source_observation.is_redacted`; export-time access gating tied to `export_contract.access_level`; no raw restricted values in this repo, at any access level | [ADR-restricted-sources](docs/decisions/ADR-restricted-sources.md) |
| Calculation-ready exports | ✅ Covered | `export_contract` (versioned, three product kinds), `export_product` (manifest + reproducibility five-tuple), `export_lossiness_report` (machine-readable). Locked invariant: imputation happens once, in the governed compiler | [ADR-exports](docs/decisions/ADR-exports.md) |
| CityGML alignment / RQM-18 export path | ✅ Covered (profile locked; exporter deferred) | ADOPT-vs-DEFER profile (7 principles adopted, 5 encoding concerns deferred); RQM-18 CityJSON path is *a contract* under ADR-exports, no schema changes required | [ADR-citygml-profile](docs/decisions/ADR-citygml-profile.md) |
| Performance & scale acceptability | 🟡 Partial — thresholds pending team ratification | Three scale tiers (100K / 1M / 10M), eight-query workload catalog, proposed thresholds, harness scaffolded at [`tools/bench/`](tools/bench/), CI check rejects fabricated numbers | [ADR-performance](docs/decisions/ADR-performance.md) |
| Monte Carlo convergence criteria | 🟡 Partial | `run_catalog.n_realizations`, `mv_loss_summary.loss_cv` (no explicit convergence record) | — |
| Coastal (wave / SWL / compound) | 🟡 Partial | `peril_type` enum, `hazard_links.velocity_grid_uri` / `duration_grid_uri` | — |
| Global sensitivity analysis | 🟡 Partial | `run_type = 'sensitivity'` only | — |
| Decision uncertainty / BCA | ⚪ Downstream | Out of scope — consumed by BCA tooling | — |

All 18 user stories (RQM-01…RQM-18) are now served by at least one ADR — see
[docs/review/issue-1-traceability.md](docs/review/issue-1-traceability.md) for the
story→ADR roll-up. Runtime data-content rules deferred to a future ingest-time
validator are cataloged in
[docs/review/consistency-report.md](docs/review/consistency-report.md).

## License

Released under the [MIT License](LICENSE).
