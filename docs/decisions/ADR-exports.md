# ADR — Export Contracts, Run-Type Products & Imputation Responsibility

**ID:** ADR-exports
**Status:** Proposed
**Date:** 2026-09-08
**Extends:** ADR-versioning, ADR-provenance, ADR-uncertainty, ADR-geometry
**Resolves:** ISSUE-Q05 (export contents), ISSUE-Q06 (imputation responsibility)
**Serves user stories:** RQM-06, RQM-09
**Locks:** immutable run snapshots; imputation happens once in the governed export compiler; canonical richness never breaks the flat-engine export boundary.

## Context

The canonical model is rich: shareable uncertainty specs, multiple geometries per asset, bitemporal revisions, alternate footprints, per-depth DDF spread. The current consequence engine (Inland Consequences baseline) still expects a **one-row-per-building, calculation-ready flat file**. That gap raises two coupled questions the reviewers asked directly:

1. **What does an export actually contain?** Sampled realizations, distribution parameters, or deterministic central estimates — and should there be separate products per run type?
2. **Who imputes?** When a source value is missing, does the canonical export compiler fill it (class default, regional average), or does the loss engine? Doing it in both places is how models silently disagree.

Left unresolved, each downstream engine will grow its own imputation, its own selection rules, and its own version-of-the-truth. Every calibration exercise becomes archaeology.

## Decision

Introduce a **versioned export-contract object** that fully describes what a compiled export must contain, how alternates and missing values are resolved, and how the run/inventory snapshot binds to the produced artifact. Emit distinct **products by run type**. Draw one hard boundary: **imputation happens once, in the governed export compiler**. Attach a machine-readable **lossiness report** to every export so nothing quietly disappears.

### New tables

`export_contract`

- `export_contract_id` — bigint PK.
- `name` — varchar, required. Human-readable contract name (e.g., `inland_consequences_flat_v1`).
- `target_engine` — varchar, required. The engine this contract feeds (default: `inland_consequences`).
- `product_kind` — varchar, required. Enum `export_product_kind`: `deterministic | distribution_parameters | pre_sampled_realizations`. Determines the row grain and the required-field set.
- `required_fields` — jsonb, required. Array of `{name, type, units, code_enum_ref, description, source_selector, imputation_policy}`. `source_selector` names the canonical-side value the field pulls from (e.g., `uncertainty_spec.attribute_name`, `geometries.role='point'`); `imputation_policy` is `null | {kind: 'class_default' | 'regional_default' | 'assumed', ref: '<lookup>'}`.
- `selection_rules` — jsonb, required. How to resolve alternates: geometry (`purpose='computation'` + `is_selected_for_export=true`), uncertainty (single spec per `uncertainty_ref` for that consumer), DDF (`ddf_library` version + peril + occupancy match).
- `sampling_policy` — jsonb, nullable. Required when `product_kind='pre_sampled_realizations'`: `{n_realizations, seed_strategy, shuffle_scope}`. Null otherwise.
- `access_level` — varchar, required. Enum `export_access_level`: `public | internal | restricted`. Restricted-source fields (ADR-restricted-sources, Prompt 6) are only emitted when the contract's access level permits.
- `version_id` — bigint FK → `versioning.version_id`, required.
- `status` — varchar, required. Enum `export_contract_status`: `active | deprecated`.
- `description` — text, nullable.
- `created_at` — timestamptz, required.

`export_product`

- `export_product_id` — bigint PK.
- `run_id` — bigint FK → `run_catalog.run_id`, required. The run that produced this export.
- `export_contract_id` — bigint FK → `export_contract.export_contract_id`, required. Exactly one contract version per product (Prompt 11 rule).
- `inventory_snapshot_id` — bigint FK → `versioning.version_id`, required. The canonical inventory snapshot the export was compiled from.
- `product_kind` — varchar, required. Must equal the contract's `product_kind` at bind time (Prompt 11 rule).
- `output_uri` — varchar, required. Object-store URI (S3 / GCS) of the produced artifact.
- `source_checksum` — varchar, required. SHA-256 of the concatenated source inputs (canonical rows) at compile time — supports the "run snapshots are immutable" invariant.
- `compiler_version` — varchar, required. Semver of the export compiler that produced this artifact.
- `seed_root` — bigint, nullable. Root RNG seed; required when `product_kind='pre_sampled_realizations'`.
- `n_realizations` — integer, nullable. Draw count; required when `product_kind='pre_sampled_realizations'`.
- `row_count` — bigint, required. Rows in the emitted artifact.
- `status` — varchar, required. Enum `export_status`: `queued | running | success | failed`.
- `started_at`, `finished_at` — timestamptz, nullable.
- `created_at` — timestamptz, required.

`export_lossiness_report`

- `lossiness_id` — bigint PK.
- `export_product_id` — bigint FK → `export_product.export_product_id`, required.
- `discarded_entity_type` — varchar, required. Enum `lossiness_entity_type`: `geometries | uncertainty_spec | building_components | ddf_uncertainty | hazard_links | uncertainty_ref`. Polymorphic pointer (no hard FK; validated in app/CI).
- `discarded_entity_id` — bigint, required. Id in the table named by `discarded_entity_type`.
- `reason` — varchar, required. Enum `lossiness_reason`: `not_selected_for_export | alternative_representation | superseded_by_default | redacted_restricted | out_of_scope_for_contract | collapsed_to_central_estimate`.
- `description` — text, nullable. Free-text elaboration (e.g., "Microsoft footprint not selected; NSI footprint chosen per contract rule §3.2").
- `created_at` — timestamptz, required.

### Product-kind semantics

**Deterministic snapshot** — one row per building; every uncertain attribute collapsed to its central estimate (spec median / mean depending on `imputation_policy`). Reproduces the current flat workflow exactly. No seed. No draws.

**Distribution-parameter export** — one row per building; uncertain attributes emitted as `(dist_family, parameters_jsonb)` pairs resolved through the distribution registry. Engine samples internally. No seed at export time; the engine records its own seeds.

**Pre-sampled realizations** — one row per `(building, event/AEP, realization_id)`; the compiler samples using `seed_root + realization_id` derivation and writes the draws directly. `loss_realizations` is the Iceberg-tier target for the sampled artifact, but a per-run object-store dump is also an export product.

A single run may produce multiple export products (e.g., a distribution-parameter export for archival and a deterministic snapshot for the current engine) — each is a separate `export_product` row bound to its own contract.

### Imputation responsibility — the hard boundary

**All cross-source selection, defaulting, and imputation happens once, in the governed export compiler.** Loss engines consume the emitted product as-is; they do not re-impute.

The mechanism uses existing tables. When the compiler imputes:

1. It creates a `prov_activity` row with `activity_type='impute'` and `method='class_default'` (or `regional_default`, `assumed`).
2. It creates an `adopted_value_link` with `is_default_or_assumed=true`, `observation_id=null`, and a required `assumption_rationale`.
3. The `imputation_policy` field on the contract's `required_fields` names the lookup rule the activity used (deterministic, auditable).

There is one **documented exception**: an engine may apply a *contract-declared post-processing transform* (e.g., unit conversion, code-value remapping) when the contract's `required_fields.transform_ref` names it. Anything not named there is out of bounds. This is the only escape hatch.

Locked invariant, stated verbatim for downstream teams: *"If your engine needs a value the export didn't provide, the fix is in the contract, not the engine."*

### Run/export manifest binding

The `export_product` row itself is the manifest. It pins:

- `inventory_snapshot_id` (canonical snapshot),
- `export_contract_id` (contract version),
- `compiler_version` (transformation code version),
- `source_checksum` (source inputs at compile time),
- `run_id` (which itself points at `manifests.manifest_id` — engine + software config),
- `seed_root` and `n_realizations` (when sampled).

Combined with `run_catalog.manifest_id → manifests.engine_version` and the `versioning` row for the inventory snapshot, this is the reproducibility five-tuple: inventory + contract + compiler + engine + seed. Serves RQM-09.

### Machine-readable lossiness report

Every export writes `export_lossiness_report` rows for anything the contract dropped:

- Alternate geometries not selected (`reason='alternative_representation'`).
- Uncertainty specs collapsed to central estimate in a deterministic export (`reason='collapsed_to_central_estimate'`).
- Components below the contract's coverage floor (`reason='out_of_scope_for_contract'`).
- Values sourced from a restricted classification the contract can't emit (`reason='redacted_restricted'`; Prompt 6).
- Values imputed rather than observed (`reason='superseded_by_default'`).

Consumers can reconstruct what the export chose to leave out without diffing the canonical database against the flat file.

### Enum additions

- `export_product_kind`: `deterministic | distribution_parameters | pre_sampled_realizations`.
- `export_contract_status`: `active | deprecated`.
- `export_status`: `queued | running | success | failed`.
- `export_access_level`: `public | internal | restricted`.
- `lossiness_reason`: `not_selected_for_export | alternative_representation | superseded_by_default | redacted_restricted | out_of_scope_for_contract | collapsed_to_central_estimate`.
- `lossiness_entity_type`: `geometries | uncertainty_spec | building_components | ddf_uncertainty | hazard_links | uncertainty_ref`.
- `entity_type`: add `export_contract` (versioned via `versioning`).

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

1. Every `export_product.export_contract_id` resolves to an `active` contract at `started_at`.
2. `export_product.product_kind` equals its bound contract's `product_kind` at bind time.
3. `export_product.seed_root` and `n_realizations` are non-null iff `product_kind='pre_sampled_realizations'`.
4. Every field in `export_contract.required_fields[*].name` resolves to a canonical source (spec/attribute/geometry role) via its `source_selector`; unmapped required fields fail the export.
5. Every emitted row satisfies the contract's `required_fields[*].type` and `units`; violations produce an `export_lossiness_report` row and fail the product.
6. Every imputed value (marked in the emitted row's metadata) has a matching `prov_activity(activity_type='impute')` + `adopted_value_link(is_default_or_assumed=true)` pair with non-null `assumption_rationale`.
7. No field classified `restricted` may be emitted by a contract whose `access_level != 'restricted'` (Prompt 6 tie-in). Redactions produce `lossiness_reason='redacted_restricted'`.
8. Exactly one `export_contract` is bound per `export_product` (no null, no multiple).
9. `export_product.source_checksum` is non-empty for every `status='success'` product (reproducibility invariant).

## Worked examples

### Example 1 — Deterministic flat export (current engine)

```
export_contract  {export_contract_id=1, name='inland_consequences_flat_v1',
                  target_engine='inland_consequences',
                  product_kind='deterministic',
                  required_fields=[
                    {name:'building_id', type:'bigint', source_selector:'buildings.building_id'},
                    {name:'first_floor_height', type:'real', units:'ft',
                     source_selector:"uncertainty_ref[consumer='buildings',attribute='first_floor_height']",
                     imputation_policy:{kind:'class_default', ref:'HAZUS TM v6 §4.3'}},
                    {name:'foundation_type', type:'varchar', source_selector:"uncertainty_ref[...,attribute='foundation_class']"},
                    {name:'lat', type:'real', source_selector:"geometries[role='point',purpose='computation',is_selected=true].ST_Y"},
                    {name:'lon', type:'real', source_selector:"geometries[...same...].ST_X"},
                    ...
                  ],
                  access_level='public', status='active', version_id=...}

export_product   {export_product_id=1001, run_id=42, export_contract_id=1,
                  inventory_snapshot_id=555, product_kind='deterministic',
                  output_uri='s3://rqm/exports/run-42/flat.parquet',
                  source_checksum='sha256:abc...', compiler_version='0.1.0',
                  row_count=124_318, status='success'}
```

Numerical-equivalence claim: for the same `inventory_snapshot_id` and contract, this product reproduces the current flat workflow row-for-row. Non-goal: preserving column *order* — order is a contract detail, not an invariant.

### Example 2 — Distribution-parameter export

```
export_contract  {export_contract_id=2, product_kind='distribution_parameters',
                  required_fields=[
                    ...,
                    {name:'ffh_dist_family', type:'varchar', source_selector:"uncertainty_spec_continuous.dist_family"},
                    {name:'ffh_parameters', type:'jsonb',    source_selector:"uncertainty_spec_continuous.parameters"},
                    ...
                  ], sampling_policy=null, ...}

export_product   {export_product_id=1002, run_id=43, export_contract_id=2,
                  product_kind='distribution_parameters', seed_root=null,
                  n_realizations=null, ...}
```

Engine samples internally. No seed at export time — the engine records its own seed via its own `manifests` binding.

### Example 3 — Pre-sampled realizations

```
export_contract  {export_contract_id=3, product_kind='pre_sampled_realizations',
                  sampling_policy={n_realizations:1000, seed_strategy:'seed_root + realization_id',
                                   shuffle_scope:'inventory_locked'}, ...}

export_product   {export_product_id=1003, run_id=44, export_contract_id=3,
                  product_kind='pre_sampled_realizations',
                  seed_root=987654321, n_realizations=1000,
                  output_uri='iceberg://ffrd/rqm/loss_realizations?run_id=44', ...}
```

Each row of the emitted artifact is reproducible from `(seed_root, realization_id)`; the Iceberg tier is the natural landing zone but object-store dumps are equally valid products.

### Example 4 — Rejected: engine tries to re-impute

An engine reads `first_floor_height` = null in the flat file (compiler chose not to impute for this record — e.g., the class default was disabled by contract override). The engine's SOP says "default to 1.0 ft." The boundary is violated: the engine's default silently disagrees with the next engine's default.

Correct fix: update `export_contract.required_fields[first_floor_height].imputation_policy` and re-run the compiler. The engine defaults are dead code.

### Example 5 — Lossiness report for a discarded alternate footprint

```
export_lossiness_report {
  export_product_id=1001, discarded_entity_type='geometries',
  discarded_entity_id=502, reason='alternative_representation',
  description='Allegheny cadastre footprint not selected; NSI footprint chosen per contract §3.2 (source preference: NSI > cadastre for RES1).'
}
```

The alternate geometry from Prompt 4's Example 2 is preserved in the canonical model *and* accounted for in the lossiness report.

## Claim discipline

- The contract-not-code pattern is *research-recommends* (standard export-contract pattern; `dbt` contracts, Iceberg schemas, PROV-O adopted-value chains) + *project-requires* (the flat-engine boundary is real; every downstream engine calls out contract stability).
- The single-imputation-boundary is *project-requires* — the reviewers raised this as ISSUE-Q06. The mechanism (existing `prov_activity` + `adopted_value_link`) is *specification-allows*.
- Product-kind trio (`deterministic | distribution_parameters | pre_sampled_realizations`) is *project-requires* — the three shapes are what FFRD calibration + engine + BCA workflows actually need. Pre-sampled matches `loss_realizations` grain by design.
- Lossiness report as first-class table is a deliberate *project-requires*: the alternative (logs, engine-side diffs, ad-hoc QC) is what created the current opacity.
- Contract JSON shape (`required_fields`, `selection_rules`, `sampling_policy`) is a *v0.1 recommendation*; the JSON-Schema formalization of these shapes is deferred to Prompt 11.

## Alternatives considered

- **Contract inline on `run_catalog`.** Rejected — contracts are shared across many runs and evolve independently; embedding on run rows duplicates and drifts.
- **Fold `export_contract` into `manifests`.** Rejected — `manifests` pins engine + config *for a run*; contract pins the *artifact shape*, which is independent of run configuration. Two concerns, two tables.
- **Let engines impute with a "recommended default" registry.** Rejected — every engine grows its own recommendation, and reproducibility is lost. The single boundary is the point.
- **One export product per run (no multi-product runs).** Rejected — calibration workflows commonly emit deterministic + parameter exports from the same run.
- **Skip the lossiness report; use logs.** Rejected — logs are not queryable, not versioned, and not attached to the artifact.
- **Enforce `required_fields` via SQL schema on the emitted artifact.** Rejected for v0.1 — enforce at compile time in Prompt 11's validator; SQL-level enforcement can be added later without a schema change (it's downstream of the same jsonb).

## Impact on other ADRs

- **ADR-versioning:** `export_contract` becomes a versioned entity (`entity_type='export_contract'`); every product cites a specific `version_id`. Immutable-run-snapshot invariant now has a concrete pin point: `(inventory_snapshot_id, contract version_id, compiler_version, source_checksum, seed_root)`.
- **ADR-provenance:** imputation flows through the existing `prov_activity(activity_type='impute')` + `adopted_value_link(is_default_or_assumed=true)` bridge — no new provenance table, just a new caller (the compiler). Rule 6 makes it enforceable.
- **ADR-uncertainty:** distribution-parameter exports read directly from `uncertainty_spec_continuous` / `_categorical` / `_deterministic` via `uncertainty_ref` — the shareable pattern is exactly what a contract's `source_selector` needs. Deterministic exports collapse using `spec.mean()` or `spec.ppf(0.5)` per `imputation_policy`.
- **ADR-distribution-registry:** the compiler renders parameter payloads through the registry's `sampler_ref` for pre-sampled products. The registry version is pinned indirectly via `inventory_snapshot_id`.
- **ADR-ddf-uncertainty:** for pre-sampled products, `ddf_percentile` in `loss_realizations` is drawn per row using the spread spec; for deterministic products, the compiler emits `damage_mean` inline. DDF percentile columns as rendered cache remain the fallback for legacy percentile-only envelopes.
- **ADR-geometry:** the contract's `selection_rules` for geometry consume `is_selected_for_export=true` on the `(asset_id, purpose='computation')` row. Alternates produce `lossiness_reason='alternative_representation'` rows.
- **ADR-restricted-sources (Prompt 6):** `export_contract.access_level` gates emission; restricted-derived fields in a `public` contract are redacted and reported in lossiness. This ADR pre-declares the mechanism; Prompt 6 formalizes the source classification.

## Deferrals

- **JSON-Schema formalization** of `required_fields` / `selection_rules` / `sampling_policy` shapes. Prompt 11 territory.
- **Multi-tenant contract governance** — who approves a contract version bump, review workflow. Out of scope for v0.1.
- **Cross-contract diff tooling.**
- **Delta / incremental exports** (only-changed-rows since last snapshot). v0.2.
- **Streaming exports.**
- **Engine-specific contract validators.** External tools; the contract itself is engine-agnostic.
- **Column-order stability.** Column order is a contract detail; not a locked invariant.
