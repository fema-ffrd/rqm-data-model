# ADR — Uncertainty as a Generalized, Shareable Class

**ID:** ADR-uncertainty
**Status:** Proposed
**Date:** 2026-09-03
**Resolves:** ISSUE-Q02, ISSUE-R01
**Serves user stories:** RQM-08, RQM-14, RQM-15, RQM-17
**Locks:** deterministic value is a valid degenerate form of uncertainty; one spec may be reused by many consumers.

## Context

The current model represents attribute uncertainty in three inconsistent shapes:

- `attribute_distributions` — a continuous-focused table with `dist_family` (varchar) and `parameters` (jsonb), bound to `building_id` (+ optional `component_id`). Each row is independent; no sharing.
- `foundation_pmf` — a separate categorical construct with per-(building, class) rows and a `shuffle_policy`.
- `ddf_uncertainty` — depth-damage percentiles with an optional inline `dist_family`/`parameters`.
- `hazard_links.depth_dist_family` + `depth_parameters` — inline distribution again, keyed off the building × grid link.

ISSUE-R01 (rjp3k) explicitly asks for uncertainty to be a general class with continuous/categorical children that any consumer — building attributes, components, DDFs, and future extensions such as building value — can *reference*, not four independent shapes. ISSUE-Q02 asks which uncertainty models are building-specific vs regional/class-level and which correlations must be preserved.

## Decision

Introduce a **generalized uncertainty superclass** with typed children, made **shareable** by a polymorphic reference table, and drop the four ad-hoc shapes.

### Superclass + subclasses

- `uncertainty_spec` (superclass) — identity, scope, kind, validity, units, version. `kind` is the discriminator.
- `uncertainty_spec_continuous` (subclass, `kind='continuous'`) — parametric family (references the distribution registry from Prompt 2A) + parameters + support / bounds. Also carries an optional `empirical_ensemble_ref` for empirical distributions.
- `uncertainty_spec_categorical` (subclass, `kind='categorical'`) — PMF as `{category: probability}` jsonb + optional `shuffle_policy`.
- `uncertainty_spec_deterministic` (subclass, `kind='deterministic'`) — a single value (scalar or category). Preserves the locked invariant that a deterministic value is a *valid degenerate form of an uncertainty spec*, not a special case handled elsewhere.

Every `uncertainty_spec` has exactly one row in exactly one subclass table (SQL supertype/subtable pattern).

### Shareable reference

- `uncertainty_ref` — polymorphic bridge: `(consumer_type, consumer_id, attribute_name) → uncertainty_spec_id`. This says "consumer X uses spec Y for attribute Z." A single spec can be referenced by many consumers (building-specific → 1 consumer; class-level → many; regional → many within the region).
- Consumers today (`enum consumer_type`): `buildings`, `building_components`, `ddf_uncertainty` (per-depth increment; formalized in Prompt 2B), `hazard_links` (depth-in-structure), and a `virtual` value for future consumers (e.g., building value).

### Correlations

- `uncertainty_correlation` — captures at least the *conditional* form: "spec A's parameters are conditional on the drawn value of spec B" (e.g., FFH conditional on foundation class). Supports:
  - `kind='conditional'` — `specification` jsonb of `{category_of_b: parameters_for_a}` for categorical-→-continuous, or a keyed map for continuous binning. Sampler draws B first, then selects A's parameters.
  - `kind='correlation'` — Pearson/Spearman coefficient between two continuous specs (a rank-preserving copula not yet modeled).
  - `kind='independent'` — an explicit declaration of independence when needed (useful in QA to prove absence of coupling was intentional).

Full **copula** modeling and multivariate joint specs are **deferred**; the ADR calls it out.

### Scope (building-specific vs class/regional/shared)

`uncertainty_spec.scope` is a small enum:
- `building_specific` — the spec is for exactly one building's attribute (the ref binds it).
- `class_level` — one spec for all buildings of an occupancy/foundation/peril class (bind to many consumers via many refs).
- `regional` — one spec for all buildings in a geography (bind to many).
- `shared` — a spec meant to be widely reused (e.g., a HAZUS class default).
`uncertainty_spec.scope_ref` — the class label / region key when applicable.

The scope field is documentation and QA; the *actual* sharing is expressed by how many `uncertainty_ref` rows point at a given spec.

## What is removed

- `attribute_distributions` — replaced by `uncertainty_ref` (binding) + `uncertainty_spec_continuous`/`_categorical`/`_deterministic` (definition). The `conditioning` jsonb column is replaced by explicit `uncertainty_correlation` rows.
- `foundation_pmf` — replaced by a `uncertainty_spec_categorical` (holding the PMF and shuffle policy) + one `uncertainty_ref` per building. Per-class rows collapse into a single jsonb column on the categorical spec.

## Deferred (explicitly)

- Full copula / joint multivariate specs beyond pairwise conditional / correlation coefficient.
- Bayesian-network representation of dependencies.
- Time-varying uncertainty (uncertainty specs that themselves evolve over time). Version bumps via the existing `versioning` table are sufficient today.

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

1. Every `uncertainty_spec.uncertainty_spec_id` has exactly one row in the subclass table named by its `kind`.
2. Every `uncertainty_ref.uncertainty_spec_id` resolves to an existing spec.
3. `uncertainty_ref.(consumer_type, consumer_id)` resolves to an existing row in the table named by `consumer_type` (polymorphic-FK check in app/CI, same pattern as `adopted_value_link`).
4. Every `uncertainty_spec_categorical.categories_pmf` sums to 1.0 within ±1e-6.
5. Every `uncertainty_spec_continuous.parameters` includes the required parameter names for its `dist_family` (contract defined by the distribution registry in Prompt 2A).
6. Every `uncertainty_correlation.spec_a_id` and `spec_b_id` resolve to existing specs; specification jsonb schema must match the declared `kind`.

## Worked examples

### Example 1 — Shared class-level FFH (RES1) reused across many buildings

```
uncertainty_spec  {id=201, kind='continuous', attribute_name='first_floor_height',
                  units='ft', scope='class_level', scope_ref='RES1', version_id=...}
uncertainty_spec_continuous {uncertainty_spec_id=201, dist_family='lognormal',
                             parameters={mu:1.5, sigma:0.4}, support_type='positive'}

uncertainty_ref {ref_id=1, consumer_type='buildings', consumer_id=42,
                 attribute_name='first_floor_height', uncertainty_spec_id=201}
uncertainty_ref {ref_id=2, consumer_type='buildings', consumer_id=43,
                 attribute_name='first_floor_height', uncertainty_spec_id=201}
...
```

One spec, many refs — the sharing pattern R01 asked for.

### Example 2 — Foundation PMF (was `foundation_pmf`)

```
uncertainty_spec  {id=305, kind='categorical', attribute_name='foundation_class',
                  scope='building_specific', version_id=...}
uncertainty_spec_categorical {uncertainty_spec_id=305,
                              categories_pmf={Slab:0.6, CrawlSpace:0.3, Basement:0.1},
                              shuffle_policy='region_correlated'}
uncertainty_ref  {ref_id=17, consumer_type='buildings', consumer_id=42,
                  attribute_name='foundation_class', uncertainty_spec_id=305}
```

### Example 3 — FFH conditioned on foundation (correlation preserved)

```
uncertainty_spec  {id=411, kind='continuous', attribute_name='first_floor_height', ...}
uncertainty_spec_continuous {uncertainty_spec_id=411, dist_family='lognormal',
                             parameters={mu:1.5, sigma:0.4}}
                             -- baseline; overridden per foundation via correlation

uncertainty_correlation
  {correlation_id=71, spec_a_id=411, spec_b_id=305, kind='conditional',
   specification={
     "Slab":       {mu:0.5, sigma:0.2},
     "CrawlSpace": {mu:2.0, sigma:0.3},
     "Basement":   {mu:0.3, sigma:0.15},
     "Pier":       {mu:3.0, sigma:0.5}
   },
   description='FFH conditional on foundation class draw'}
```

Sampler contract: draw spec 305 → get a category → look up per-category parameters for spec 411 → sample.

### Example 4 — Deterministic value as a valid uncertainty form

```
uncertainty_spec  {id=902, kind='deterministic', attribute_name='number_stories',
                  scope='building_specific', version_id=...}
uncertainty_spec_deterministic {uncertainty_spec_id=902, value='2', value_type='scalar'}
```

Number-of-stories pinned. Same reference pattern; downstream samplers treat it as a degenerate draw.

## Claim discipline

- The generalization is *research-recommends* (SQL supertype/subtable is standard) + *project-requires* (RQM-17 explicitly asks for continuous/categorical uncertainty that can be shared between attributes).
- Scope taxonomy is *research-recommends*; can be trimmed after real usage.
- Correlations table is *project-requires* to the extent that FFH-vs-foundation coupling is a documented modeling choice; broader coupling patterns are *research-recommends*.

## Alternatives considered

- **Keep the four current shapes; add a light shared-registry table.** Rejected — R01 asks explicitly for a general class, and the shape mismatch keeps DDF / hazard / attribute uncertainty from reusing the same sampler contract.
- **Single-table inheritance (kind + all-columns nullable).** Rejected — makes required parameters unenforceable and confuses categorical vs continuous fields.
- **Class-table inheritance with no superclass row (subclass tables carry identity).** Rejected — breaks the shareable identity that `uncertainty_ref` and `adopted_value_link` need.

## Impact on other ADRs

- **ADR-provenance:** the `adopted_entity_type` enum swaps `attribute_distributions` and `foundation_pmf` for `uncertainty_spec`. Provenance now attaches to the spec, not to each ref. If a spec is class-level and shared across 5,000 buildings, the sources supporting it live in one place.
- **ADR-distribution-registry (Prompt 2A):** `uncertainty_spec_continuous.dist_family` becomes a FK into the registry; parameter contracts are enforced there.
- **ADR-ddf-uncertainty (Prompt 2B):** `ddf_uncertainty` gains an `uncertainty_spec_id` column per depth increment, reusing the same superclass.
- **ADR-restricted-sources (Prompt 6):** a spec derived from a restricted source records that via provenance; the spec itself is not classified restricted.
