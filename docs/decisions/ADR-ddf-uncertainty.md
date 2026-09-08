# ADR — DDF Uncertainty as True Distribution

**ID:** ADR-ddf-uncertainty
**Status:** Proposed
**Date:** 2026-09-03
**Extends:** ADR-uncertainty, ADR-distribution-registry
**Supersedes (partially):** the inline `dist_family` / `parameters` columns that ADR-distribution-registry added to `ddf_uncertainty`. Direct FK to `distribution_registry` on `ddf_uncertainty` is replaced by an indirect path through `uncertainty_spec_continuous`.
**Resolves:** Prompt 2B of the RQM issue-resolution prompt pack — the outstanding shape-drift on `ddf_uncertainty` after ADR-uncertainty formalized the superclass and ADR-distribution-registry formalized the contract.
**Serves user stories:** RQM-08, RQM-13, RQM-14, RQM-15, RQM-17
**Locks:** deterministic value is a valid degenerate form of uncertainty (inherited); parameter contracts are enforced by the registry (inherited); the DDF is itself a distribution — not a single mean curve.

## Context

`ddf_uncertainty` sits in an awkward middle state after Prompts 2 and 2A:

- It carries a per-depth **percentile envelope** (`damage_mean`, `damage_p10`, `damage_p50`, `damage_p90`) — the shape published USACE / FIA / HAZUS tables use.
- ADR-distribution-registry then bolted on inline `dist_family` + `parameters` columns so the spread could be *parametric* when known, with the registry contract enforcing shape.
- But ADR-uncertainty formalized a superclass (`uncertainty_spec`) that every other consumer — attributes on `buildings`, `building_components`, `hazard_links` — already binds through, either directly (typed FK) or through the polymorphic `uncertainty_ref`.

`ddf_uncertainty` is the last uncertainty consumer that does *not* speak uncertainty_spec. Three consequences:

1. **Two different shapes for "the DDF spread at depth d is lognormal(μ,σ)":** inline columns on `ddf_uncertainty`, or an `uncertainty_spec_continuous` reached through `uncertainty_ref`. Analysts have to know which convention their DDF was authored in.
2. **No sharing.** A class-level DDF-spread spec (e.g., "USACE RES1 riverine damage-ratio spread is lognormal with σ growing 0.02/ft above the FF elevation") cannot be authored once and bound to many `(ddf_id, depth_ft)` rows. Every row re-declares its own family + parameters.
3. **Provenance and versioning drift.** `uncertainty_spec` versions via `versioning` and carries validity intervals + fitting-method + description. The inline columns on `ddf_uncertainty` carry none of that — the spread has no independent version.

The reviewer-driving user story RQM-13 (DDF sensitivity ensembles) needs the DDF spread to be **shareable, versioned, and typed the same way every other uncertainty is** so a Monte Carlo run can swap "authored" spread for "wide" spread by version-bumping one spec, not by editing 3,000 rows.

## Decision

Elevate `ddf_uncertainty` to reference an `uncertainty_spec` — the same superclass every other uncertainty consumer uses. Concretely:

### 1. `spread_spec_id` — nullable FK to `uncertainty_spec.uncertainty_spec_id`

- **When set:** the spec is the authoritative definition of the damage-ratio spread at this `(ddf_id, depth_ft)`. Its subclass row (`uncertainty_spec_continuous`, `_categorical`, or `_deterministic`) carries the family, parameters, and — via the registry — the compute backend.
- **When null:** back-compat with USACE / FIA / HAZUS-style published DDFs that publish percentiles only. The `damage_p10 / p50 / p90` columns are then the only representation of the spread.

The spec is a full `uncertainty_spec`. Continuous is the expected common case (lognormal, beta, truncated_normal spread around the mean). Deterministic is the degenerate "zero uncertainty at this depth" form. Categorical is not expected for DDFs but is not forbidden (a future ADR could use it for classed-damage envelopes).

### 2. `damage_mean` remains required inline

Every DDF row publishes a mean — that is what USACE / FIA / HAZUS publish, and it is the canonical central tendency downstream aggregators (`mv_loss_summary.loss_mean`) reduce against. Making `damage_mean` optional would break every DDF ingested from an external library. So: `damage_mean` stays inline, non-null, and independent of the spread spec. The spec describes the *distribution around* `damage_mean`, not `damage_mean` itself.

Uncertainty on `damage_mean` itself (i.e., a distribution of means) is out of scope for this ADR — deferred to a possible future "DDF ensemble" ADR.

### 3. `damage_p10 / p50 / p90` become a rendered cache

- When `spread_spec_id` is null, the percentile columns are the authoritative representation.
- When `spread_spec_id` is set, the percentile columns are a **rendered cache** — the p10 / p50 / p90 evaluated from the spec at ingest / rebuild time using the registry's `ppf_ref`. The spec is authoritative; the columns exist for fast reads and for legacy tools that only speak percentiles.
- Prompt 11 will validate cache freshness at row-insert time — `|cache − ppf(spec, q)| < ε` for q ∈ {0.1, 0.5, 0.9} — and reject silent drift.

### 4. Remove the inline `dist_family` and `parameters` columns from `ddf_uncertainty`

Added in ADR-distribution-registry as a stopgap; superseded by `spread_spec_id`. Rationale: two overlapping representations of the same thing (inline family + parameters vs spec reference) is exactly the shape-drift ADR-uncertainty was written to eliminate. Removing them keeps one path: the spec, always. The registry contract still applies — it just applies through `uncertainty_spec_continuous.dist_family` rather than directly on `ddf_uncertainty`.

This is a fresh-repo v0.1 change: no ingested data yet, no downstream engines depending on the inline columns. The trade-off is entirely on the ADR-writing side.

### 5. Sharing model

Two ways a DDF row can bind to a shared spread spec:

- **Row-local FK.** `ddf_uncertainty.spread_spec_id → uncertainty_spec.uncertainty_spec_id`. Fastest lookup, one FK, per-row. Works when each row picks its own spec.
- **Polymorphic ref.** `uncertainty_ref(consumer_type='ddf_uncertainty', consumer_id=ddf_unc_id, attribute_name='damage_ratio_spread')`. Same mechanism `buildings` and `hazard_links` already use. Preferred when a class-level spec is bound to many DDF rows in one place.

Both are valid. Prompt 11 will enforce that a DDF row does not carry both a `spread_spec_id` and a conflicting `uncertainty_ref` for `attribute_name='damage_ratio_spread'`.

## What is chosen vs deferred

**Chosen (in-scope):**
- `spread_spec_id` FK on `ddf_uncertainty` to `uncertainty_spec`.
- `damage_mean` stays required inline.
- Percentile columns become a rendered cache when a spec is present; authoritative when not.
- Inline `dist_family` / `parameters` on `ddf_uncertainty` are removed (superseded).
- `uncertainty_ref` with `consumer_type='ddf_uncertainty'` remains available for class-level binding.

**Deferred:**
- **Uncertainty on `damage_mean` itself.** A distribution of means (rather than a mean plus spread) is a different shape and would require either a second spec column or a rethink of `damage_mean` as a spec reference. Not in scope.
- **Depth-parametric single-spec DDFs.** One spec per DDF whose `parameters` are functions of `depth_ft` (e.g., `sigma = 0.02 * depth`). Requires a parameter-expression grammar the registry does not yet support. Deferred to v0.2.
- **DDF ensemble registry.** A typed registry of empirical DDF ensembles (analogous to the empirical distribution family in the registry). Deferred; today the DDF library is a single-curve-per-version registry.
- **Cross-depth spread correlation.** Whether the spread draw at depth d₁ is correlated with the spread draw at depth d₂ during a single realization. Today: assumed independent within a DDF row; cross-depth shape draws come from re-sampling. A proper correlated-along-depth model is out of scope.
- **Categorical DDF spread.** Not forbidden by the schema, but no worked example is provided.

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

1. Every `ddf_uncertainty.spread_spec_id` (when non-null) resolves to an existing `uncertainty_spec`; that spec's `kind` matches its subclass row.
2. When `spread_spec_id` is set and `kind='continuous'`, the referenced `uncertainty_spec_continuous.dist_family` resolves in `distribution_registry` and satisfies the family's `parameter_contract` (inherited from ADR-distribution-registry rule 4).
3. When `spread_spec_id` is set AND `damage_p10 / p50 / p90` are non-null, the cache values are within ε (default 1e-3 damage-ratio units, configurable) of the spec's `ppf(spec, q)` at the row's `depth_ft` — or the row is rejected with a stale-cache error.
4. `damage_mean` is always non-null.
5. A `ddf_uncertainty` row does not carry both a `spread_spec_id` AND an `uncertainty_ref` row with `consumer_type='ddf_uncertainty'`, `consumer_id=this.ddf_unc_id`, `attribute_name='damage_ratio_spread'` pointing at a different spec — one of the two, not both with conflict.
6. If `spread_spec_id` is null AND `damage_p10 / p50 / p90` are all null, the row is a **point DDF** (mean-only, no uncertainty). Explicit and legal; flagged by Prompt 11 as a diagnostic, not an error.

## Worked examples

### Example 1 — Shared class-level DDF spread bound via `uncertainty_ref`

A single lognormal spread spec is authored once for USACE RES1 riverine building DDFs and bound to every `(ddf_id, depth_ft)` row of that DDF.

```
uncertainty_spec  {id=610, kind='continuous', attribute_name='damage_ratio_spread',
                   units='ratio', scope='class_level',
                   scope_ref='USACE-RES1-riverine-building',
                   fitting_method='MLE on 2018 flood-loss claims',
                   version_id=...}

uncertainty_spec_continuous {uncertainty_spec_id=610, dist_family='lognormal',
                             parameters={mu:0.0, sigma:0.35},
                             support_type='unit_interval',
                             lower_bound:0.0, upper_bound:1.0}

For each of the 27 depth rows of DDF id=88:

ddf_uncertainty {ddf_unc_id=..., ddf_id=88, depth_ft=..,
                 damage_mean=0.42, spread_spec_id=610,
                 damage_p10=0.28, damage_p50=0.42, damage_p90=0.60}

uncertainty_ref {ref_id=..., consumer_type='ddf_uncertainty',
                 consumer_id=<ddf_unc_id>,
                 attribute_name='damage_ratio_spread',
                 uncertainty_spec_id=610}
```

One spec, one version_id, one place to bump when the calibration ensemble is refit. The percentile cache is derived from spec 610 at rebuild time.

### Example 2 — Row-local spec (per-depth different shape)

A DDF where the spread widens with depth: each row references its own spec so the shape at depth 8ft can differ from the shape at depth 1ft.

```
uncertainty_spec  {id=711, kind='continuous',
                   attribute_name='damage_ratio_spread',
                   scope='building_specific', version_id=...}
uncertainty_spec_continuous {uncertainty_spec_id=711, dist_family='beta',
                             parameters={alpha:2, beta:5}, support_type='unit_interval'}

ddf_uncertainty {ddf_unc_id=..., ddf_id=88, depth_ft=1.0,
                 damage_mean=0.05, spread_spec_id=711, ...}

uncertainty_spec  {id=712, kind='continuous', ...}
uncertainty_spec_continuous {uncertainty_spec_id=712, dist_family='beta',
                             parameters={alpha:2, beta:2}, support_type='unit_interval'}
                             -- flatter shape: uncertainty is wider deeper

ddf_uncertainty {ddf_unc_id=..., ddf_id=88, depth_ft=8.0,
                 damage_mean=0.72, spread_spec_id=712, ...}
```

Per-depth shape variation is expressible without waiting for the deferred depth-parametric grammar.

### Example 3 — Point DDF (mean only, no uncertainty)

Ingested from a published table that only publishes a mean curve.

```
ddf_uncertainty {ddf_unc_id=..., ddf_id=101, depth_ft=4.0,
                 damage_mean=0.31,
                 spread_spec_id=NULL,
                 damage_p10=NULL, damage_p50=NULL, damage_p90=NULL}
```

Prompt 11 flags this as a diagnostic (not an error) so analysts can see which DDFs still lack an uncertainty envelope.

### Example 4 — Legacy USACE-style envelope with no spec (percentile-only)

```
ddf_uncertainty {ddf_unc_id=..., ddf_id=42, depth_ft=3.0,
                 damage_mean=0.22,
                 spread_spec_id=NULL,
                 damage_p10=0.14, damage_p50=0.22, damage_p90=0.34}
```

Legal. Analysts can later fit a family (lognormal / beta / triangular) to those percentiles and attach a `spread_spec_id` without dropping the row.

### Example 5 — Rejected row (stale percentile cache)

A row references a spec that would render p90 = 0.55, but the cache column reads 0.60.

```
uncertainty_spec_continuous {id=610, dist_family='lognormal',
                             parameters={mu:0.0, sigma:0.35}}
                             -- ppf(0.90) ≈ 0.55

ddf_uncertainty {..., spread_spec_id=610,
                 damage_mean=0.42, damage_p90=0.60}   -- STALE
```

Prompt 11 rejects with `[ddf_uncertainty.damage_p90] cache diverges from ppf(spec:610, 0.90)=0.55 by 0.05; refresh cache or clear column.`

## Claim discipline

- Elevating `ddf_uncertainty` to reference `uncertainty_spec` is *project-requires* — RQM-13 explicitly needs shareable, versioned DDF-spread specs.
- Keeping `damage_mean` inline (rather than folding it into the spec too) is *specification-allows* + a compatibility choice: published DDFs already come with a mean per depth, and it is what downstream aggregators reduce against.
- Percentile-column-as-rendered-cache is *research-recommends* (standard "materialized quantile" pattern) with an explicit staleness check.
- Removing the inline `dist_family` / `parameters` columns is a v0.1 clean-up choice, safe only because nothing has been ingested against them yet.
- The point-DDF diagnostic (no spec, no percentiles) is *project-requires* — teams need to see which DDFs still have zero uncertainty at v0.1 so they can be prioritized for fitting.

## Alternatives considered

- **Keep the inline `dist_family` / `parameters` on `ddf_uncertainty`** (added in ADR-distribution-registry). Rejected — two paths for the same information (inline vs spec) is exactly the shape-drift ADR-uncertainty was written to eliminate.
- **Make `damage_mean` a spec reference too.** Rejected — every published DDF ships a scalar mean per depth; the inline column is the least-surprise representation. A future ADR can add an "uncertainty-on-the-mean" column pair.
- **One spec per DDF (not per (ddf, depth)).** Rejected — the shape of the damage-ratio distribution genuinely changes with depth, and depth-parametric expression grammar is deferred. Per-row specs are the honest fit.
- **Route everything through `uncertainty_ref` only, no direct FK.** Rejected — the direct FK is typed, cheap, and enforceable at DB level. `uncertainty_ref` remains available for class-level sharing; it is not the only path.
- **Drop the percentile columns entirely once a spec is present.** Rejected — legacy tools consume percentile columns, and a null cache would force every reader to render from the spec at read time.

## Impact on other ADRs

- **ADR-uncertainty:** the `uncertainty_ref.consumer_type` enum already lists `ddf_uncertainty` (pre-committed in that ADR at line 157). No further change needed there.
- **ADR-distribution-registry:** the direct FK from `ddf_uncertainty.dist_family` to `distribution_registry.family_name` is removed. The registry contract still governs DDF spreads — indirectly, through `uncertainty_spec_continuous.dist_family`. The registry's role does not shrink; the path just becomes one hop longer.
- **ADR-provenance:** DDF-spread provenance now attaches to the *spec* (`adopted_entity_type='uncertainty_spec'`), not to `ddf_uncertainty`. A class-level DDF-spread spec fitted from 500 flood-loss claims has one adopted-value link, not one per depth row.
- **ADR-versioning:** DDF-spread specs version via `versioning.version_id` on the spec, independently of the DDF library. A calibration refit bumps the spec's semver without republishing the DDF library.
- **ADR-export-contract (Prompt 5):** the compiler pins `(ddf_id, spread_spec_id, spec.version_id)` per exported row so a reader can render the DDF distribution the same way at export time — even if the spec is later refit.
- **ADR-restricted-sources (Prompt 6):** unchanged. If a DDF-spread spec is fitted from restricted claims data, the restriction lives on the observations feeding the spec, not on the DDF row.
