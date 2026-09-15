# ADR — Distribution Rendering & Compute Registry

**ID:** ADR-distribution-registry
**Status:** Proposed
**Date:** 2026-09-03
**Extends:** ADR-uncertainty
**Resolves:** parameter-contract enforcement gap identified in ADR-uncertainty §Impact on other ADRs (Prompt 2A of the RQM issue-resolution prompt pack).
**Serves user stories:** RQM-08, RQM-14, RQM-15, RQM-17
**Locks:** parameter contracts are enforced by the registry (not per-consumer); deterministic value remains a valid degenerate form of uncertainty (unchanged from ADR-uncertainty).

## Context

ADR-uncertainty made every uncertain attribute a shareable `uncertainty_spec` with a `dist_family` label and a `parameters` jsonb. But `dist_family` today is just a controlled string (enum `dist_family`) — nothing enforces that the payload in `parameters` matches the declared family. A `lognormal` row could carry `{mu, sigma}`, `{loc, scale}`, or `{a, b, c}` and the FK-only validator in `preview_dict.py` cannot tell.

Three consuming tables have the same shape-drift risk:

- `uncertainty_spec_continuous.(dist_family, parameters)`
- `hazard_links.(depth_dist_family, depth_parameters)`
- `ddf_uncertainty.(dist_family, parameters)`

We also do not have a canonical pointer to the **compute backend** that renders samples / PDF / CDF from those parameters. Every downstream engine has to hard-code the mapping (`lognormal → scipy.stats.lognorm.rvs(...)`), which drifts across services and makes reproducibility a code question rather than a data question.

## Decision

Introduce a single `distribution_registry` table that:

1. Enumerates every parametric distribution family the model supports.
2. Declares the parameter contract each family expects — canonical parameter names, JSON types, constraints.
3. Points at the canonical compute backend (sampler / PDF / CDF / PPF references).
4. Is versioned via `versioning`.

Consuming tables keep their `dist_family` varchar column; the column gains a foreign key to `distribution_registry.family_name`. `family_name` is used as the natural primary key — this trades the schema's surrogate-PK convention for readability at every reference site (`dist_family='lognormal'` reads better than `dist_family_id=7`).

### Schema

`distribution_registry`
- `family_name` — varchar PK (natural key; the string every consumer already writes).
- `family_kind` — `continuous | discrete | empirical` (enum `distribution_family_kind`).
- `parameter_contract` — jsonb array of `{name, json_type, required, constraint, description}`. Canonical parameter names for the family; a spec's `parameters` jsonb must obey this.
- `support_type` — `real | positive | unit_interval | bounded | integer_nonneg | empirical` (enum `distribution_support_type`).
- `default_lower_bound`, `default_upper_bound` — real, nullable; support hints, overridable per-spec.
- `sampler_backend` — `scipy | numpy | custom` (enum `sampler_backend`).
- `sampler_ref` — varchar; canonical callable reference (e.g., `scipy.stats.lognorm.rvs`).
- `pdf_ref`, `cdf_ref`, `ppf_ref` — varchar, nullable; canonical callables when the backend exposes them.
- `status` — `active | deprecated` (enum `distribution_family_status`).
- `version_id` — FK `versioning.version_id`.
- `description` — text.
- `citations` — text, nullable.

### Contract enforcement

Prompt 11's validator, using the registry's `parameter_contract`, will enforce:

- Every consumer's `parameters` jsonb contains every parameter marked `required=true` in the referenced family's contract, with JSON types matching the declared `json_type`.
- Each `constraint` sub-object (e.g., `{"gt":0}` for `sigma`) is satisfied by the corresponding value in `parameters`.

This moves parameter validation from "hoped-for" (a comment in the ADR) to "enforced" (a data-side contract preview_dict.py can check).

### Seed content (v0.1)

Ten seed families cover the current enum without loss:

- **Continuous** — `normal`, `truncated_normal`, `lognormal`, `uniform`, `triangular`, `beta`, `gamma`, `weibull`.
- **Discrete** — `bernoulli`.
- **Empirical** — `empirical` (parameters empty; `empirical_ensemble_ref` on the spec carries the reference).

`categorical` and `deterministic` remain handled by their respective subclass tables (`uncertainty_spec_categorical`, `uncertainty_spec_deterministic`) — those tables carry structural PMF / pinned-value columns rather than a parametric jsonb, so a registry entry would be empty. The subclass tables are their own contract.

## What is chosen vs deferred

**Chosen (in-scope):**
- Central `distribution_registry` with parameter contract + compute backend pointers.
- FK from each of the three consumer sites to `family_name`.
- Enum coverage of the current `dist_family` enum (seed rows enumerated above).
- Version-tracked via `versioning` (registry entries can evolve with a bumped semver).

**Deferred:**
- **User-defined / custom distribution families.** `sampler_backend='custom'` is reserved but the registration workflow (who can register, review, deprecate) is out of scope.
- **GPU / vectorized sampler variants.** The registry stores a canonical backend; deployment can route to alternates.
- **Multivariate parametric families** (multivariate normal, copulas). Deferred with the copula deferral in ADR-uncertainty.
- **PDF / CDF / PPF consistency checks** across the three ref pointers.
- **Rich parameter-constraint DSL.** Today: `{gt, ge, lt, le, in}` sub-object per parameter. A JSON-Schema-based expression grammar is a possible v0.2 upgrade.
- **Empirical ensemble registry** (typed metadata for `empirical_ensemble_ref` targets). Deferred; empirical refs today are opaque URIs.

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

1. Every `uncertainty_spec_continuous.dist_family` resolves to a `distribution_registry.family_name` with `status='active'` at the referencing time.
2. Every `hazard_links.depth_dist_family` resolves to a `distribution_registry.family_name`.
3. Every `ddf_uncertainty.dist_family` (when non-null) resolves to a `distribution_registry.family_name`.
4. Every consumer's `parameters` (or `depth_parameters`) jsonb contains each parameter marked `required=true` in the referenced family's `parameter_contract`, with JSON type matching `json_type`.
5. Every `constraint` sub-object in `parameter_contract` is satisfied by the corresponding value in `parameters`.
6. `distribution_registry.parameter_contract` is a non-empty array iff `family_kind != 'empirical'`.
7. `distribution_registry.sampler_ref` is non-null for every `status='active'` row; `pdf_ref` / `cdf_ref` / `ppf_ref` may be null when the backend does not expose them.

## Worked examples

### Example 1 — Lognormal family and a spec that references it

```
distribution_registry  {family_name='lognormal', family_kind='continuous',
                        parameter_contract=[
                          {"name":"mu","json_type":"number","required":true,
                           "description":"mean of the natural log"},
                          {"name":"sigma","json_type":"number","required":true,
                           "constraint":{"gt":0},
                           "description":"std-dev of the natural log"}
                        ],
                        support_type='positive', default_lower_bound=0.0,
                        sampler_backend='scipy',
                        sampler_ref='scipy.stats.lognorm.rvs',
                        pdf_ref='scipy.stats.lognorm.pdf',
                        cdf_ref='scipy.stats.lognorm.cdf',
                        ppf_ref='scipy.stats.lognorm.ppf',
                        status='active', version_id=...}

uncertainty_spec_continuous {uncertainty_spec_id=201, dist_family='lognormal',
                             parameters={mu:1.5, sigma:0.4}, support_type='positive'}
```

Validator: `mu` present (number), `sigma` present (number), `sigma > 0` — pass.

### Example 2 — A spec the validator rejects

```
uncertainty_spec_continuous {uncertainty_spec_id=202, dist_family='lognormal',
                             parameters={mu:1.5}}
```

Missing required `sigma` → Prompt 11 rejects with `[uncertainty_spec_continuous.parameters] required key 'sigma' missing for family 'lognormal'`.

### Example 3 — Empirical family, no parameters

```
distribution_registry  {family_name='empirical', family_kind='empirical',
                        parameter_contract=[], support_type='empirical',
                        sampler_backend='custom',
                        sampler_ref='rqm.samplers.empirical.draw',
                        status='active', version_id=...}

uncertainty_spec_continuous {dist_family='empirical', parameters={},
                             empirical_ensemble_ref='iceberg://ffrd/ensembles/xyz'}
```

Validator allows empty `parameters` when `family_kind='empirical'`; separately requires `empirical_ensemble_ref` non-null.

### Example 4 — Same registry row serves hazard_links

```
hazard_links {hazard_link_id=99, building_id=42, depth_dist_family='lognormal',
              depth_parameters={mu:0.8, sigma:0.3}, ...}
```

Same contract, same backend — one source of truth for what `lognormal` means, wherever it appears.

## Claim discipline

- The registry pattern is *research-recommends* (standard type-class / family-table pattern, cf. `scipy.stats`) + *project-requires* (Prompt 11's validator needs a data-side contract).
- `family_name` as varchar PK is a deliberate deviation from the schema's surrogate-PK convention; documented here and paid back by readability at every reference site.
- Seed family list is *project-requires* (the current enum) — expansion is an insert, not a schema change.
- The parameter-contract shape (`{name, json_type, required, constraint, description}`) is *research-recommends* — the minimal shape a checker needs; richer grammars are deferred.

## Alternatives considered

- **Keep `dist_family` as a plain enum; put contracts in code.** Rejected — Prompt 11 needs a data-side contract to validate against; code-side contracts drift from the schema silently.
- **Bigint surrogate PK.** Rejected — introduces opaque IDs at every reference site without payoff; readability of `dist_family='lognormal'` wins.
- **Per-consumer contract tables** (e.g., a contract on `uncertainty_spec_continuous`, another on `hazard_links`). Rejected — duplicates the same knowledge three ways.
- **Full JSON Schema payload instead of a `parameter_contract` array.** Rejected for v0.1 — a small structured array renders and validates more simply; a JSON-Schema payload is a possible v0.2 upgrade.
- **Fold the compute registry into `manifests`.** Rejected — `manifests` pins the engine *version* for a given run; the registry is a *contract* independent of any run.

## Impact on other ADRs

- **ADR-uncertainty:** `uncertainty_spec_continuous.dist_family` becomes an FK into this registry. Parameter contract enforcement moves from prose to code.
- **ADR-provenance:** unaffected — the registry is public/reference data; provenance still attaches to specs, observations, and adopted values.
- **ADR-versioning:** registry rows are versioned via `versioning.version_id`; a family whose contract changes bumps its version rather than being edited in place.
- **ADR-ddf-uncertainty (Prompt 2B):** the FK from `ddf_uncertainty.dist_family` will remain valid whether Prompt 2B keeps the inline family field or replaces it with a full `uncertainty_spec_id` reference.
- **ADR-restricted-sources (Prompt 6):** the registry stays public; a spec derived from a restricted source is classified via provenance, not by hiding a family name.
- **ADR-export-contract (Prompt 5):** the compiler pins `(dist_family, parameters)` at export time; the registry version stamped in the export manifest guarantees the reader can render the distribution the same way.
