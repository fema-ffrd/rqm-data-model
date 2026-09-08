# ADR — Provenance Grain

**ID:** ADR-provenance
**Status:** Proposed
**Date:** 2026-09-03
**Resolves:** ISSUE-Q01
**Serves user stories:** RQM-04, RQM-07, RQM-09, RQM-16
**Locks:** observation ≠ adopted value; the two are linked by an activity carried out by an agent.

## Context

Issue #1 Q01 asks at what grain provenance must be captured — dataset, building, attribute value, geometry, or transformation activity. Current schema captures at most a `source` string and a `version_id` on each `attribute_distributions` row, and nothing on `buildings.geom`. That is insufficient for RQM-07 ("retain the sources, assumptions, defaults, and changes applied to asset data"), RQM-09 (asset-to-result linkage), and RQM-04 (source-inventory identifiers survive standardization).

The bar is not a full ontology. It is: given an adopted value, can we point at (1) the raw observations it came from, (2) the activity that derived it, (3) the agent responsible? And can we tell when an adopted value has *no* supporting observation and must be labeled a default/assumption?

## Decision

Adopt a **lightweight W3C PROV-O-inspired profile** at **attribute-value grain and geometry grain**, with dataset-level provenance recoverable as a rollup of an observation's `source_ref`. Four new tables carry it, all in the `provenance` domain:

1. `prov_agent` — who (person, organization, or software).
2. `prov_activity` — the derivation activity (import, fit, impute, override, calibrate), timestamped and attributed to an agent, with a `code_ref` for the tool/commit that ran it.
3. `source_observation` — an atomic raw observation from a named source: attribute observed, value, units, quality/confidence, effective time, recorded time, and agent.
4. `adopted_value_link` — the n:m bridge from any adopted entity (attribute distribution, foundation PMF, DDF, hazard link, geometry — see Prompt 4) to the source observations that support it, plus the derivation activity that produced it, plus an `is_default_or_assumed` flag with a mandatory rationale when true.

The polymorphic parent (`adopted_entity_type`, `adopted_entity_id`) uses the discriminator-on-relationship pattern (not a hard FK). This is consistent with the guardrail that supertype formalization is deferred until a demonstrated need appears; the discriminator is documented on the link table, and the enum `adopted_entity_type` fixes the allowed parents.

## Grain — chosen vs deferred

**Chosen (in-scope):**
- **Attribute-value grain** — every `attribute_distributions` row is an adopted value; provenance links point to it. Same treatment for `foundation_pmf` (as one adopted PMF spec, not per-class) and `hazard_links.depth_parameters`.
- **Geometry grain** — every geometry (currently `buildings.geom`, later formalized as a class in Prompt 4) can be linked via the same table. Until Prompt 4, `buildings` is registered as a valid `adopted_entity_type` and the link points at `building_id`; Prompt 4 will redirect it to `geometry_id`.
- **Transformation-activity grain** — `prov_activity` captures the "what happened" step.
- **Dataset-level provenance** — recoverable by rollup of `source_observation.source_ref`. No separate dataset entity today.

**Deferred (out-of-scope for this ADR):**
- Full PROV-O ontology (specific properties like `wasQuotedFrom`, `wasInformedBy`, bundles, provenance-of-provenance).
- Multi-step activity graphs. We record only the terminal adopted-value → source-observations link; intermediate entities in a chain are not modeled as separate provenance entities. The activity's `description` and `code_ref` are sufficient at RQM's current maturity.
- Entity-level revision within provenance (PROV-O `specializationOf`, `alternateOf`). RQM's existing `versioning` table handles revision; provenance points *to* whatever version is current, not through it.
- Building-level provenance as a distinct grain. A building is a container; provenance lives at the attribute and geometry values it carries.

## Validation rules (enforced by extended `preview_dict.py` — see §Validation extension)

1. Every `attribute_distributions.dist_id`, `foundation_pmf.pmf_id`, `hazard_links.hazard_link_id`, `ddf_uncertainty.ddf_unc_id`, and (post-Prompt-4) `geometries.geometry_id` must have **at least one** `adopted_value_link` row **OR** exactly one `adopted_value_link` row with `is_default_or_assumed = true` and a non-null `assumption_rationale`.
2. Every `source_observation` must have `agent_id`, `effective_time`, and a non-empty `source_ref`.
3. Every `prov_activity` must have `agent_id` and `activity_type`.
4. `adopted_value_link.adopted_entity_type` must be one of the enumerated parents; the corresponding `adopted_entity_id` must exist in that parent table.

Rules 1–3 are enforceable at insert time via check constraints and application logic. Rule 4 is the polymorphic-FK check that stays in application/CI code since there is no supertype table yet (would be revisited if Prompt 9 is later reintroduced).

## Worked example — an adopted first-floor height

A first-floor height for `building_id = 42` is adopted as a lognormal(μ=1.5, σ=0.4) spec:

```
attribute_distributions
  dist_id=901, building_id=42, attribute_name='first_floor_height',
  dist_family='lognormal', parameters={mu:1.5, sigma:0.4}

prov_activity
  activity_id=71, activity_type='fit',
  method='MLE on stratified NSI+field survey',
  agent_id=5, started_at=..., ended_at=..., code_ref='rqm-fit@a1b2c3'

source_observation
  observation_id=311, source_ref='NSI-2024.1/RES1', attribute_name='first_floor_height',
  observed_value={value:1.6}, units='ft', quality=0.7, effective_time=2024-01-01, agent_id=5
source_observation
  observation_id=312, source_ref='ALLEGHENY-FS-2023', attribute_name='first_floor_height',
  observed_value={value:1.4}, units='ft', quality=0.9, effective_time=2023-06-15, agent_id=6

adopted_value_link
  link_id=1001, adopted_entity_type='attribute_distributions', adopted_entity_id=901,
  observation_id=311, activity_id=71, role='primary', is_default_or_assumed=false
adopted_value_link
  link_id=1002, adopted_entity_type='attribute_distributions', adopted_entity_id=901,
  observation_id=312, activity_id=71, role='supporting', is_default_or_assumed=false
```

If instead the spec were a HAZUS class default, exactly one row:

```
adopted_value_link
  link_id=1003, adopted_entity_type='attribute_distributions', adopted_entity_id=901,
  observation_id=NULL, activity_id=71, role='primary', is_default_or_assumed=true,
  assumption_rationale='RES1 default from HAZUS TM v6 §4.3; no site observation.'
```

## Validation extension

`data-dictionary/preview_dict.py` currently validates FKs and PK presence only. This ADR adds rule 1 as a required extension. Implementation lands with Prompt 11 (synchronization + validation), not here, so this ADR only records the requirement. Rules 2 and 3 fall out of `NOT NULL` and enum checks and are enforced by the YAML declarations directly.

## Claim discipline

- Adding the four tables is *specification-allows* + *project-requires* (RQM-07 explicitly requires source traceability, RQM-16 requires editor + reason).
- The PROV-O framing is *research-recommends*: we borrow the entity/activity/agent shape, not the ontology.
- No production maturity is claimed. The tables are proposed; no data exists in them yet.

## Alternatives considered

- **Freeform JSONB `provenance` column on each versioned row.** Rejected: unqueryable at scale, cannot enforce "at least one observation OR flagged assumption," fights RQM-09.
- **Full PROV-O with bundles.** Rejected: no user story demands it; adds many joins and terminology no reviewer will use.
- **Dataset-level provenance only (a single `source` string per row, as today).** Rejected: fails RQM-07 for anything derived from >1 source, and cannot express "adopted value" vs "raw observation."
- **Supertype table for adopted values now.** Deferred with Prompt 9; the discriminator-on-relationship pattern serves today's need.

## Impact on other prompts

- **Prompt 2 / 2A / 2B (uncertainty superclass, distribution registry, DDF true distribution):** the uncertainty_spec entity introduced there also becomes a valid `adopted_entity_type`. That's a one-enum-value addition, not a redesign.
- **Prompt 3 (versioning):** the `revision_log` table introduced there records *editor + reason for a change to a value*. This ADR records *sources of the value itself*. They are complementary. Cross-linked via the versioned entity's id.
- **Prompt 4 (geometry as its own class):** the polymorphic parent gains `geometries` and drops `buildings` for geometry provenance.
- **Prompt 5 (export contract):** the compiler's imputation activities are `prov_activity` rows; the `is_default_or_assumed=true` flag flows into the export's lossiness report.
- **Prompt 6 (restricted sources):** `source_observation.source_ref` becomes an FK to a new `source_ref` table with `access_class`; adopted values derived from restricted sources retain the link but the underlying observation can be redacted at export time.
