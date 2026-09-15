# ADR — Restricted-Source Handling (Reference & Policy, Never Data)

**ID:** ADR-restricted-sources
**Status:** Proposed
**Date:** 2026-09-08
**Extends:** ADR-provenance, ADR-exports
**Resolves:** ISSUE-Q07
**Serves user stories:** RQM-04, RQM-07, RQM-10
**Locks:** no sensitive/protected content lives in the repo docs folder; restricted sources are referenced, classified, and masked — never embedded; do not place any real restricted data anywhere in the repo.

## Context

Some source datasets that inform RQM inventories carry access constraints — tribal-owned parcel data, licensed commercial building attributes, PII in field surveys, DoD/agency-restricted structure catalogs. The canonical model still needs to record that a value *came from* such a source (for provenance, calibration audit, and dispute resolution), *without* the underlying protected record ever entering this repository or any public deliverable derived from it.

Today the model has one place to record source: `source_observation.source_ref` (varchar). It has no classification. That means:

- A compiler cannot mechanically distinguish public sources from restricted ones — the same varchar could refer to either.
- A public export could carry a value whose *only* observational support is restricted, with no formal redaction mechanism.
- Stewardship (who owns the access decision) is folded into free-text `notes`.

ISSUE-Q07 asks for a reference-only pattern that keeps the reference + classification + steward in the model, keeps the protected values out, and gates emission through the export layer already introduced in ADR-exports.

## Decision

Introduce a `source_registry` table as the single policy surface for every source referenced anywhere in the model. Every `source_observation.source_ref` becomes an FK into this registry. Classification and steward are mandatory. Restricted raw values never live in the schema — only the *reference to* the restricted source, the *adopted derivative* (which is not itself restricted), and the *policy pointer* (URI to an external document, not embedded).

### New table

`source_registry`

- `source_ref` — varchar(128) PK. Natural key; matches `source_observation.source_ref`. Deliberate varchar-PK parallel to `distribution_registry.family_name` — readability at reference sites (`source_ref='ALLEGHENY-restricted-parcels-2023'`) beats an opaque id.
- `source_name` — varchar(128), required. Human-readable dataset name.
- `source_kind` — varchar(32), required. Enum `source_kind`: `national_inventory | class_default | field_survey | remote_sensing | commercial_parcel | tribal | proprietary | agency_restricted | other`.
- `access_classification` — varchar(16), **required, no default**. Enum `access_classification`: `public | internal | restricted`.
- `steward_agent_id` — bigint FK → `prov_agent.agent_id`, required. The party who owns access decisions for this source. Stewardship is a person or organization, not a role — it must resolve to a real `prov_agent` row.
- `policy_uri` — varchar(256), nullable. External pointer to the access/handling policy document (SharePoint, contract portal, data-use agreement). Deliberately *not* an in-repo path — see the locked invariant.
- `restricted_reason` — text, nullable. Required when `access_classification='restricted'` (Prompt 11 rule). Free-text: why restricted (PII / proprietary / security-sensitive / tribal-owned / …). Contains rationale, not protected values.
- `description` — text, nullable. Free-text; may not include restricted content.
- `version_id` — bigint FK → `versioning.version_id`, required. Classification can be re-evaluated; changes bump a new version rather than mutating the existing row.
- `created_at` — timestamptz, required.

### Changes to `source_observation`

- `source_ref` becomes an FK → `source_registry.source_ref` (was free-text varchar). Ingest that references an unknown source must first register the source.
- `observed_value` semantics tighten: when the referenced registry row has `access_classification='restricted'`, the observation row exists to record *that an observation was consulted* — but `observed_value` MUST be null or a documented redaction placeholder. The raw restricted value never lands in Postgres.
- New column: `is_redacted` — boolean, required (default false). True when the observation references a restricted source and the raw value has been withheld. Independent of `observed_value` being null so a caller can tell "null because no observation" from "null because redacted."

### Adopted-value semantics under a restricted source

When an adopted value is *derived from* a restricted observation:

1. The `source_observation` row records the reference (`source_ref` FK into `source_registry`) + `is_redacted=true`.
2. The derivation `prov_activity` records how the adopted value was computed (`activity_type ∈ {fit, derive, impute}`; `method` names the transform).
3. The `adopted_value_link` binds the (non-restricted) adopted value to the redacted observation and the activity. Because the adopted value is a *derivative* (a class default, a regional average, a fitted parameter) and not the raw restricted attribute, it is safe to carry through the canonical model.
4. If no non-restricted derivative is possible for a required field, the adopted value is treated as absent and the export path chooses its response per `export_contract.access_level`.

Verbatim rule for downstream teams: *"The reference to a restricted source is public; the derivative computed from it may be public; the raw value is not, and it is never in this repository."*

### Export behavior

`export_contract.access_level` (already introduced in ADR-exports) gates emission:

- **`public`** — a field whose adopted value is supported *only* by restricted observations is emitted as null and produces one `export_lossiness_report` row with `reason='redacted_restricted'`. A field with mixed support (some restricted, some public) emits the public-supported derivative and reports the restricted-source contribution as `redacted_restricted` metadata.
- **`internal`** — restricted-derived adopted values are emitted (they are derivatives, not raw); restricted raw values are still not present because they are not stored anywhere.
- **`restricted`** — same as `internal` for this schema. Distinguishes a downstream distribution channel that requires additional access checks, but does not unlock a store of raw restricted values (there is none).

No contract at any access level ever emits a raw restricted value, because none is stored. That is the whole point.

### Enum additions

- `access_classification`: `public | internal | restricted`.
- `source_kind`: `national_inventory | class_default | field_survey | remote_sensing | commercial_parcel | tribal | proprietary | agency_restricted | other`.
- `entity_type`: add `source_registry` (versioned via `versioning`).

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

1. Every `source_observation.source_ref` resolves to a `source_registry` row.
2. `source_registry.access_classification` is non-null for every row (classification is mandatory).
3. When `source_registry.access_classification='restricted'`: `restricted_reason` is non-null; every `source_observation` citing this row has `is_redacted=true` and `observed_value` either null or a documented redaction placeholder.
4. Every `source_registry` row has a `steward_agent_id` (no ownerless sources).
5. A `public` `export_contract` may not resolve a required field solely through restricted observations. Contracts must declare a non-restricted fallback (class default, regional default, imputation_policy) or the compile fails.
6. Every compiled export that redacts a restricted-derived field writes one `export_lossiness_report` row with `reason='redacted_restricted'` referencing the discarded observation (`discarded_entity_type` extended to include `source_observation`; see Impact §).
7. `source_registry.policy_uri`, when set, must not be an in-repo path (starts with `http`, `https`, `s3://`, `sharepoint://`, or another external scheme). In-repo policy embedding is rejected at CI time.
8. No `source_observation.observed_value` may be non-null when its `source_registry` classification is `restricted` (enforced as a data-load-time check; Prompt 11 surfaces violations).

## Worked examples

### Example 1 — Register a restricted commercial parcel source

```
source_registry {source_ref='COMMERCIAL-PARCEL-VENDOR-2024Q2',
                 source_name='Vendor X Commercial Parcel Attributes (2024 Q2)',
                 source_kind='commercial_parcel',
                 access_classification='restricted',
                 steward_agent_id=17,  -- points at prov_agent row for the data owner
                 policy_uri='https://vendor-x.example.com/data-use-agreement',
                 restricted_reason='Licensed data; per DUA §4, raw attributes may not be stored or re-distributed. Derivatives at census-block aggregation are permitted.',
                 version_id=..., created_at=...}
```

### Example 2 — Observation from that source, redacted

```
source_observation {observation_id=901,
                    source_ref='COMMERCIAL-PARCEL-VENDOR-2024Q2',
                    attribute_name='replacement_cost',
                    observed_value=null,  -- restricted; raw value never stored
                    is_redacted=true,
                    units='USD',
                    effective_time='2024-04-01T00:00Z',
                    recorded_at='2024-06-15T00:00Z',
                    agent_id=17,
                    notes='Reference-only record: raw cost withheld per source policy. Adopted derivative (block-level mean) at adopted_value_link:link_id=5501.'}
```

The observation exists so that provenance can trace back to a specific dataset and time. No protected number is in this repo.

### Example 3 — Adopted derivative published as public

```
prov_activity      {activity_id=302, activity_type='derive',
                    method='census_block_mean',
                    agent_id=17, code_ref='rqm-derive@a1b2c3', ...}

uncertainty_spec_deterministic {uncertainty_spec_id=812,
                                value={"amount": 245_000},
                                value_type='scalar'}

adopted_value_link {link_id=5501,
                    adopted_entity_type='uncertainty_spec',
                    adopted_entity_id=812,
                    observation_id=901,   -- redacted observation
                    activity_id=302,
                    role='primary',
                    is_default_or_assumed=false,
                    assumption_rationale=null,
                    created_at=...}
```

The *adopted* value (block-level mean $245 K) is emitable through a `public` contract because the derivative is DUA-compliant. The provenance chain still traces "derived from `COMMERCIAL-PARCEL-VENDOR-2024Q2`" without ever exposing per-parcel figures.

### Example 4 — Public export redacts a restricted-only field

Suppose a required field `parcel_owner_name` is supported *only* by restricted observations and no non-restricted fallback is declared.

```
export_contract    {export_contract_id=1, access_level='public',
                    required_fields=[..., {name:'parcel_owner_name', ...}], ...}

-- compiler tries to compile
-- Prompt 11 rule 5 rejects: public contract has a field with no non-restricted fallback.
-- Fix: either drop parcel_owner_name from the contract, or declare an imputation_policy.
```

If we instead declared `imputation_policy={kind:'assumed', ref:'null-owner'}`:

```
export_product     {export_product_id=1001, export_contract_id=1, ...}

export_lossiness_report {lossiness_id=7001,
                         export_product_id=1001,
                         discarded_entity_type='source_observation',
                         discarded_entity_id=901,
                         reason='redacted_restricted',
                         description='parcel_owner_name derived from COMMERCIAL-PARCEL-VENDOR-2024Q2 (restricted). Public contract emits null per contract §4.1.'}
```

Downstream consumers can enumerate exactly what was redacted and why, without seeing the underlying restricted content.

### Example 5 — Internal export emits the derivative

Same run, `access_level='internal'` contract. The block-level mean $245 K flows through (it is a public-safe derivative under the DUA), while the raw per-parcel figures still do not exist in the repo. No lossiness row is written for the derivative — it is emitted as intended.

### Example 6 — Rejected: attempt to store a raw restricted value

```
source_observation {source_ref='COMMERCIAL-PARCEL-VENDOR-2024Q2',
                    observed_value={"amount": 312_500},   -- raw per-parcel figure
                    is_redacted=false, ...}
```

Prompt 11 rule 8 rejects: `access_classification='restricted'` requires `observed_value=null` and `is_redacted=true`. Ingest is blocked before commit; the operator is directed to register a redacted stub + a derivative activity instead.

## Claim discipline

- The registry-as-policy-surface pattern is *research-recommends* (FAIR data / PROV-O / catalog patterns) + *project-requires* (ISSUE-Q07 asks for reference + classification + steward as first-class model concerns).
- `access_classification` as a small enum is *project-requires*. `restricted` is intentionally not sub-typed (`restricted:tribal`, `restricted:PII`, etc.) in v0.1 — the rationale lives in `restricted_reason` as free text; a taxonomy is a possible v0.2 upgrade if downstream tooling needs it.
- Varchar PK on `source_registry.source_ref` is a deliberate parallel to `distribution_registry.family_name`; readability at every reference site, no opaque ids.
- The redacted-observation shape (nullable `observed_value` + required `is_redacted`) is *project-requires* — the observation must persist as a reference even though the value cannot.
- The "internal ≡ restricted at this schema layer" rule is *specification-allows*, not *implementation-demonstrates* — distinguishing the two channels is a distribution/deployment concern, not a schema concern.

## Alternatives considered

- **Add `access_classification` inline on `source_observation`.** Rejected — classification is per-source, not per-observation; storing it inline would let two observations from the same source diverge in classification.
- **Store restricted raw values encrypted at rest.** Rejected — violates the locked invariant that restricted content is not in this repository at all. Encryption + key management is a separate deployment problem the *canonical schema* refuses to take on.
- **Skip the redacted-observation stub; store no row at all when the source is restricted.** Rejected — provenance breaks. The whole point of ISSUE-Q07 is that the *reference* is required even when the value is not.
- **Embed the access policy text in the registry row.** Rejected — the policy document is external (DUA, agency memo). `policy_uri` points at it; the registry does not become a policy library.
- **Fold `source_registry` into `prov_agent`.** Rejected — an agent is a party (person, org, software); a source is a dataset. Distinct concepts, both referenced by `source_observation`.
- **Auto-classify from `source_kind`.** Rejected — a `commercial_parcel` source could be either public or restricted depending on licensing; a `national_inventory` could be internal (pre-release). Classification is a policy choice on the specific source, not a derived attribute.

## Impact on other ADRs

- **ADR-provenance:** `source_observation.source_ref` becomes an enforced FK; `is_redacted` is a new column; the "observation ≠ adopted value" invariant now holds even when the observation is a redacted stub. Rule 6 references `source_observation` under a lossiness discriminator, so `lossiness_entity_type` gains `source_observation`.
- **ADR-exports:** the `access_level` gate pre-declared there now has its counterpart at the source layer. `export_lossiness_report.reason='redacted_restricted'` is populated by this pathway. Rule 5 (public contract must have non-restricted fallback) is the compile-time enforcement.
- **ADR-versioning:** `source_registry` is versioned; a re-classification bumps a new `version_id` rather than mutating classification in place, so historical exports remain reconstructable.
- **ADR-uncertainty:** unchanged. Adopted derivatives (class default, regional average) that supersede a restricted observation are stored as `uncertainty_spec_deterministic` or a fitted `uncertainty_spec_continuous` — the same shapes any other adopted value uses.
- **ADR-geometry:** a geometry adopted from a restricted source uses the same redacted-observation + adopted-derivative pattern; e.g., a restricted parcel footprint is redacted at the observation, and the adopted footprint (public census-block-aligned polygon) rides through `adopted_value_link` with `adopted_entity_type='geometries'`.
- **ADR-distribution-registry:** unaffected — the registry stays public/reference data; a spec derived from a restricted source is classified via provenance, not by hiding a family name.
- **ADR-ddf-uncertainty:** unaffected — DDF sources are typically public (USACE / FIA / HAZUS). If a private DDF becomes relevant, it uses this same pattern.

## Deferrals

- **Fine-grained classification taxonomy** (`restricted:tribal`, `restricted:PII`, `restricted:proprietary`). Deferred; today lives in `restricted_reason` free text.
- **Row-level access control** in Postgres (RLS policies enforcing `access_classification`). Deployment concern; the schema exposes the columns the policy needs.
- **Audit log for classification changes.** `revision_log` covers structural change (which version), but who read which restricted-derivative when is a runtime audit concern, not schema.
- **Distinct treatment of `internal` vs `restricted` distribution channels.** Both emit derivatives; the deployment layer decides distribution.
- **Automated policy enforcement from `policy_uri`** (parse DUAs to derive contract-emitable fields). Out of scope.
- **Cryptographic hashing of the raw restricted value for dedup** (without storing the value). Out of scope for v0.1.
