# RQM Data Model — Consistency & Validation Report

**Date:** 2026-09-08
**Scope:** synchronize + validate the state Prompts 1 → 8A left behind. No new modeling decisions.

## Summary

| Check | Result |
|---|---|
| YAML tables | **35** |
| YAML columns | **303** |
| ERD entity blocks | **35** |
| Tables in YAML but not ERD | (none) |
| Tables in ERD but not YAML | (none) |
| Markdown regenerated from YAML | ✅ passes `preview_dict.py` |
| Every table has description + grain | ✅ |
| Every column has type + description | ✅ |
| Every table's domain is in `metadata.domains` | ✅ |
| Every table's domain renders in Markdown output | ✅ (see drift fix below) |
| Every postgres table has a PK | ✅ |
| Every FK resolves to `table.column` | ✅ |
| SQL supertype/subtable identity naming | ✅ (subclass PK == superclass PK name) |
| Mermaid ERD render sanity | ✅ (35 balanced entity blocks; relationships block-structured) |

Command:

```
python data-dictionary/preview_dict.py
# OK: validated 35 tables, 303 columns. Wrote data-dictionary.md.
```

## Drift fixed in this pass

1. **`preview_dict.py` was missing an `exports` entry in `DOMAIN_TITLES`.** Domain G tables (`export_contract`, `export_product`, `export_lossiness_report`, added in Prompt 5) parsed and validated but did not render in the generated Markdown. Fixed by adding `"exports": "G — Exports & Contracts"` to `DOMAIN_TITLES`. Domain G now renders (verified: `data-dictionary.md` line 632, `## Domain G — Exports & Contracts`).

2. **`preview_dict.py` did not enforce description / grain / type presence.** Added four new static checks:
   - Every table has a non-empty `description`.
   - Every table has a non-empty `grain`.
   - Every column has a non-empty `type`.
   - Every column has a non-empty `description`.
   All existing rows already satisfy these — the checks catch regressions in future edits.

3. **`preview_dict.py` did not check domain declaration.** Added a check that every table's `domain` is listed in `metadata.domains`, and that every used `domain` has a matching `DOMAIN_TITLES` key (so no table can be added to a domain that would silently disappear from the Markdown output).

4. **`preview_dict.py` did not enforce the SQL supertype/subtable naming rule.** The `uncertainty_spec_*` subclasses and `generic_building_component` all declare their identity column as **both** `pk: true` and `fk: <superclass>.<same-column-name>`. Added a check that this naming discipline holds — a subclass PK column that FKs a superclass must share the parent's PK name (identity inheritance). Prevents a future "subclass" from silently drifting into a plain FK-child.

## New validation rules added

These are schema-level, run at every `preview_dict.py` invocation:

- Table `description` and `grain` presence.
- Column `type` and `description` presence.
- Table `domain` ∈ `metadata.domains`.
- Table `domain` ∈ `DOMAIN_TITLES` (rendering-safety check).
- SQL supertype/subtable identity name match (`pk: true` + `fk: X.Y` requires `column_name == Y`).

## Validation rules deferred to runtime / ingest-time

The ADRs accumulated a large body of *data-content* rules that this static validator cannot check — they need populated rows. These are enumerated verbatim by ADR so the runtime validator (or CI-integrated data check) has a single list to work from.

### ADR-uncertainty
- Every `uncertainty_spec` has exactly one row in exactly one of `uncertainty_spec_{continuous, categorical, deterministic}` (kind discriminator match).
- `uncertainty_spec_categorical.categories_pmf` sums to 1.0 ± 1e-6.
- Every `uncertainty_ref.consumer_type` ∈ `uncertainty_consumer_type` enum; `consumer_id` resolves in the table named by `consumer_type`.

### ADR-distribution-registry
- Every `uncertainty_spec_continuous.dist_family` resolves to a `distribution_registry` row with `status='active'`.
- Every `hazard_links.depth_dist_family` resolves to an active registry row.
- Every `parameters` (or `depth_parameters`) jsonb satisfies the referenced family's `parameter_contract`: required parameters present, JSON types match, constraints satisfied.
- `distribution_registry.parameter_contract` is a non-empty array iff `family_kind != 'empirical'`.
- `distribution_registry.sampler_ref` non-null for every `status='active'` row.

### ADR-ddf-uncertainty
- `damage_mean` required inline on every `ddf_uncertainty` row.
- When `spread_spec_id` is set: `damage_p10/p50/p90` (when non-null) satisfy `|cache − spec.ppf(q)| ≤ 1e-3` — stale caches rejected.
- Point DDF (both `spread_spec_id` and all percentile columns null) is legal — flag as diagnostic, not error.
- Simultaneous `spread_spec_id` FK AND `uncertainty_ref` binding for the same row is rejected (conflicting binding).

### ADR-versioning
- Every revision has an editor `agent_id`, `created_at`, and non-null `reason` on the `revision_log` row.
- `valid_from < valid_to` when both set; `record_from` non-null when referenced by an `asset_revision`; `record_to` null iff current record.
- No revision overwrites a prior — every version bump inserts a new `versioning` row + `revision_log` row.
- Exactly one `asset_revision` per `(asset_id, branch_id)` has `is_current=true` at any given valid-time.

### ADR-geometry
1. Non-null `role`, `crs`, `geom`, `validity_version_id`.
2. Exactly one row per `(asset_id, purpose='computation')` has `is_selected_for_export=true` per branch/valid-time.
3. `geometries.component_id`, when non-null, resolves via `building_components.building_id → buildings.asset_id` to match `geometries.asset_id`.
4. `alt_group_key`: rows sharing a key share `(asset_id, role)`.
5. Every `buildings` row has ≥ 1 `geometries` row with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`.
6. `geom` payload kind consistent with `role` (`ST_GeometryType`).
7. Adopted-value links use `adopted_entity_type='geometries'` (placeholder `buildings` migrated at v0.1 cutover).

### ADR-exports
1. `export_product.export_contract_id` → active contract at `started_at`.
2. `export_product.product_kind` == bound contract's `product_kind`.
3. `seed_root` and `n_realizations` non-null iff `product_kind='pre_sampled_realizations'`.
4. Every contract `required_fields[*].name` resolves via `source_selector`.
5. Emitted rows satisfy `type` and `units`.
6. Every imputed value has matching `prov_activity(activity_type='impute')` + `adopted_value_link(is_default_or_assumed=true)` with non-null `assumption_rationale`.
7. Restricted-classified fields not emitted by contracts with `access_level != 'restricted'` (redacted; lossiness row written).
8. Exactly one contract bound per product.
9. `source_checksum` non-empty for every `status='success'` product.

### ADR-restricted-sources
1. Every `source_observation.source_ref` resolves to a `source_registry` row (now an FK — enforced by DB).
2. `source_registry.access_classification` non-null for every row.
3. `access_classification='restricted'` requires `restricted_reason` non-null; every citing observation has `is_redacted=true` and `observed_value` null or a documented placeholder.
4. Every `source_registry` row has a `steward_agent_id`.
5. `public` `export_contract` may not resolve a required field solely through restricted observations — must declare a non-restricted fallback.
6. Every restricted redaction writes an `export_lossiness_report` row with `reason='redacted_restricted'`.
7. `policy_uri`, when set, is an external scheme (`http`, `https`, `s3`, `sharepoint`, …), not an in-repo path.
8. No `observed_value` may be non-null when the referenced source is `restricted`.

### ADR-citygml-profile
1. `component_connection` endpoints both resolve.
2. Both endpoints belong to the same `building_id`.
3. No self-connection (`from != to`).
4. `adjacent_to`: at most one row per unordered `{from, to}` pair.
5. Component-geometry parent exists (restates ADR-geometry rule 3).
6. LOD promotion: acquiring an `envelope_3d`/`detailed_3d` geometry must not orphan components that already have lower-role geometries.
7. Footprint covers point (`ST_Covers(footprint.geom, point.geom)`), tolerance TBD — diagnostic in v0.1.

### ADR-generics
1. Every `generics` row with `kind='building_component'` has exactly one matching `generic_building_component` row.
2. Every `building_components.generic_id` → `generics.kind='building_component'`.
3. Category-kind consistency (`building_component` ⇒ `category='Buildings'`; `infrastructure_component` ⇒ `category ∈ {Transportation, Utilities, Communications, Water}`).
4. No orphaned `generic_building_component` rows (FK-enforced by definition).
5. Deferred kinds (`infrastructure_component`, `uncertainty_class`, `future_asset_kind`) may exist as `generics` rows but must not be referenced by any consumer FK.
6. `typical_replacement_cost_fraction` in `[0, 1]` when non-null.

### ADR-performance
1. `docs/review/perf-benchmark.md` measured cells must be JSON-backed by files under `tools/bench/results/`. Fabricated numbers rejected at CI.

## Polymorphic patterns — discriminators documented

Three polymorphic bridges exist in the model. Each has a discriminator column (enum) + an untyped id column (no hard FK; validated in app/CI). Documented here for the "polymorphic → discriminator" check the prompt calls for.

| Bridge | Discriminator column | Enum | Untyped id column |
|---|---|---|---|
| `uncertainty_ref` | `consumer_type` | `uncertainty_consumer_type` | `consumer_id` |
| `adopted_value_link` | `adopted_entity_type` | `adopted_entity_type` | `adopted_entity_id` |
| `export_lossiness_report` | `discarded_entity_type` | `lossiness_entity_type` | `discarded_entity_id` |

All three appear as Mermaid comments explaining the pattern; no hard FK is declared because the target table varies.

## Entities changed vs pre-session baseline

| Table | Prompt introduced | Notes |
|---|---|---|
| `asset` | 3 | Stable identity registry |
| `asset_revision` | 3 | Per-branch revision snapshot |
| `inventory_branch` | 3 | Named branches |
| `revision_log` | 3 | Editor + reason + diff |
| `prov_agent` | 1 | Provenance actor |
| `prov_activity` | 1 | Derivation activity |
| `source_observation` | 1 | Atomic observation; extended in P6 (FK + `is_redacted`) |
| `adopted_value_link` | 1 | Polymorphic bridge |
| `uncertainty_spec` | 2 | Superclass |
| `uncertainty_spec_continuous` | 2 | Subclass |
| `uncertainty_spec_categorical` | 2 | Subclass |
| `uncertainty_spec_deterministic` | 2 | Subclass |
| `uncertainty_ref` | 2 | Polymorphic bridge |
| `uncertainty_correlation` | 2 | Pairwise dependence |
| `distribution_registry` | 2A | Family contract + compute backend |
| `ddf_uncertainty` | 2B (reshape) | Removed inline family/params; added `spread_spec_id`; retained `damage_mean`; percentile columns became rendered-cache |
| `geometries` | 4 | Geometry as its own class |
| `export_contract` | 5 | Versioned contract |
| `export_product` | 5 | Emitted artifact + manifest |
| `export_lossiness_report` | 5 | Machine-readable lossiness |
| `source_registry` | 6 | Classification + steward |
| `component_connection` | 8 | Typed edges between components |
| `generic_building_component` | 8A | First `generics` subclass |
| `generics` (extended) | 8A | Added `kind`, `description`, `version_id`, `created_at` |
| `buildings` (reduced) | 4 | Removed `geom` (moved to `geometries`) |

## Remaining mismatches / open items

**None blocking.** Everything the accumulated ADRs asked for is either:
- (a) enforced statically by `preview_dict.py` (schema-level rules), or
- (b) documented as a runtime rule in the section above (data-content rules that need populated rows).

Follow-up work for Prompt 12 and beyond:
- Wire the runtime rules above into a data-content validator (Postgres constraint / trigger set + CI check). Out of scope for Prompt 11.
- Run the ADR-performance benchmark (Prompt 7 scaffold) and populate `docs/review/perf-benchmark.md`.
- Prompt 12: assemble `docs/review/issue-1-responses.md` and the traceability matrix; draft the GitHub-issue-1 comment.

## Mermaid render sanity

The ERD file `diagrams/rqm-erd.mmd`:

- 35 entity blocks (matches YAML).
- Balanced `{` / `}` for every entity block.
- Every FK column carries the `FK` marker.
- Every relationship uses valid Mermaid ER cardinality (`||--o{`, `||--||`, `|o--o{`, `}o--||`).
- Polymorphic bridges annotated with `%%` comments describing the discriminator-on-relationship pattern (no hard FK arrow, as intended).

Numerical claim not made: this report does not attempt to render the diagram; it verifies structural sanity by string checks. GitHub's Mermaid renderer is the authoritative renderer.
