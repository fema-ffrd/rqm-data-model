# ADR — Generics as a Generalized Base Class

**ID:** ADR-generics
**Status:** Proposed
**Date:** 2026-09-08
**Extends:** ADR-uncertainty (SQL supertype/subtable pattern), ADR-versioning, ADR-citygml-profile (extensibility principle A7)
**Resolves:** ISSUE-R07 (ERD L30 — generics generalization)
**Serves user stories:** RQM-05, RQM-10, RQM-13
**Locks:** one coherent generalization pattern across the model (uncertainty_spec / generics / hierarchical-FK geometry); do not add speculative subclasses — generalize where a demonstrated or clearly anticipated cross-domain use exists.

## Context

Reviewer comment (ERD L30): `generics` today is building-focused — its rows exist to type `building_components`. But the same "extensible controlled vocabulary" pattern should also support future infrastructure asset types (bridges, levees, utilities) and cross-domain uncertainty *class* registrations (e.g., a shared "FFH_RES1_class_default" descriptor). The current shape (`generic_id`, `category`, `subtype`, `schema_ref`) is close, but it has no discriminator, no version, and no subclass hook — three additions any real generalization needs.

The other risk: the model already uses two generalization patterns (SQL supertype/subtable on `uncertainty_spec`; hierarchical FKs on `geometries`). Adding a third ad-hoc pattern for generics would fragment the design just when Prompt 8's CityGML profile lists "one coherent generalization/polymorphism pattern" as an ADOPT principle. Generics should reuse the `uncertainty_spec` shape.

## Decision

Refactor `generics` into a generalized superclass with a discriminator and a versioning hook. Add one concrete subclass now (`generic_building_component`) with the domain-specific columns building components actually use. Reserve two additional `generic_kind` values (`infrastructure_component`, `uncertainty_class`) as declared-but-unpopulated extension hooks; a fourth (`future_asset_kind`) is the escape hatch. Every concrete subclass follows the same SQL supertype/subtable pattern used by `uncertainty_spec`.

### Superclass — updated `generics`

Existing columns retained; four columns added.

- `generic_id` — bigint PK. (unchanged)
- `kind` — varchar(32), **required (new)**. Enum `generic_kind`: `building_component | infrastructure_component | uncertainty_class | future_asset_kind`. Discriminator for the subclass row (identical pattern to `uncertainty_spec.kind`).
- `category` — varchar(32), required. (unchanged) Top-level class (Buildings, Transportation, Utilities, …). Prompt 11 rule 3 requires consistency with `kind`.
- `subtype` — varchar(64), required. (unchanged) Specific type within the category.
- `schema_ref` — varchar(128), nullable. (unchanged) Pointer to the type-specific attribute schema.
- `description` — text, **nullable (new)**. Free-text description.
- `version_id` — bigint FK → `versioning.version_id`, **required (new)**. Generics are reference data — they change with a version bump rather than in place.
- `created_at` — timestamptz, **required (new)**. Row insertion timestamp.

Every row must have exactly one subclass row that matches `kind` (SQL supertype/subtable pattern; Prompt 11 rule 1). Same enforcement approach as `uncertainty_spec`.

### Subclass — `generic_building_component` (populated in v0.1)

- `generic_id` — bigint PK/FK → `generics.generic_id`, required.
- `damage_relevance` — varchar(16), nullable. Enum `damage_relevance`: `primary | secondary | contents_only | inventory_only`. Names how this component participates in loss (a primary structural piece is treated differently from a contents-only element in a fragility calculation).
- `typical_replacement_cost_fraction` — real, nullable. Component's typical share of the building's total replacement cost (`[0,1]`). Used by the compiler as a fallback when a specific `building_components.replacement_cost` is missing.

Only `generic_id` is required; the other two columns are class defaults that a project can fill or leave null. The subclass exists to hold *building-component-relevant* extension attributes that don't belong on every generic row.

### Deferred subclasses (declared, not populated)

`generic_kind` values that exist in the enum but have no subclass table yet:

| Enum value | Meaning | Why declared now, not populated |
|---|---|---|
| `infrastructure_component` | Future non-building assets (bridge deck, levee toe, pump station bay) | RQM-10 asks for this extensibility; a subclass will be added when a real ingest lands. Declaring the discriminator now keeps the code path open. |
| `uncertainty_class` | A cross-domain uncertainty-class descriptor (e.g., "FFH_RES1_class_default"). Points at a shared class-level `uncertainty_spec.scope='class_level'` by reference. | Anticipated cross-domain use; no consumer yet. |
| `future_asset_kind` | Explicit escape hatch — a kind that has been reserved but not committed to a name. | Prevents ad-hoc enum growth; new kinds get proposed as a named value with an ADR bump. |

Rule (Prompt 11 rule 5): a `generics` row with a deferred kind may exist in the enum, but if `generic_id` is referenced by any consumer (today: `building_components.generic_id`), the row must have a matching subclass. Deferred kinds without a subclass table cannot be referenced by other tables until the subclass lands.

### Where the pattern already lives (and why we match it)

Three generalization loci already exist:

1. **`uncertainty_spec` + subclass tables** (ADR-uncertainty) — SQL supertype/subtable with a `kind` discriminator. Same pattern this ADR adopts.
2. **`geometries` with hierarchical FKs** (ADR-geometry) — real FK per level (`asset_id` required, `component_id` optional). Not polymorphic. Chosen because the semantic hierarchy is only two levels.
3. **Polymorphic bridges** — `uncertainty_ref`, `adopted_value_link`, `export_lossiness_report`. `(consumer_type, consumer_id)` pattern; no hard FK; discriminator-on-relationship.

Generics generalization sits in the same category as (1): identity + kind discriminator + subclass rows for kind-specific columns. Not polymorphic (there's no floating parent), not hierarchical (there's no chain of nested asset types).

Reviewer checklist: does the change use ONE coherent generalization pattern? Yes — supertype/subtable now, same as `uncertainty_spec`.

### What belongs at the base vs. the subclass

**At the base** (`generics`):

- Stable identity (`generic_id`).
- Discriminator (`kind`).
- Common vocabulary (`category`, `subtype`, `schema_ref`).
- Versioning + audit (`version_id`, `created_at`).
- Free-text description.

**At the subclass:**

- Domain-specific attributes that would be null for other kinds (e.g., `damage_relevance` makes sense for building components; it means nothing for a bridge deck).
- Domain-specific integrity constraints.

The base is deliberately thin. The temptation to load "commonly needed" attributes onto the superclass is the standard supertype anti-pattern — resist it.

### Enum additions

- `generic_kind`: `building_component | infrastructure_component | uncertainty_class | future_asset_kind`.
- `damage_relevance`: `primary | secondary | contents_only | inventory_only`.
- `entity_type`: add `generics` (versioned via `versioning`).

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

1. **Supertype/subtable integrity.** Every `generics` row with `kind='building_component'` has exactly one matching `generic_building_component` row (`generic_id` PK/FK). Rows with deferred kinds must not be referenced by any consumer until a subclass table exists (rule 5).
2. **Consumer FK kind check.** Every `building_components.generic_id` resolves to a `generics` row with `kind='building_component'`. Enforced in-app/CI; the FK itself does not carry the kind.
3. **Category-kind consistency.** `generic_kind='building_component'` implies `category='Buildings'`; `infrastructure_component` implies `category ∈ {Transportation, Utilities, Communications, Water}`. Prompt 11 rejects mismatches.
4. **No orphaned subclass rows.** A `generic_building_component` row must have a `generics` row (FK-enforced by definition, called out here for the LOD-consistency pattern from ADR-citygml-profile).
5. **Deferred kinds are non-referenceable.** A `generics` row with `kind ∈ {infrastructure_component, uncertainty_class, future_asset_kind}` may exist but must not be pointed at by any consumer's FK until the corresponding subclass table lands. Prevents phantom references.
6. **`typical_replacement_cost_fraction` in [0, 1] when non-null.**

## Worked examples

### Example 1 — Existing v0.1 building-component generic under the new shape

```
generics                    {generic_id=10, kind='building_component',
                             category='Buildings', subtype='LoadBearingWall',
                             schema_ref='rqm.building_components.load_bearing_wall',
                             description='Interior or exterior wall carrying vertical load.',
                             version_id=..., created_at=...}

generic_building_component  {generic_id=10, damage_relevance='primary',
                             typical_replacement_cost_fraction=0.12}
```

Consumer `building_components.generic_id=10` resolves. Rule 2 accepts (kind is `building_component`). Rule 3 accepts (category is `Buildings`).

### Example 2 — Reserved kind without a subclass (safe)

```
generics {generic_id=200, kind='infrastructure_component',
          category='Transportation', subtype='BridgeDeck',
          description='Placeholder; no ingest yet.',
          version_id=..., created_at=...}

-- no matching subclass row (generic_infrastructure_component does not exist yet)
-- no consumer FK references generic_id=200
```

Rule 1 accepts (deferred kind + no consumer FK). Rule 5 accepts (no reference). If a future `bridges.generic_id=200` FK lands *before* the subclass table exists, rule 5 rejects — the ingest is blocked until the subclass is created.

### Example 3 — Rejected: kind/category mismatch

```
generics {kind='building_component', category='Transportation', ...}
```

Rule 3 rejects — building_component must have `category='Buildings'`.

### Example 4 — Rejected: consumer points at a deferred kind

```
generics             {generic_id=300, kind='uncertainty_class', ...}
building_components  {component_id=..., generic_id=300, ...}
```

Rule 2 rejects — `building_components.generic_id` must resolve to `kind='building_component'`. Rule 5 additionally rejects — the deferred kind is being consumed without a subclass. Either the row's kind is wrong, or the consumer table needs a different reference.

### Example 5 — Future infrastructure ingest (structurally reachable)

When a bridge inventory lands, this ADR gets a follow-up (ADR-generics-v0.2) that adds:

```
generic_infrastructure_component  {generic_id (PK/FK -> generics),
                                   asset_domain, span_class, ...}
```

Consumers (a new `bridges` table) declare `generic_id` FKs pointing at `generic_kind='infrastructure_component'` rows. Rules 1/2/3 pick up the new kind automatically. **No change to `generics` itself is required** — the base is stable across subclass additions.

## Claim discipline

- The supertype/subtable pattern is *research-recommends* (standard relational modeling; matches the ADR-uncertainty precedent) + *project-requires* (Prompt 8A explicitly asks for one coherent generalization pattern).
- Four `generic_kind` values, three deferred: *project-requires*. Only `building_component` is populated in v0.1. Declaring the enum values now, populating them only when a use case commits, is the pattern.
- `damage_relevance` and `typical_replacement_cost_fraction` are *specification-allows* — building components benefit from having them; nothing requires them at v0.1 grain.
- `future_asset_kind` as an explicit escape hatch is deliberate — it forces new kinds through an ADR revision, not through silent enum growth.
- The category-kind consistency rule (3) encodes a soft-typing invariant. It is *project-requires* — the reviewer flagged that generics needs to work across domains, and cross-domain requires clean partitioning.

## Alternatives considered

- **Add subclass tables for `infrastructure_component` and `uncertainty_class` in v0.1.** Rejected — "generalize only where demonstrated." No ingest asks for them yet. Reserving the enum value + rule 5 keeps the extension path open without paying for speculative structure.
- **Fold `generics` into `distribution_registry`.** Rejected — different purposes: `distribution_registry` is a compute contract; `generics` is a controlled vocabulary. Both are natural-key or surrogate-key registries but they answer different questions.
- **Use a polymorphic bridge (`generic_ref`) instead of subclass tables.** Rejected — no floating consumer wants to reference "some generic of any kind." Consumers know exactly which kind they need (`building_components` wants building_component generics only). Polymorphic bridges pay off when the parent is genuinely polymorphic; here it isn't.
- **Merge `damage_relevance` into `building_components` directly.** Rejected — it's a class-level property (all walls tend to have `primary` relevance), not a per-component fact. Living on `generic_building_component` amortizes it correctly.
- **Drop `schema_ref` since it's not consulted at v0.1.** Rejected — it's the extensibility hook for RQM-10 (a subtype's attribute schema pointer). Removing it would refork the extensibility work.
- **Version `generic_building_component` separately from `generics`.** Rejected — the subclass row is subordinate to its superclass row; one version_id at the base is enough. Consistent with `uncertainty_spec`.

## Impact on other ADRs

- **ADR-uncertainty:** the pattern used here mirrors uncertainty_spec's SQL supertype/subtable; no changes to that ADR. The `uncertainty_class` `generic_kind` value is reserved for a future descriptor that would link back to shared class-level uncertainty specs — not populated today.
- **ADR-geometry:** `component_id` FK on `geometries` continues to resolve through `building_components` and thence to `generic_kind='building_component'`. LOD-promotion rule 6 (ADR-citygml-profile) picks up any new component kinds automatically.
- **ADR-citygml-profile:** the ADOPT-A7 principle (extensibility / Application Domain Extensions analogue) is what this ADR implements. `future_asset_kind` is the RQM analogue of a CityGML ADE — reserve the slot, populate on demand.
- **ADR-versioning:** `generics.version_id` extends bitemporal coverage to the vocabulary. Historical exports remain reconstructable against the vocabulary that was in effect at the time.
- **ADR-exports:** `export_contract.required_fields[*].source_selector` can point at `generic_building_component.typical_replacement_cost_fraction` as an imputation fallback; the mechanism (already in ADR-exports) needs no schema change to consume the new column.
- **ADR-distribution-registry:** unchanged. Distribution family registry stays a compute contract, separate from vocabulary.
- **ADR-restricted-sources:** unchanged. Vocabulary is public.
- **ADR-performance:** the row-count impact is negligible (generics is a small reference table). No workload catalog change.
- **ADR-provenance:** `adopted_entity_type` may grow `generics` if audit demand justifies it later (e.g., tracking who added a subtype). Not added in v0.1.

## Deferrals

- **`generic_infrastructure_component` subclass table.** Lands when a real infrastructure ingest commits. Enum slot reserved.
- **`generic_uncertainty_class` subclass table.** Lands when a cross-domain shared uncertainty descriptor becomes concrete. Enum slot reserved.
- **Domain-specific `schema_ref` validators.** Prompt 11 checks that `schema_ref` is a well-formed pointer string but does not resolve the target schema.
- **Cross-generic relationships** (e.g., "this generic component type interacts with that generic hazard type"). If needed, would follow the `component_connection` pattern (a typed edge table); not added.
- **Migration of existing generics rows.** In-place: the four new columns (`kind`, `description`, `version_id`, `created_at`) are backfilled to `kind='building_component'`, description = null, a fresh version_id, and the row's original insertion time. Because this schema is v0.1 pre-ingest, the migration is a re-seed rather than a data-preserving ALTER.
- **Ordering constraints across generic kinds** (e.g., an `infrastructure_component` must have a stricter subtype vocabulary than a `building_component`). Deferred; kind-specific rules go in the subclass ADRs.
- **`generic_kind` extension governance** (who approves a new kind, review workflow). Same governance question ADR-restricted-sources deferred; solvable at deployment time.
