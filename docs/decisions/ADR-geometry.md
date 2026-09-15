# ADR — Geometry as Its Own Class (with a Component-Level Hook)

**ID:** ADR-geometry
**Status:** Proposed
**Date:** 2026-09-08
**Extends:** ADR-versioning (pre-committed geometry-attaches-to-`asset_id`)
**Resolves:** ISSUE-Q04, ISSUE-R06
**Serves user stories:** RQM-01, RQM-05, RQM-13, RQM-18
**Locks:** the semantic object and its geometric representation are linked but separable; a single asset may carry multiple representations *and* multiple alternates at the same representation level; geometry revises independently of non-geometric attributes.

## Context

Today geometry is a column on `buildings` (`geom geometry(Point,5070)`, `NOT NULL`). This has three problems that show up as soon as the model has to describe anything realistic:

1. **Only one representation per building.** A building has one point. There is nowhere to put a footprint (needed for hazard sampling / QC), let alone alternates (two competing footprints from different sources at the same LOD).
2. **No component-level attach point.** Prompt 8 (CityGML profile) and the reviewer comment on ERD L14 both anticipate future component geometry — HVAC, foundation, roof envelope. A column on `buildings` cannot host that.
3. **Revision is coupled to the building row.** Any geometry change forces a `buildings` revision, even when nothing non-geometric changed. ADR-versioning already anticipated this: "the `geometries` table introduced in Prompt 4 will attach to `asset_id`, not `building_id`, so a building's geometry can revise independently of its non-geometric attributes" (ADR-versioning §Impact).

The reviewer comment (ERD L14) frames the ask as CityGML's *coherent semantical-geometrical modeling* principle — the semantic hierarchy (asset → component) and the geometric hierarchy should track each other, but the geometry is a separate object.

## Decision

Promote geometry to a first-class entity `geometries`. Remove `buildings.geom`. Attach every geometry to an `asset_id`, with an optional `component_id` when the geometry represents a specific sub-assembly.

### Schema

`geometries`

- `geometry_id` — bigint PK.
- `asset_id` — bigint FK → `asset.asset_id`, **required**. Every geometry belongs to a top-level asset (the pre-commitment from ADR-versioning). Geometry survives revisions of the building's non-geometric attributes and vice versa.
- `component_id` — bigint FK → `building_components.component_id`, **nullable**. When set, this geometry represents the component (foundation, HVAC, roof, …). When null, it represents the asset itself. The component's `building_id.asset_id` must equal this row's `asset_id` (Prompt 11 rule; enforces the CityGML coherence principle at the FK layer instead of via polymorphism).
- `role` — varchar(24), **required**. Enum `geometry_role`: `point | footprint | envelope_3d | detailed_3d`. RQM's compact LOD-like tag. We do not adopt the CityGML LOD0/1/2/3/4 taxonomy in v0.1 — `role` covers today's needs and can be extended.
- `crs` — varchar(24), **required**. EPSG code (e.g., `EPSG:5070`). Stored per-row so alternates from different sources can preserve their native CRS.
- `geom` — PostGIS `geometry`, **required**. The actual geometry payload; the shape is dictated by `role` (point ↔ Point; footprint ↔ Polygon/MultiPolygon; 3D roles ↔ PolyhedralSurface / TIN when populated).
- `purpose` — varchar(24), **required**. Enum `geometry_purpose`: `computation | qc | hazard_sampling | display | future_export`. Encodes *why* this representation exists.
- `is_selected_for_export` — boolean, **required** (default false). Exactly one row per `(asset_id, purpose='computation')` may be `true` for a given branch/valid-time (Prompt 11 rule 2). This is the "selection rule" from the prompt: the flat export always resolves the same one representation regardless of how many alternates are stored.
- `alt_group_key` — varchar(64), nullable. Groups alternate representations at the *same* `(asset_id, role)`. Two competing footprints from different sources share one `alt_group_key`; a footprint and a point do not.
- `source_ref` — varchar(128), nullable. Source dataset or observation reference (e.g., `NSI-2024.1`, `Allegheny-FS-2023`). Full provenance flows through `adopted_value_link` with `adopted_entity_type='geometries'`.
- `validity_version_id` — bigint FK → `versioning.version_id`, **required**. Bitemporal registry entry for this geometry revision (ADR-versioning).
- `created_at` — timestamptz, **required**.

### Enum additions

- `geometry_role`: `point | footprint | envelope_3d | detailed_3d`. `envelope_3d` and `detailed_3d` are declared but not populated in v0.1 — they exist so upstream tooling knows the model reserves the vocabulary.
- `geometry_purpose`: `computation | qc | hazard_sampling | display | future_export`.

### Enum edits

- `asset_type`: **remove** the `geometry` placeholder. Geometries do not need their own identity registry — they have their own `geometry_id` and their own `validity_version_id`. Assets are the *things* the world cares about; geometries are how the model *represents* one.
- `entity_type`: add `geometries` so a geometry revision can be tracked through `versioning`.
- `adopted_entity_type`: replace the `buildings` placeholder (marked "until Prompt 4 introduces `geometries`") with `geometries`. Provenance now attaches to the geometry record, not to the parent building — geometry is where a coordinate change lives.

### Coherent semantical-geometrical modeling — how RQM keeps it, lightly

CityGML's principle is that the semantic hierarchy (building → building-part → wall → …) and the geometric hierarchy (solid → surfaces → curves) should track each other. RQM adopts the principle at a lightweight level:

- The semantic hierarchy today is **asset → component** (two levels — enough for RQM's coverage story). `geometries.asset_id` + optional `geometries.component_id` mirrors that hierarchy exactly.
- The coherence rule ("a component's geometry must roll up to the same asset as its parent component") is a real FK-computable check (Prompt 11 rule 3), not a comment.
- No full CityGML topology (adjacency, boundaries, part-of graphs). Those are deferred to Prompt 8 (component-to-component connections) and remain optional.

### What `buildings.geom` becomes

Removed. Every building must have at least one `geometries` row with `role='point'` and `purpose='computation'` and `is_selected_for_export=true` — that row *is* what `buildings.geom` used to be, but resolvable to a specific representation instead of a floating column. Prompt 11 will surface any building without such a row.

Safe as a v0.1 change: nothing has been ingested against the schema; the seed workflow (NSI point ingest) becomes "insert one `buildings` row + one `geometries` row" instead of "insert one `buildings` row with `geom`."

## Recommended starting scope

- **Populate now:** `point` and `footprint` (where a source dataset provides one — NSI structure points always, HAZUS occtype footprints when available).
- **Reserve, do not populate:** `envelope_3d`, `detailed_3d`, and component-level geometry. `component_id` on `geometries` is the attach point when a demonstrated use case (BIM ingest / façade-level flood forensics / CityJSON export via RQM-18) justifies it.

Per-use-case need statement:

| Candidate use case | Point | Footprint | 3D (envelope/detailed) | Component-level |
|---|---|---|---|---|
| Loss computation (current flat engine) | required | not needed | no demonstrated need — deferred | no demonstrated need — deferred |
| Inventory QC (visual review, dedup) | required | useful | no demonstrated need — deferred | no demonstrated need — deferred |
| Hazard sampling (depth-in-structure) | required | useful (footprint-aware sampling) | no demonstrated need — deferred | no demonstrated need — deferred |
| Future CityJSON / 3DCityDB export (RQM-18) | required | required | *would be* required | *would be* useful |
| Component-fragility loss (future) | — | — | no demonstrated need — deferred | *would be* required |

Nothing outside the first row is required for the current flat workflow. The other rows describe optional modules unlocked by populating additional geometry rows — no schema change.

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

1. Every `geometries` row has a non-null `role`, `crs`, `geom`, and `validity_version_id`.
2. Exactly one `geometries` row per `(asset_id, purpose='computation')` has `is_selected_for_export=true`, resolved at the branch/valid-time of the export contract.
3. When `geometries.component_id` is non-null: `building_components.building_id` for that component resolves (via `buildings.asset_id`) to the same `asset_id` on this geometry row (coherent semantical-geometrical rule).
4. When `geometries.alt_group_key` is non-null: all rows sharing that key must share the same `(asset_id, role)`. Alternates are alternates *at the same representation level*, not across levels.
5. Every `buildings` row has at least one `geometries` row with `role='point'` and `is_selected_for_export=true` on the current branch. This is the drop-in replacement for the old `buildings.geom NOT NULL`.
6. `geometries.geom` payload kind is consistent with `role`: `point → ST_GeometryType='ST_Point'`; `footprint → ST_Polygon | ST_MultiPolygon`; 3D roles → 3D types when populated.
7. Any adopted-value link into a geometry uses `adopted_entity_type='geometries'`; existing `adopted_entity_type='buildings'` rows (placeholder) are migrated to `geometries` at v0.1 cutover.

## Worked examples

### Example 1 — Standard NSI ingest (point only)

```
asset       {asset_id=42, asset_type='building', canonical_external_ref='NSI:fd_id:1234567'}
buildings   {building_id=100, asset_id=42, occupancy_type='RES1', ...}    -- no geom column
geometries  {geometry_id=500, asset_id=42, component_id=null,
             role='point', crs='EPSG:5070',
             geom=ST_Point(-80.11, 40.44),
             purpose='computation', is_selected_for_export=true,
             alt_group_key=null,
             source_ref='NSI-2024.1', validity_version_id=...}
```

Flat export resolves `geometry_id=500` for asset 42. Same shape as before, one indirection deeper — but the row can carry its own version + provenance.

### Example 2 — Two competing footprints at the same LOD

```
geometries  {geometry_id=501, asset_id=42, role='footprint', crs='EPSG:5070',
             geom=<polygon A>, purpose='qc', is_selected_for_export=false,
             alt_group_key='asset:42:footprint', source_ref='MicrosoftBuildings-2022'}

geometries  {geometry_id=502, asset_id=42, role='footprint', crs='EPSG:5070',
             geom=<polygon B>, purpose='qc', is_selected_for_export=false,
             alt_group_key='asset:42:footprint', source_ref='Allegheny-cadastre-2023'}
```

Both footprints coexist. Alt-group key makes them queryable as alternates without collapsing to one. Selection for downstream export is a policy decision, not a data-loss event.

### Example 3 — Foundation as component-level geometry (future / deferred population)

```
building_components  {component_id=700, building_id=100, component_type='foundation', ...}

geometries  {geometry_id=503, asset_id=42, component_id=700,
             role='envelope_3d', crs='EPSG:5070',
             geom=<polyhedral surface>,
             purpose='future_export', is_selected_for_export=false,
             alt_group_key=null, source_ref='pilot-BIM-2027-Q1',
             validity_version_id=...}
```

Component-level 3D lands in the same table via `component_id`. Coherence rule holds: `building_components[700].building_id=100` → `buildings[100].asset_id=42` matches `geometries[503].asset_id=42`.

### Example 4 — Rejected: component geometry pointing at the wrong asset

```
geometries  {geometry_id=504, asset_id=99, component_id=700, ...}
             -- building_components[700].building_id → buildings.asset_id = 42 ≠ 99
```

Prompt 11 rule 3 rejects — the semantic hierarchy and the geometric hierarchy would be inconsistent.

### Example 5 — Rejected: two selected computation-points for one asset

```
geometries  {geometry_id=505, asset_id=42, role='point', purpose='computation', is_selected_for_export=true}
geometries  {geometry_id=506, asset_id=42, role='point', purpose='computation', is_selected_for_export=true}
```

Prompt 11 rule 2 rejects — the export contract must resolve to exactly one representation.

## Claim discipline

- The class-not-column pattern is *research-recommends* (CityGML coherent semantical-geometrical modeling is the design reference) + *project-requires* (RQM-01 and RQM-18 both need more than one geometry per asset).
- Two-real-FK attach (`asset_id` required + `component_id` optional) instead of a polymorphic `(owner_type, owner_id)` bridge is *project-requires* — RQM's semantic hierarchy is only two levels, so a hierarchical FK is both simpler and DB-enforceable. Polymorphic patterns stay reserved for genuinely polymorphic bridges (`uncertainty_ref`, `adopted_value_link`).
- Removal of `buildings.geom` is a v0.1 breaking change; documented here and paid back by CityGML coherence + independent geometry versioning.
- `envelope_3d` / `detailed_3d` in the enum are *specification-allows*, not *implementation-demonstrates*. Populated when a named use case arrives.

## Alternatives considered

- **Keep `buildings.geom` as a "current-head projection" of the selected computation-point geometry.** Rejected — duplicates the same fact in two places; Prompt 11 would have to police the tie forever. The whole point of the ADR is that geometry is a separate object, not a column.
- **Polymorphic parent via `(owner_type, owner_id)`.** Rejected — RQM's semantic hierarchy has only two levels (asset → component); a real FK per level is enforceable at the database and cheaper to reason about. Polymorphism buys nothing here.
- **Adopt full CityGML LOD taxonomy (LOD0–LOD4).** Rejected — heavy for v0.1; the compact `role` enum covers today's use and can be extended without a schema break. RQM-18 escape hatch (export to CityJSON / 3DCityDB) remains feasible.
- **Split the geometry payload into shape-typed columns** (`geom_point`, `geom_polygon`, `geom_solid`). Rejected — one `geometry` column + role discriminator matches PostGIS conventions and keeps queries polymorphic.
- **Make geometry its own asset kind (`asset_type='geometry'`).** Rejected (and the existing placeholder removed) — geometries are *representations of* assets, not assets themselves. Registering them as separate assets would fragment identity for no gain.
- **Attach geometry to `building_id` instead of `asset_id`.** Rejected by ADR-versioning's pre-commitment: geometry must survive `buildings` revisions, and the `asset` identity is the thing that survives.

## Impact on other ADRs

- **ADR-versioning:** the pre-committed hook is now real. `geometries.validity_version_id` carries the bitemporal quadruple; a "geometry_move" `change_kind` produces a `revision_log` row against that version. `buildings.geom` removal deletes the last non-attribute concern from `buildings`.
- **ADR-provenance:** `adopted_entity_type` gains `geometries` (replaces the `buildings` placeholder). A relocated NSI point records the source observation, the derivation activity ("Reviewer 1 relocation pass"), and the adopted `geometry_id` — provenance now attaches at the grain the change actually occurred at.
- **ADR-uncertainty:** unaffected in v0.1 — positional uncertainty on geometry is deferred. When needed, it will bind via `uncertainty_ref` with a new `consumer_type='geometries'` (small enum extension, no structural change).
- **ADR-distribution-registry:** unaffected.
- **ADR-ddf-uncertainty:** unaffected.
- **ADR-citygml-profile (Prompt 8):** the coherence principle is realized here at a lightweight level; component-to-component *connections* (ERD-5) are the next Prompt-8 concern and layer cleanly on top of `building_components` and `geometries`.
- **ADR-exports (Prompt 5):** the export compiler resolves geometry via `(asset_id, purpose='computation', is_selected_for_export=true)` — an explicit selection rule, not an implicit column read. Lossiness report enumerates unselected alternates.
- **ADR-generics (Prompt 8A):** when `asset_type` extends beyond `building`, `geometries.asset_id` continues to work — a bridge asset or a levee asset carries its own geometry rows the same way.
- **ADR-restricted-sources (Prompt 6):** a geometry adopted from a restricted source is classified via `source_observation` + `adopted_value_link`, not by hiding a column. The public export contract may null out the geometry payload with a redaction flag while retaining the record's existence.

## Deferrals

- **`envelope_3d` / `detailed_3d` population.** Declared in the enum; no ingest until a named use case (RQM-18 CityJSON export, façade-level flood forensics) commits.
- **Component-level geometry population.** `component_id` FK provided; no seed workflow.
- **Positional uncertainty on geometry.** Would attach via `uncertainty_ref` with `consumer_type='geometries'`; enum extension deferred.
- **CityGML LOD0/1/2/3/4 taxonomy.** RQM uses the compact `role` enum; migration path to LOD numerics remains open.
- **Component-to-component topology / connections** (roof → wall → foundation). Prompt 8 territory.
- **Geometric validity checks** (self-intersecting polygons, degenerate solids). Postgres/PostGIS can enforce; validation-CI cost deferred to Prompt 11.
- **`geom` type constraints per role at the SQL layer** (a CHECK constraint per row). Recommended shape captured in Prompt 11 rule 6; not encoded as a column-level constraint in v0.1.
