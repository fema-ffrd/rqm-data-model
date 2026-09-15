# ADR — CityGML Concept Profile (Adopt / Defer) & Component Connections

**ID:** ADR-citygml-profile
**Status:** Proposed
**Date:** 2026-09-08
**Extends:** ADR-geometry, ADR-versioning, ADR-uncertainty
**Resolves:** ISSUE-R05 (component connections, ERD L23), ISSUE-R08 (CityGML concepts useful vs overkill — Passarelli comment)
**Serves user stories:** RQM-05, RQM-10, RQM-13, RQM-18
**Locks:** RQM stays quasi-hybrid — adopt CityGML *principles* where they pay off, do not adopt the *encoding*; the RQM-18 export path to CityJSON / 3DCityDB stays a documented escape hatch, not a mandate.

## Context

The reviewer (Passarelli) asked: which CityGML concepts are useful for RQM, and which are overkill? And: what benefits do CityGML principles (validation, LOD, coherent semantics-geometry) offer as representation moves toward higher LOD? Reviewer ERD comment L23 pushed further: components need first-class *connections* — a roof sits on a wall which sits on a foundation — modeled the way CityGML handles relationships between objects.

Left unresolved, two failure modes:

1. **Over-adoption.** Someone reads "quasi-hybrid" as "adopt everything CityGML." The model bloats with textures, appearances, interior furniture, complete topology, and full GML — none of which serves an RQM user story today.
2. **Under-adoption.** Someone reads "no full CityGML" as "no CityGML principles either." Component relationships remain implicit, LOD promotion has no validation gates, and RQM-18 becomes structurally unreachable.

This ADR draws the line and adds the minimum structure (a `component_connection` table + validation rules) needed to keep future LOD growth honest.

## Decision

Adopt seven CityGML *principles*, defer five *encoding-level* concerns, add a `component_connection` table for typed component-to-component relationships, and lock a small set of validation rules that pay off progressively as LOD increases.

### ADOPT (7 principles) — with concrete RQM benefit

| # | CityGML principle | Where in RQM | User story | Benefit |
|---|---|---|---|---|
| A1 | **Stable identity across representations and revisions** | `asset` + `asset_revision` (ADR-versioning) | RQM-02, RQM-11 | An asset survives every geometry / attribute / location change without breaking the FK graph. |
| A2 | **Multiple representations / LOD per object** | `geometries.role` + `alt_group_key` (ADR-geometry) | RQM-01 | Point + footprint + reserved 3D roles + alternates at the same role — one asset, many representations. |
| A3 | **Building hierarchy / components** | `building_components` + `generics` typing | RQM-05, RQM-13 | A structure decomposes into modular sub-assemblies; loss can be evaluated at component grain. |
| A4 | **Coherent semantical-geometrical modeling** | `geometries.component_id` FK — the semantic hierarchy (asset → component) is mirrored by the geometric attach point | RQM-05, RQM-13 | The rule "a component's geometry must roll up to the same asset as its parent" is DB-enforceable (Prompt 11), not a comment. |
| A5 | **Defined relationships between objects** | New `component_connection` table (this ADR) | RQM-05, RQM-13 | Typed edges between components (`supports`, `contains`, `adjacent_to`, `attached_to`) let QC ask "does this roof have a wall to sit on?" without inventing a graph out of nowhere. |
| A6 | **Temporal / version concepts** | `versioning` (bitemporal) + `revision_log` + `inventory_branch` (ADR-versioning) | RQM-03, RQM-16 | Assets and their representations evolve with valid-time + record-time interval semantics that mirror the CityGML temporal profile. |
| A7 | **Extensibility (Application Domain Extensions analogue)** | `generics` + Prompt 8A generalization | RQM-10, RQM-18 | New asset kinds (bridges, levees, utilities) attach without a schema break — parallels CityGML's ADE mechanism at the schema layer. |

### DEFER / EXCLUDE (5 encoding concerns) — why overkill now

| # | CityGML concern | Why overkill for RQM v0.1 |
|---|---|---|
| D1 | **Full GML encoding** (XML schemas, `gml:*` element structure) | RQM's canonical form is Postgres + Iceberg + Icechunk. GML encoding is a target for CityJSON / 3DCityDB *export* (RQM-18), not the internal representation. Adopting GML natively would double the schema authoring surface for no consequence-modeling benefit. |
| D2 | **Textures and appearance** | Loss estimation does not consume material colors, façade textures, or reflectance. No user story lands here. |
| D3 | **Interior rooms / furniture (LoD4)** | No FFRD workflow reasons about interior spaces or furniture placement at that resolution. Content-loss uses inventory value at building or component grain, not room grain. |
| D4 | **Complete topology (LoD2/3 explicit boundary/adjacency graphs)** | The component-connection table (§A5) captures the relationships RQM actually asks — "roof sits on wall." Full topology (`gml:CompositeSurface` boundary graphs, exterior/interior shell walking) is overkill until 3D geometry is populated *and* a use case demands topological queries. |
| D5 | **Mandatory detailed 3D at any LoD tier** | ADR-geometry already reserves `envelope_3d` / `detailed_3d` in the enum but does not require them. No RQM user story requires detailed 3D; adopting it as mandatory would push cost onto every ingest for no computed benefit. |

The two-column framing is deliberately blunt: "adopt seven, defer five." Anything not listed here is *not silently adopted* — a future ADR that argues for adoption is welcome, but the default is "outside the profile."

### Component connections — schema

`component_connection`

- `connection_id` — bigint PK.
- `from_component_id` — bigint FK → `building_components.component_id`, required.
- `to_component_id` — bigint FK → `building_components.component_id`, required.
- `connection_kind` — varchar(24), required. Enum `connection_kind`: `supports | contains | adjacent_to | attached_to`.
- `description` — text, nullable. Free-text elaboration ("wood-frame roof sits on load-bearing wall, nailed and hurricane-strapped").
- `version_id` — bigint FK → `versioning.version_id`, required. Connections are versioned so a component swap doesn't rewrite history.
- `created_at` — timestamptz, required.

**Semantics per kind:**

- `supports` — asymmetric. `from` is load-bearing; `to` rests on `from`. Foundation `supports` structure; wall `supports` roof.
- `contains` — asymmetric. `from` encloses `to`. Structure `contains` finish; envelope `contains` HVAC.
- `adjacent_to` — symmetric. Two components share a boundary without one supporting the other.
- `attached_to` — asymmetric, non-load-bearing. HVAC `attached_to` structure; contents `attached_to` finish.

The enum is intentionally small (four kinds). CityGML supports richer relationship taxonomies; RQM v0.1 covers the four cases the reviewer actually asked for and defers the rest.

### Enum additions

- `connection_kind`: `supports | contains | adjacent_to | attached_to`.
- `entity_type`: add `component_connection` (versioned via `versioning`).

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

These pay off progressively as LOD increases. Rules 1–4 apply today; 5–7 activate as 3D roles get populated.

1. **Both endpoints exist.** `component_connection.from_component_id` and `to_component_id` both resolve to `building_components.component_id`.
2. **Same building.** `from_component_id.building_id` equals `to_component_id.building_id`. Cross-building topology is out of scope for v0.1 (see §Deferrals).
3. **No self-connection.** `from_component_id != to_component_id`.
4. **Symmetric-kind pairs are recorded once.** For `connection_kind='adjacent_to'`, at most one row per unordered `{from, to}` pair. Two symmetric rows would double-count in queries.
5. **A component's geometry must have a valid component.** `geometries.component_id`, when non-null, resolves — orphan component geometry is rejected. (Restates ADR-geometry rule 3 in Prompt-8 terms; kept explicit so LOD-promotion checks find it.)
6. **LOD promotion does not orphan components.** When a building acquires an `envelope_3d` or `detailed_3d` geometry, every component that already has a lower-role geometry must still resolve to an existing component_id — a 3D promotion cannot silently drop components that populated 2D representations.
7. **Footprint consistent with building.** When a `role='footprint'` geometry exists for an asset, the asset's selected `role='point'` geometry (Prompt 11 rule for ADR-geometry) must fall within or on the footprint (`ST_Covers(footprint.geom, point.geom)`), within a small tolerance. Diagnostic in v0.1 (log-and-report), promotable to error later.

Rules 6 and 7 exist so that the ADR-geometry reserved 3D roles are usable safely when they finally get populated — the validator catches misalignment at that moment, not months later.

## RQM-18 escape hatch — CityJSON / 3DCityDB export

The seven ADOPT items were chosen so that a downstream export to CityJSON / 3DCityDB remains feasible:

- **Identity** (A1) maps to CityJSON's persistent object ids.
- **Multiple representations** (A2) maps to per-LoD geometry members.
- **Building hierarchy** (A3) maps to `Building` / `BuildingPart` (or `BuildingInstallation` for HVAC-style components).
- **Coherent semantics-geometry** (A4) is the CityJSON invariant — same principle, cheaper implementation.
- **Component connections** (A5) map to CityJSON's `parent` / `children` and `parts` relationships.
- **Temporal concepts** (A6) map to CityJSON's `+versioning` extension.
- **Extensibility** (A7) maps to CityJSON's `+extensions`.

No RQM structure needs to be redesigned to support the export path. The reverse — RQM ingesting a CityJSON dataset — is out of scope for v0.1 but structurally reachable through the same mapping.

Concretely: an RQM-18 exporter is a *contract* in the ADR-exports sense — one `export_contract` with `target_engine='cityjson'`, `product_kind='deterministic'`, `required_fields` naming the mapping above. No schema changes required to build it.

## Worked examples

### Example 1 — Roof sits on wall sits on foundation

```
building_components  {component_id=701, building_id=100, component_type='foundation', generic_id=...}
                     {component_id=702, building_id=100, component_type='structure', generic_id=...}  -- load-bearing wall
                     {component_id=703, building_id=100, component_type='structure', generic_id=...}  -- roof

component_connection {connection_id=901, from=701, to=702, connection_kind='supports',
                      description='concrete slab supports wood-frame wall'}
component_connection {connection_id=902, from=702, to=703, connection_kind='supports',
                      description='load-bearing wall supports gable roof'}
```

QC query: "list every roof that has no `supports` link inbound." Now expressible.

### Example 2 — HVAC attached to structure, then contained in envelope

```
building_components  {component_id=704, building_id=100, component_type='structure', generic_id='hvac'}
                     {component_id=705, building_id=100, component_type='structure', generic_id='envelope'}

component_connection {connection_id=903, from=704, to=702, connection_kind='attached_to',
                      description='HVAC unit fastened to interior wall'}
component_connection {connection_id=904, from=705, to=704, connection_kind='contains',
                      description='envelope shell encloses HVAC'}
```

Later, when a component-level 3D geometry lands on the HVAC unit (`geometries.component_id=704`, `role='envelope_3d'`), rule 5 accepts (component exists) and rule 6's LOD-promotion check compares the 3D role against the 2D representations that were previously present.

### Example 3 — Rejected: cross-building connection

```
component_connection {from=701, to=812, ...}
                     -- building_components[701].building_id = 100
                     -- building_components[812].building_id = 205
```

Prompt 11 rule 2 rejects — component connections are intra-structure only in v0.1. Cross-building topology (e.g., row-house shared walls, breezeway connections) is deferred.

### Example 4 — Rejected: self-connection

```
component_connection {from=703, to=703, ...}
```

Prompt 11 rule 3 rejects.

### Example 5 — CityJSON export (RQM-18) is *just a contract*

```
export_contract {export_contract_id=42, name='rqm_to_cityjson_v1',
                 target_engine='cityjson', product_kind='deterministic',
                 required_fields=[
                   {name:'@id', source_selector:'asset.asset_id'},
                   {name:'type', source_selector:"'Building'"},
                   {name:'geometry[]',
                    source_selector:"geometries[asset_id=@id]"},
                   {name:'children[]',
                    source_selector:"building_components[building_id in (SELECT building_id FROM buildings WHERE asset_id=@id)]"},
                   {name:'parts_topology[]',
                    source_selector:"component_connection[from.building_id=@building_id OR to.building_id=@building_id]"},
                   ...
                 ], access_level='public', ...}
```

Zero schema changes. The exporter is a compiler task under the ADR-exports contract, not a schema redesign.

## Claim discipline

- The ADOPT/DEFER framing is *research-recommends* (CityGML documentation names these concepts by architecture) + *project-requires* (ISSUE-R08 asked for the split).
- The seven adopted principles are what RQM has *already* built by v0.1 (six) plus the one added here (component connections). Nothing on the list is aspirational.
- The four `connection_kind` values are *project-requires* — the four the reviewer cited. Richer taxonomies (`bolted_to`, `welded_to`, load-fraction weights) are *specification-allows* and deferred.
- The LOD-promotion rules (6–7) are *specification-allows*: they encode a checker that only bites when 3D data is present. Today's data doesn't trip them; that is by design.
- The RQM-18 mapping table is *specification-allows*, not *implementation-demonstrates* — an actual exporter is a follow-up task, not part of this ADR.

## Alternatives considered

- **Adopt full CityGML (encoding + all concepts).** Rejected — quasi-hybrid direction from Prompt 0. Encoding cost is enormous; no RQM story needs it internally.
- **No connection table; encode adjacency in `building_components.notes`.** Rejected — free text is not queryable and drifts.
- **Bidirectional connection rows for symmetric kinds.** Rejected — doubles storage and breaks unique-pair reasoning. Rule 4 keeps `adjacent_to` single-row per unordered pair.
- **Polymorphic connections (component ↔ building, component ↔ geometry).** Rejected — the reviewer asked about component-to-component connections. Broader polymorphism is deferred; adding it now would blur the ADR's scope.
- **Load-fraction / support-fraction weights on `supports` connections.** Rejected for v0.1 — fragility-weighted damage propagation is future work; today's `supports` is a boolean edge.
- **Enforce `supports` chain consistency in the schema (foundation < structure < roof).** Rejected — the vocabulary (`component_type`) has legitimate overlap ("structure" can be wall or roof). Rule 4 covers the double-count case; ordering rules are v0.2 territory.
- **Fold `component_connection` into `building_components.parent_component_id`.** Rejected — a component can have multiple simultaneous relationships (a wall supports the roof AND is adjacent to the wall next door). A single parent pointer collapses the graph.

## Impact on other ADRs

- **ADR-geometry:** rule 5 restates the component-geometry integrity check in Prompt-8 terms; rule 6 activates when reserved 3D roles get populated. `geometries.component_id` FK stays the coherence anchor.
- **ADR-versioning:** `component_connection.version_id` versions edges the same way node revisions are versioned. A component swap that leaves connections orphaned is caught by rule 1.
- **ADR-uncertainty:** unchanged — component connections carry no uncertainty in v0.1 (a fragility edge might in the future via `uncertainty_ref` with a new `consumer_type='component_connection'`; not added).
- **ADR-exports:** the RQM-18 CityJSON exporter is a contract, not a schema change. `target_engine='cityjson'` and `product_kind='deterministic'` compose from existing pieces.
- **ADR-generics (Prompt 8A):** the ADOPT-A7 pattern (extensibility) is the design principle Prompt 8A implements. When `asset_type` extends beyond `building`, connections remain intra-asset (rule 2) or evolve their scope in a follow-up ADR.
- **ADR-provenance:** connections can be adopted from source observations the same way any other structural claim is (`adopted_entity_type` may grow `component_connection` if audit demand justifies it; not added in v0.1).
- **ADR-restricted-sources:** unchanged — connections are structural, not source-classified.
- **ADR-performance:** ADR-performance's workload catalog does not currently benchmark connection queries. If component-fragility work lands, add "Q9 — connection walk for a building's load-path" to the harness.

## Deferrals

- **Cross-building topology** (shared walls, breezeways, row-house adjacency). Rule 2 blocks it today; a v0.2 ADR can lift the constraint when a use case demands.
- **Richer connection vocabulary** (`bolted_to`, `welded_to`, `hurricane_strapped_to`, structural steel joint types). Free-text `description` covers today's storytelling.
- **Load-fraction / support-fraction weights.** Deferred with component-fragility loss.
- **Full topological graph queries** (boundary walks, shell traversal). Deferred with 3D role population.
- **Ordering-consistency rules** (foundation < structure < roof). Deferred; overlaps in `component_type` make v0.1 rules brittle.
- **Bidirectional connection semantics** at the enum layer (a `contains` implies a reciprocal `contained_by`). Deferred; single-row asymmetric records + query-time inversion is cheaper.
- **Actual CityJSON exporter.** Follow-up task; the mapping table in this ADR is the contract spec, not the implementation.
- **CityJSON ingestor** (reverse direction). Out of scope for v0.1.
