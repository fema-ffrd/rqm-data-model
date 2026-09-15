# Risk Quantification Methodology (RQM) Data Model — Data Dictionary

**Version:** 0.1.0 &nbsp;|&nbsp; **Companion:** `fema-ffrd/inland-consequences (SPHERE core schemas)`

Deterministic building-loss inputs (flood depth, foundation type, first-floor height, depth-damage function) are re-architected as first-class, versioned **distribution** objects for probabilistic risk assessment. Storage tiers are inherited from the FFRD data model: **PostgreSQL** (relational integrity), **Iceberg** (ensemble-scale results), **Icechunk/Zarr** (gridded hazard fields).

## Contents

- **Domain A — Inventory (coverage of building inventory / components)**
  - [`buildings`](#buildings)
  - [`building_components`](#building_components)
  - [`component_connection`](#component_connection)
  - [`generics`](#generics)
  - [`generic_building_component`](#generic_building_component)
  - [`geometries`](#geometries)
- **Domain B — Attribute Uncertainty (uncertainty quantification)**
  - [`uncertainty_spec`](#uncertainty_spec)
  - [`uncertainty_spec_continuous`](#uncertainty_spec_continuous)
  - [`uncertainty_spec_categorical`](#uncertainty_spec_categorical)
  - [`uncertainty_spec_deterministic`](#uncertainty_spec_deterministic)
  - [`uncertainty_ref`](#uncertainty_ref)
  - [`uncertainty_correlation`](#uncertainty_correlation)
  - [`distribution_registry`](#distribution_registry)
- **Domain C — Hazard Linkage (probabilistic flood depth)**
  - [`events`](#events)
  - [`hazard_links`](#hazard_links)
- **Domain D — Depth-Damage Functions (probabilistic DDFs)**
  - [`ddf_library`](#ddf_library)
  - [`ddf_uncertainty`](#ddf_uncertainty)
- **Domain E — Realization & Loss Results (ensemble scale)**
  - [`loss_realizations`](#loss_realizations)
  - [`mv_loss_summary`](#mv_loss_summary)
- **Domain F — Provenance & Versioning**
  - [`run_catalog`](#run_catalog)
  - [`manifests`](#manifests)
  - [`run_logs`](#run_logs)
  - [`versioning`](#versioning)
  - [`asset`](#asset)
  - [`inventory_branch`](#inventory_branch)
  - [`asset_revision`](#asset_revision)
  - [`revision_log`](#revision_log)
  - [`prov_agent`](#prov_agent)
  - [`prov_activity`](#prov_activity)
  - [`source_registry`](#source_registry)
  - [`source_observation`](#source_observation)
  - [`adopted_value_link`](#adopted_value_link)
- **Domain G — Exports & Contracts**
  - [`export_contract`](#export_contract)
  - [`export_product`](#export_product)
  - [`export_lossiness_report`](#export_lossiness_report)

## Enumerations

- **`dist_family`**: `categorical`, `bernoulli`, `empirical`, `normal`, `truncated_normal`, `lognormal`, `uniform`, `triangular`, `deterministic`
- **`foundation_class`**: `Slab`, `CrawlSpace`, `Basement`, `Pier`, `Pile`, `SolidWall`, `FillOrElevated`
- **`peril_type`**: `riverine`, `coastal`, `compound`
- **`ddf_kind`**: `building`, `content`, `inventory`
- **`run_status`**: `queued`, `running`, `success`, `failed`, `superseded`
- **`shuffle_policy`**: `independent`, `region_correlated`, `inventory_locked`
- **`component_type`**: `finish`, `foundation`, `structure`, `contents`, `inventory`
- **`agg_level`**: `building`, `census_block`, `community`
- **`run_type`**: `loss`, `sensitivity`, `calibration`
- **`entity_type`**: `buildings`, `uncertainty_spec`, `uncertainty_correlation`, `ddf_library`, `events`, `asset`, `asset_revision`, `inventory_branch`, `distribution_registry`, `geometries`, `export_contract`, `source_registry`, `component_connection`, `generics`
- **`asset_type`**: `building`, `component`, `virtual`
- **`branch_status`**: `open`, `merged`, `abandoned`
- **`change_kind`**: `attribute_edit`, `geometry_move`, `add`, `delete`, `metadata`
- **`uncertainty_kind`**: `continuous`, `categorical`, `deterministic`
- **`uncertainty_scope`**: `building_specific`, `class_level`, `regional`, `shared`
- **`uncertainty_consumer_type`**: `buildings`, `building_components`, `ddf_uncertainty`, `hazard_links`, `virtual`
- **`correlation_kind`**: `conditional`, `correlation`, `independent`
- **`distribution_family_kind`**: `continuous`, `discrete`, `empirical`
- **`distribution_support_type`**: `real`, `positive`, `unit_interval`, `bounded`, `integer_nonneg`, `empirical`
- **`sampler_backend`**: `scipy`, `numpy`, `custom`
- **`distribution_family_status`**: `active`, `deprecated`
- **`agent_type`**: `person`, `organization`, `software`
- **`activity_type`**: `import`, `fit`, `impute`, `override`, `derive`, `calibrate`
- **`adopted_entity_type`**: `uncertainty_spec`, `ddf_uncertainty`, `hazard_links`, `geometries`
- **`observation_role`**: `primary`, `supporting`, `contradictory`
- **`geometry_role`**: `point`, `footprint`, `envelope_3d`, `detailed_3d`
- **`geometry_purpose`**: `computation`, `qc`, `hazard_sampling`, `display`, `future_export`
- **`export_product_kind`**: `deterministic`, `distribution_parameters`, `pre_sampled_realizations`
- **`export_contract_status`**: `active`, `deprecated`
- **`export_status`**: `queued`, `running`, `success`, `failed`
- **`export_access_level`**: `public`, `internal`, `restricted`
- **`lossiness_reason`**: `not_selected_for_export`, `alternative_representation`, `superseded_by_default`, `redacted_restricted`, `out_of_scope_for_contract`, `collapsed_to_central_estimate`
- **`lossiness_entity_type`**: `geometries`, `uncertainty_spec`, `building_components`, `ddf_uncertainty`, `hazard_links`, `uncertainty_ref`, `source_observation`
- **`access_classification`**: `public`, `internal`, `restricted`
- **`source_kind`**: `national_inventory`, `class_default`, `field_survey`, `remote_sensing`, `commercial_parcel`, `tribal`, `proprietary`, `agency_restricted`, `other`
- **`connection_kind`**: `supports`, `contains`, `adjacent_to`, `attached_to`
- **`generic_kind`**: `building_component`, `infrastructure_component`, `uncertainty_class`, `future_asset_kind`
- **`damage_relevance`**: `primary`, `secondary`, `contents_only`, `inventory_only`

## Domain A — Inventory (coverage of building inventory / components)

### `buildings`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per structure.

Anchor structure inventory, sourced from NSI / HAZUS. Holds identity and immutable/base attributes. Geometry is NOT stored here — ADR-geometry promotes geometry to the `geometries` table, attached to asset_id. Every buildings row must have at least one geometries row with role='point', purpose='computation', is_selected_for_export=true on the current branch (Prompt 11 rule). Probabilistic attributes live in uncertainty_spec and are resolved per realization. Field aliases align to SPHERE core.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `building_id` | bigint | PK | NOT NULL | Surrogate primary key (current-head projection id; stable across revisions). |
| `asset_id` | bigint | FK → `asset.asset_id` | NOT NULL | Stable asset identity (ADR-versioning). Survives geometry / attribute / location changes; revisions materialize in asset_revision. Geometry attaches to asset_id, not building_id (ADR-geometry). |
| `fd_id` | bigint |  |  | Source NSI/HAZUS structure id (alias id/bldg_id/fd_id). |
| `occupancy_type` | varchar(16) |  | NOT NULL | Occupancy/occtype (e.g. RES1 |
| `general_building_type` | varchar(32) |  |  | General construction class (bldgtype). |
| `number_stories` | smallint |  |  | Story count (num_story). |
| `area_sqft` | double |  |  | Footprint/floor area |
| `building_cost` | double |  |  | Structure replacement cost |
| `content_cost` | double |  |  | Contents replacement cost |
| `inventory_cost` | double |  |  | Business inventory cost |
| `census_block` | varchar(15) |  |  | Census block GEOID for community aggregation. |
| `inventory_version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Snapshot/version of the inventory this row belongs to. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `building_components`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per damageable sub-assembly of a building.

Decomposes a structure into modular, shareable components (finish, foundation, structure, contents, inventory) so component-/fragility-level loss can be added without a schema break. Realizes the roadmap "Components" concept (modular, improvable, reproducible).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `component_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `building_id` | bigint | FK → `buildings.building_id` | NOT NULL | Parent structure. |
| `component_type` | varchar(24) |  | NOT NULL | One of finish|foundation|structure|contents|inventory (enum component_type). |
| `generic_id` | bigint | FK → `generics.generic_id` | NOT NULL | Controlled-vocabulary inventory/component type. |
| `replacement_cost` | double |  |  | Component replacement value |
| `notes` | text |  |  | Free-text provenance / assumptions. |

### `component_connection`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per typed edge between two building_components.

Typed component-to-component relationship (ADR-citygml-profile). Encodes what CityGML captures natively for object relationships — but at the minimum shape RQM needs today (roof supports wall supports foundation; HVAC attached to structure; envelope contains HVAC). Both endpoints must belong to the same building (Prompt 11 rule 2). Self-connections and duplicate symmetric adjacent_to pairs are rejected. Component-fragility weights, richer joint vocabularies, and cross-building topology are deferred (see ADR §Deferrals). Connections are versioned so a component swap does not rewrite history.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `connection_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `from_component_id` | bigint | FK → `building_components.component_id` | NOT NULL | Source component. For asymmetric kinds (supports / contains / attached_to): the load-bearing / containing / anchor component. Rule: from_component_id != to_component_id. |
| `to_component_id` | bigint | FK → `building_components.component_id` | NOT NULL | Target component. For supports: the component resting on `from`. For adjacent_to: the other side of the shared boundary (symmetric; single row per unordered pair, Prompt 11 rule 4). |
| `connection_kind` | varchar(24) |  | NOT NULL | supports | contains | adjacent_to | attached_to (enum connection_kind). supports/contains/attached_to are asymmetric; adjacent_to is symmetric. |
| `description` | text |  |  | Free-text elaboration (e.g., "wood-frame roof sits on load-bearing wall, hurricane-strapped"). |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Versioning row so a component swap does not rewrite connection history. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `generics`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per controlled-vocabulary asset/component type (superclass).

Generalized controlled-vocabulary superclass (ADR-generics). Every row has exactly one matching subclass row in exactly one subclass table selected by `kind` (SQL supertype/subtable pattern; identical to uncertainty_spec). v0.1 populates the building_component kind only; infrastructure_component / uncertainty_class / future_asset_kind are reserved discriminator values with no subclass table yet (deferred, declared to keep the extension path open — Prompt 11 rule 5 blocks consumers from referencing them until a subclass lands).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `generic_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `kind` | varchar(32) |  | NOT NULL | building_component | infrastructure_component | uncertainty_class | future_asset_kind (enum generic_kind). Discriminator for the subclass row (Prompt 11 rule 1). |
| `category` | varchar(32) |  | NOT NULL | Top-level class (Buildings, Transportation, Utilities, ...). Consistency with kind enforced by Prompt 11 rule 3 (e.g., kind=building_component implies category=Buildings). |
| `subtype` | varchar(64) |  | NOT NULL | Specific type within the category (e.g. |
| `schema_ref` | varchar(128) |  |  | Pointer to the type-specific attribute schema (extensibility hook for RQM-10). |
| `description` | text |  |  | Free-text description of the type. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Version/lineage of this vocabulary entry. Vocabulary changes bump a new version rather than mutating in place, so historical exports remain reconstructable. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `generic_building_component`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per building_component-kind generic (subclass).

Subclass of generics for kind='building_component' (ADR-generics). Holds building-component-specific defaults that would be null for other kinds (damage relevance, typical replacement-cost share). All columns except generic_id are optional class defaults — the subclass exists to hold building-component-relevant attributes without loading them onto the superclass. Consumer: building_components.generic_id must resolve to a generics row of kind='building_component' (Prompt 11 rule 2).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `generic_id` | bigint | PK | NOT NULL | FK+PK to generics (subtype pattern; same shape as uncertainty_spec subclasses). |
| `damage_relevance` | varchar(16) |  |  | primary | secondary | contents_only | inventory_only (enum damage_relevance). How this component kind participates in loss aggregation. |
| `typical_replacement_cost_fraction` | real |  |  | Component's typical share of building total replacement cost in [0,1] (Prompt 11 rule 6). Compiler fallback when building_components.replacement_cost is missing (ADR-exports imputation_policy). |

### `geometries`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (asset or component, representation, alternate) geometric record.

First-class geometry registry. Each row represents one geometric representation of an asset (or of a specific component of that asset). Multiple rows per asset are expected — point + footprint + optional 3D, plus alternates from different sources at the same role. Exactly one row per (asset_id, purpose='computation') is selected for export at any given branch/valid-time. Component-level attach is optional and structurally consistent (Prompt 11 enforces that a component geometry's asset_id matches its parent component's asset_id). Payload kind is dictated by `role` (point / footprint / 3D). Revisions attach here, not to buildings, so a geometry_move never forces a non-geometric revision.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `geometry_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `asset_id` | bigint | FK → `asset.asset_id` | NOT NULL | Stable asset this geometry represents (ADR-versioning pre-commitment). Required — every geometry rolls up to an asset. |
| `component_id` | bigint | FK → `building_components.component_id` |  | Optional component this geometry represents (foundation, HVAC, roof, ...). Null when the geometry represents the asset itself. Coherent semantical-geometrical rule: when non-null, the component's buildings.asset_id must equal this row's asset_id (Prompt 11). |
| `role` | varchar(24) |  | NOT NULL | point | footprint | envelope_3d | detailed_3d (enum geometry_role). RQM's compact LOD-like tag; envelope_3d / detailed_3d reserved but not populated in v0.1. |
| `crs` | varchar(24) |  | NOT NULL | Coordinate reference system (e.g., EPSG:5070). Per-row so alternates from different sources preserve their native CRS. |
| `geom` | geometry |  | NOT NULL | PostGIS geometry payload. Shape dictated by role: point -> Point; footprint -> Polygon/MultiPolygon; 3D roles -> PolyhedralSurface/TIN when populated. |
| `purpose` | varchar(24) |  | NOT NULL | computation | qc | hazard_sampling | display | future_export (enum geometry_purpose). Encodes why this representation exists. |
| `is_selected_for_export` | boolean |  | NOT NULL | True iff this row is the export-contract-selected representation for its (asset_id, purpose=computation) group on the current branch. Exactly one true per group (Prompt 11 rule 2). |
| `alt_group_key` | varchar(64) |  |  | Optional group key clustering alternates at the SAME (asset_id, role) — e.g., two competing footprints from different sources share one key. Rows sharing a key must share (asset_id, role) (Prompt 11 rule 4). |
| `source_ref` | varchar(128) |  |  | Source dataset / observation reference (e.g., NSI-2024.1, MicrosoftBuildings-2022). Full provenance flows via adopted_value_link with adopted_entity_type=geometries. |
| `validity_version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Bitemporal versioning row (ADR-versioning) for this geometry revision. A geometry_move produces a new row + a revision_log entry keyed to this version_id. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

## Domain B — Attribute Uncertainty (uncertainty quantification)

### `uncertainty_spec`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per shareable uncertainty specification (any kind).

Superclass for every uncertainty specification in the model. Identity, scope, kind, validity, units, version. Every uncertainty_spec has exactly one row in exactly one subclass table (uncertainty_spec_continuous | uncertainty_spec_categorical | uncertainty_spec_deterministic) selected by `kind`. Consumers reference a spec via uncertainty_ref; a single spec may be reused by many consumers (class-level / regional / shared) or by exactly one (building-specific).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `uncertainty_spec_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `kind` | varchar(16) |  | NOT NULL | continuous | categorical | deterministic (enum uncertainty_kind). Discriminator for the subclass row. |
| `attribute_name` | varchar(48) |  | NOT NULL | Attribute this spec describes semantically (e.g., first_floor_height, foundation_class). |
| `units` | varchar(16) |  |  | Physical units of the sampled quantity (ft |
| `scope` | varchar(24) |  | NOT NULL | building_specific | class_level | regional | shared (enum uncertainty_scope). Documentation/QA; actual sharing is expressed by uncertainty_ref rows pointing at this spec. |
| `scope_ref` | varchar(64) |  |  | Class label / region key when scope != building_specific (e.g., "RES1", "AlleghenyCounty"). |
| `fitting_method` | varchar(64) |  |  | Method used to derive the spec (e.g., MLE, class-default, expert-elicitation). |
| `validity_from` | timestamptz |  |  | Real-world start of validity (valid-time). |
| `validity_to` | timestamptz |  |  | Real-world end of validity (null = open-ended). |
| `description` | text |  |  | Free-text description of the spec. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Version/lineage of this spec. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `uncertainty_spec_continuous`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per continuous uncertainty spec.

Continuous subtype of uncertainty_spec. Points at the distribution registry (ADR-distribution-registry) for family + parameterization contract + compute backend. Supports parametric families (normal, lognormal, uniform, triangular, ...) and empirical ensembles via an optional ensemble reference.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `uncertainty_spec_id` | bigint | PK | NOT NULL | FK+PK to uncertainty_spec (subtype pattern). |
| `dist_family` | varchar(48) | FK → `distribution_registry.family_name` | NOT NULL | Distribution family name. FK into distribution_registry (ADR-distribution-registry) — parameter contract and compute backend are enforced there. `parameters` must satisfy the referenced family_name.parameter_contract. |
| `parameters` | jsonb |  | NOT NULL | Family parameters honoring the registry contract (e.g., {"mu":1.5,"sigma":0.4}). |
| `support_type` | varchar(16) |  |  | real | positive | bounded | discrete (documents parameter support). |
| `lower_bound` | real |  |  | Optional lower support bound. |
| `upper_bound` | real |  |  | Optional upper support bound. |
| `empirical_ensemble_ref` | varchar(256) |  |  | URI to an empirical ensemble (e.g., an Icechunk/Iceberg reference) when dist_family="empirical". |

### `uncertainty_spec_categorical`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per categorical uncertainty spec.

Categorical subtype of uncertainty_spec. Holds the PMF as a single jsonb object (category → probability) and the shuffle_policy that governs reproducibility and cross-building correlation. Replaces the former per-(building, class) foundation_pmf rows; the whole PMF is one spec.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `uncertainty_spec_id` | bigint | PK | NOT NULL | FK+PK to uncertainty_spec (subtype pattern). |
| `categories_pmf` | jsonb |  | NOT NULL | PMF as {"Slab":0.6,"CrawlSpace":0.3,"Basement":0.1}; must sum to 1.0 (±1e-6). |
| `shuffle_policy` | varchar(24) |  |  | independent | region_correlated | inventory_locked (enum shuffle_policy); null = default independent. |

### `uncertainty_spec_deterministic`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per deterministic (degenerate) uncertainty spec.

Deterministic subtype of uncertainty_spec. Preserves the locked invariant that a deterministic value is a valid degenerate form of an uncertainty spec, not a special case handled elsewhere. Downstream samplers treat it as a degenerate draw.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `uncertainty_spec_id` | bigint | PK | NOT NULL | FK+PK to uncertainty_spec (subtype pattern). |
| `value` | jsonb |  | NOT NULL | Pinned value; scalar or category, JSON-encoded (e.g., 2, "Slab"). |
| `value_type` | varchar(16) |  | NOT NULL | scalar | category — what kind of value is pinned. |

### `uncertainty_ref`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (consumer, attribute) binding to an uncertainty_spec.

Polymorphic bridge: "consumer X uses spec Y for attribute Z." A single uncertainty_spec may be referenced by many consumers (class/regional /shared) or by exactly one (building-specific). Consumer parent uses the discriminator-on-relationship pattern via consumer_type; the paired consumer_id resolves in the table named by consumer_type (validated in app/CI, same as adopted_value_link).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `ref_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `consumer_type` | varchar(32) |  | NOT NULL | Consumer entity kind (enum uncertainty_consumer_type). Discriminator for the polymorphic consumer. |
| `consumer_id` | bigint |  | NOT NULL | Id in the table named by consumer_type. No hard FK (target varies); validated in application/CI. |
| `attribute_name` | varchar(48) |  | NOT NULL | Attribute for which this consumer uses this spec (matches uncertainty_spec.attribute_name). |
| `uncertainty_spec_id` | bigint | FK → `uncertainty_spec.uncertainty_spec_id` | NOT NULL | Spec being referenced. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `uncertainty_correlation`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per pairwise coupling between two uncertainty specs.

First-class representation of dependencies between two uncertainty specs. Supports conditional (parameters of A depend on drawn value of B), correlation (Pearson/Spearman coefficient between two continuous specs), and independent (explicit declaration of no coupling, used in QA). Full copula / multivariate joint modeling is deferred.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `correlation_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `spec_a_id` | bigint | FK → `uncertainty_spec.uncertainty_spec_id` | NOT NULL | Dependent spec (its draw depends on spec_b or is correlated with it). |
| `spec_b_id` | bigint | FK → `uncertainty_spec.uncertainty_spec_id` | NOT NULL | Conditioning / correlated spec. |
| `kind` | varchar(16) |  | NOT NULL | conditional | correlation | independent (enum correlation_kind). |
| `specification` | jsonb |  |  | Kind-dependent payload. conditional: {category_of_b: params_for_a}. correlation: {rho: 0.7, method: "pearson"}. independent: null. |
| `description` | text |  |  | Free-text description of the dependence. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Version/lineage of this correlation. |

### `distribution_registry`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per supported parametric distribution family.

Compute + rendering contract for every parametric distribution family referenced elsewhere in the model. Each row declares the required parameters (parameter_contract), the support semantics, and the canonical compute backend (sampler/PDF/CDF/PPF callables). Prompt 11 enforces that every consumer's parameters jsonb satisfies the referenced family's contract. categorical and deterministic uncertainty do not live here — they use structural columns on uncertainty_spec_categorical / uncertainty_spec_deterministic instead of parametric jsonb.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `family_name` | varchar(48) | PK | NOT NULL | Family identifier used by consumers (e.g., lognormal, truncated_normal, empirical). Natural key; matches uncertainty_spec_continuous.dist_family / hazard_links.depth_dist_family / ddf_uncertainty.dist_family. |
| `family_kind` | varchar(16) |  | NOT NULL | continuous | discrete | empirical (enum distribution_family_kind). |
| `parameter_contract` | jsonb |  | NOT NULL | Array of {name, json_type, required, constraint, description}. Canonical parameter names for the family. Empty array iff family_kind=empirical. |
| `support_type` | varchar(16) |  | NOT NULL | real | positive | unit_interval | bounded | integer_nonneg | empirical (enum distribution_support_type). |
| `default_lower_bound` | real |  |  | Support hint (optional; overridable per-spec). |
| `default_upper_bound` | real |  |  | Support hint (optional; overridable per-spec). |
| `sampler_backend` | varchar(16) |  | NOT NULL | scipy | numpy | custom (enum sampler_backend). |
| `sampler_ref` | varchar(128) |  | NOT NULL | Canonical callable reference for sampling (e.g., scipy.stats.lognorm.rvs). |
| `pdf_ref` | varchar(128) |  |  | Canonical PDF callable (nullable when backend does not expose one). |
| `cdf_ref` | varchar(128) |  |  | Canonical CDF callable (nullable when backend does not expose one). |
| `ppf_ref` | varchar(128) |  |  | Canonical inverse-CDF / percentile callable (nullable when backend does not expose one). |
| `status` | varchar(16) |  | NOT NULL | active | deprecated (enum distribution_family_status). Consumers may only reference active families. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Version/lineage of the family contract (contract changes bump semver). |
| `description` | text |  |  | Free-text description of the family and its use. |
| `citations` | text |  |  | Optional references / citations for the family. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

## Domain C — Hazard Linkage (probabilistic flood depth)

### `events`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per hazard event / scenario.

Canonical registry of hazard events and scenarios. Gives the event_id referenced by run_catalog, hazard_links, and loss_realizations a single authoritative home so every run, hazard surface, and loss draw ties back to a named, versioned scenario.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `event_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `event_name` | varchar(96) |  | NOT NULL | Human-readable event/scenario name. |
| `peril_type` | varchar(12) |  | NOT NULL | riverine | coastal | compound (enum peril_type). |
| `aep` | real |  |  | Representative annual exceedance probability |
| `return_period` | real |  |  | Representative return period in years |
| `description` | text |  |  | Free-text scenario description / assumptions. |
| `source` | varchar(128) |  |  | Source model or study that defined the event. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Version/lineage of this event definition. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `hazard_links`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (building, AEP/event) hazard association.

Links a structure to a versioned gridded depth surface (Icechunk/Zarr) and its uncertainty layer, instead of copying a scalar depth into the building row. Supports AEP depth rasters plus velocity/duration/uncertainty layers (Inland Consequences pattern).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `hazard_link_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `building_id` | bigint | FK → `buildings.building_id` | NOT NULL | Structure. |
| `event_id` | bigint | FK → `events.event_id` |  | Event/scenario this surface represents (null for pure AEP surfaces). |
| `aep` | real |  |  | Annual exceedance probability of the associated surface. |
| `peril_type` | varchar(12) |  | NOT NULL | riverine | coastal | compound (enum peril_type). |
| `depth_grid_uri` | varchar(256) |  | NOT NULL | Icechunk repo/branch URI for the depth field (versioned). |
| `velocity_grid_uri` | varchar(256) |  |  | Optional velocity field URI. |
| `duration_grid_uri` | varchar(256) |  |  | Optional duration field URI. |
| `depth_dist_family` | varchar(48) | FK → `distribution_registry.family_name` | NOT NULL | Distribution family of depth-in-structure uncertainty. FK into distribution_registry (ADR-distribution-registry); `depth_parameters` must satisfy its parameter contract. |
| `depth_parameters` | jsonb |  | NOT NULL | Params of the depth-in-structure distribution at this point. |
| `grid_version` | varchar(64) |  | NOT NULL | Icechunk snapshot/commit id for reproducibility. |

## Domain D — Depth-Damage Functions (probabilistic DDFs)

### `ddf_library`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per depth-damage function version.

Versioned registry of depth-damage functions keyed by SPHERE ddf ids (bddf_id/cddf_id/iddf_id). Maps functions to occupancy, foundation and peril so damage-function assignment is explicit and auditable.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `ddf_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `ddf_code` | varchar(48) |  | NOT NULL | External id (maps to bddf_id/cddf_id/iddf_id). |
| `ddf_kind` | varchar(12) |  | NOT NULL | building | content | inventory (enum ddf_kind). |
| `peril_type` | varchar(12) |  | NOT NULL | riverine | coastal | compound. |
| `occupancy_type` | varchar(16) |  |  | Occupancy the DDF applies to. |
| `foundation_class` | varchar(24) |  |  | Foundation class the DDF applies to. |
| `source_library` | varchar(64) |  | NOT NULL | Library of origin (e.g. OpenHazus |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Version/lineage of this DDF. |

### `ddf_uncertainty`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (ddf, depth) damage-ratio distribution.

Makes the DDF itself a distribution (ADR-ddf-uncertainty). Each row publishes a canonical mean damage ratio at this depth and, optionally, a reference to an uncertainty_spec that authoritatively describes the spread around that mean. When spread_spec_id is set, the p10/p50/p90 columns are a rendered cache from spec.ppf(q) — Prompt 11 rejects stale caches. When spread_spec_id is null, the percentile columns are the authoritative spread (legacy USACE / FIA / HAZUS envelope shape). When both spread_spec_id AND percentile columns are null, the row is a point DDF (mean only) — legal, flagged as diagnostic by Prompt 11. Supersedes the inline dist_family/parameters columns from ADR-distribution-registry (registry contract now applies indirectly via uncertainty_spec_continuous.dist_family).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `ddf_unc_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `ddf_id` | bigint | FK → `ddf_library.ddf_id` | NOT NULL | Parent DDF. |
| `depth_ft` | real |  | NOT NULL | Flood depth relative to first floor |
| `damage_mean` | real |  | NOT NULL | Canonical mean damage ratio in [0,1] at this depth. Required inline — matches the shape of published USACE/FIA/HAZUS DDFs and is what mv_loss_summary.loss_mean reduces against. Uncertainty on damage_mean itself is out of scope (ADR-ddf-uncertainty §Deferred). |
| `spread_spec_id` | bigint | FK → `uncertainty_spec.uncertainty_spec_id` |  | Authoritative uncertainty_spec for the damage-ratio spread at this depth (ADR-ddf-uncertainty). When set, describes the distribution around damage_mean; parameter contract flows through uncertainty_spec_continuous.dist_family → distribution_registry. Null for point DDFs or legacy percentile-only envelopes. |
| `damage_p10` | real |  |  | 10th percentile damage ratio. Authoritative when spread_spec_id is null; rendered cache (spec.ppf(0.1)) when set — Prompt 11 rejects stale caches. |
| `damage_p50` | real |  |  | Median damage ratio. Authoritative when spread_spec_id is null; rendered cache (spec.ppf(0.5)) when set. |
| `damage_p90` | real |  |  | 90th percentile damage ratio. Authoritative when spread_spec_id is null; rendered cache (spec.ppf(0.9)) when set. |

## Domain E — Realization & Loss Results (ensemble scale)

### `loss_realizations`
**Storage:** Iceberg &nbsp;|&nbsp; **Grain:** One row per (building x event/AEP x Monte Carlo draw).

Ensemble-scale table of per-draw sampled inputs and resulting losses. Iceberg tier for billions of rows with partition pruning and time-travel. Every row is reproducible from its seed + version pointers.

*Partitioned by:* `event_id`, `aep`

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `realization_id` | bigint |  | NOT NULL | Draw identifier within the run. |
| `building_id` | bigint |  | NOT NULL | Structure (logical FK to buildings). |
| `run_id` | bigint |  | NOT NULL | Producing run (logical FK to run_catalog). |
| `event_id` | bigint |  |  | Event/scenario id |
| `aep` | real |  |  | Annual exceedance probability (partition key). |
| `seed` | bigint |  | NOT NULL | RNG seed for exact reproducibility. |
| `depth_in_structure` | real |  | NOT NULL | Sampled depth in structure |
| `foundation_type` | varchar(24) |  | NOT NULL | Sampled/shuffled foundation class this draw. |
| `first_floor_height` | real |  | NOT NULL | Sampled first-floor height |
| `ddf_percentile` | real |  | NOT NULL | Sampled DDF percentile in [0,1]. |
| `bddf_id` | varchar(48) |  |  | Building DDF applied. |
| `cddf_id` | varchar(48) |  |  | Content DDF applied. |
| `iddf_id` | varchar(48) |  |  | Inventory DDF applied. |
| `building_damage_percent` | real |  |  | Structure damage ratio in [0,1]. |
| `building_loss` | double |  |  | Structure loss |
| `content_loss` | double |  |  | Contents loss |
| `inventory_loss` | double |  |  | Inventory loss |
| `total_loss` | double |  |  | Sum of building+content+inventory loss |

### `mv_loss_summary`
**Storage:** Iceberg &nbsp;|&nbsp; **Grain:** One row per (building or community, event/AEP).

Materialized view pre-computing central tendency AND upper prediction limits from loss_realizations. Rebuildable from source. Aggregation tier where variance reduction for large-scale community analysis is realized.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `summary_id` | bigint |  | NOT NULL | Surrogate key. |
| `agg_level` | varchar(16) |  | NOT NULL | building | census_block | community (enum agg_level). |
| `agg_key` | varchar(32) |  | NOT NULL | Identifier at the aggregation level. |
| `event_id` | bigint |  |  | Event/scenario id. |
| `aep` | real |  |  | Annual exceedance probability. |
| `n_realizations` | integer |  | NOT NULL | Number of draws in the aggregate. |
| `loss_mean` | double |  | NOT NULL | Mean total loss |
| `loss_median` | double |  |  | Median total loss |
| `loss_p90` | double |  |  | 90th percentile total loss (upper prediction limit). |
| `loss_p95` | double |  |  | 95th percentile total loss (upper prediction limit). |
| `loss_cv` | real |  |  | Coefficient of variation (variance diagnostic). |
| `aal` | double |  |  | Average annualized loss across AEPs |
| `run_id` | bigint |  | NOT NULL | Producing run for provenance. |

## Domain F — Provenance & Versioning

### `run_catalog`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per cloud-compute run.

One row per run pairing an event/scenario with a model+config version.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `run_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `event_id` | bigint | FK → `events.event_id` |  | Event/scenario executed. |
| `manifest_id` | bigint | FK → `manifests.manifest_id` | NOT NULL | Software/config manifest used. |
| `run_type` | varchar(24) |  | NOT NULL | Kind of run: loss | sensitivity | calibration (enum run_type). |
| `status` | varchar(16) |  | NOT NULL | queued|running|success|failed|superseded. |
| `n_realizations` | integer |  |  | Monte Carlo draws requested. |
| `started_at` | timestamptz |  |  | Run start time. |
| `finished_at` | timestamptz |  |  | Run completion time. |

### `manifests`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per software/config manifest.

Captures the exact software configuration (plugin names, versions, distribution + DDF library versions) so a run is fully reconstructable.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `manifest_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `engine_version` | varchar(32) |  | NOT NULL | Sampling/consequence engine version. |
| `ddf_library_version` | varchar(32) |  |  | DDF library version pinned for the run. |
| `dist_ruleset_version` | varchar(32) |  |  | Attribute-distribution ruleset version. |
| `config` | jsonb |  |  | Full serialized run configuration. |
| `created_at` | timestamptz |  | NOT NULL | Manifest creation timestamp. |

### `run_logs`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per run log pointer.

URI to full container/execution logs for a run.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `log_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `run_id` | bigint | FK → `run_catalog.run_id` | NOT NULL | Run the log belongs to. |
| `log_uri` | varchar(256) |  | NOT NULL | Object-store URI to full logs. |

### `versioning`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per versioned entity snapshot (bitemporal).

Central bitemporal lineage registry (ADR-versioning). Every versioned entity — uncertainty spec, DDF, event, asset revision, branch head — resolves through a version_id and inherits a valid-time interval (real-world) and a record-time interval (knowledge). Bitemporal columns apply only where change actually occurs (locked invariant); reference tables and vocabularies leave valid_from/valid_to null. record_from is non-null on any row referenced by an asset_revision; record_to is null iff the row is the current record for its entity_ref.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `version_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `entity_type` | varchar(32) |  | NOT NULL | Versioned entity kind (enum entity_type). |
| `entity_ref` | varchar(64) |  | NOT NULL | Natural/business key of the versioned entity (e.g., asset:42, uncertainty_spec:201). |
| `semver` | varchar(16) |  | NOT NULL | Semantic version string (e.g. 1.2.0). |
| `parent_version_id` | bigint | FK → `versioning.version_id` |  | Lineage pointer to prior version. |
| `valid_from` | timestamptz |  |  | Real-world start of validity (valid-time). Null for reference tables that do not need bitemporality. |
| `valid_to` | timestamptz |  |  | Real-world end of validity (valid-time). Null = open-ended. |
| `record_from` | timestamptz |  |  | System time this assertion was recorded (record-time / transaction-time). Non-null when referenced by an asset_revision. |
| `record_to` | timestamptz |  |  | System time this assertion was superseded. Null iff current record. |
| `author` | varchar(64) |  |  | Author/owner string (kept for readability; authoritative editor lives in revision_log.editor_agent_id). |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `asset`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per real-world asset identity (stable across all revisions).

Stable identity registry. asset_id survives every geometry, attribute, and location change (locked invariant). Downstream FKs may point at asset_id directly, or via buildings.building_id → buildings.asset_id. Extensible via asset_type — today: building; tomorrow (Prompt 8A): component, geometry, bridge, levee, virtual.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `asset_id` | bigint | PK | NOT NULL | Surrogate stable identity. |
| `asset_type` | varchar(16) |  | NOT NULL | building | component | geometry | virtual (enum asset_type). Hooks Prompt 8A generics generalization. |
| `canonical_external_ref` | varchar(128) |  |  | Optional stable external identifier (e.g., NSI fd_id, cadastral parcel id) preserved across renames. |
| `created_at` | timestamptz |  | NOT NULL | Identity registration timestamp. |

### `inventory_branch`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per named branch of an inventory.

Named branch (main, scenario forks, calibration passes, hypothesis explorations). Every asset_revision carries a branch_id; two reviewers editing the same asset on different branches produce co-existing revisions without conflict. Merging is recorded via merged_into_branch_id; automatic three-way merge is deferred.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `branch_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `name` | varchar(96) |  | NOT NULL | Human-readable branch name (e.g., main, calibration-2026Q3). |
| `parent_branch_id` | bigint | FK → `inventory_branch.branch_id` |  | Parent branch (null for main). No cycles allowed. |
| `purpose` | varchar(64) |  |  | Free text purpose (calibration | scenario | hypothesis | cleanup | ...). |
| `created_by_agent_id` | bigint | FK → `prov_agent.agent_id` | NOT NULL | Agent that created the branch. |
| `created_at` | timestamptz |  | NOT NULL | Branch creation timestamp. |
| `status` | varchar(16) |  | NOT NULL | open | merged | abandoned (enum branch_status). |
| `merged_into_branch_id` | bigint | FK → `inventory_branch.branch_id` |  | Non-null only when status = merged; the branch this one merged back into. |
| `merged_at` | timestamptz |  |  | Merge timestamp when status = merged. |

### `asset_revision`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (asset, branch, versioning) revision.

Materializes a revision of an asset on a specific branch, tied to a versioning row that carries the bitemporal quadruple. The "current head" projection lives in the domain table (e.g., buildings); historical and alternate-branch revisions live here and are reconstructable by view. Uniqueness: exactly one revision per (asset, branch, valid_from).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `revision_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `asset_id` | bigint | FK → `asset.asset_id` | NOT NULL | Stable asset identity this revision belongs to. |
| `branch_id` | bigint | FK → `inventory_branch.branch_id` | NOT NULL | Branch on which this revision was recorded. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Versioning row (bitemporal registry) for this revision. |
| `valid_from` | timestamptz |  | NOT NULL | Real-world start of this revision (denormalized from versioning for query convenience). |
| `valid_to` | timestamptz |  |  | Real-world end of this revision (null = open-ended on this branch). |
| `is_current` | boolean |  | NOT NULL | True iff this revision is the current head on its branch. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `revision_log`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per structured change to a versioned entity.

Editor + reason + diff for each version bump. Complementary to adopted_value_link (which records sources); this table records edits. Together they answer "where did this value come from AND who changed it, when, why." diff schema is recommended (path/before/after) but not formalized — see Prompt 11.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `log_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Versioning row this log entry produced. |
| `editor_agent_id` | bigint | FK → `prov_agent.agent_id` | NOT NULL | Agent responsible for this edit. |
| `change_kind` | varchar(24) |  | NOT NULL | attribute_edit | geometry_move | add | delete | metadata (enum change_kind). |
| `reason` | text |  | NOT NULL | Free-text rationale for the change (required — supports RQM-16). |
| `diff` | jsonb |  |  | Structured diff payload (recommended shape: [{"path":[column],"before":v,"after":v}]). |
| `created_at` | timestamptz |  | NOT NULL | Log insertion timestamp. |

### `prov_agent`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per person, organization, or software system responsible for observations or activities.

Provenance actor. A person (analyst, GIS reviewer), an organization (USACE, FEMA, a contractor), or a software system (fit tool, ETL pipeline). Referenced by source_observation and prov_activity so any adopted value can be traced to responsible parties.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `agent_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `agent_type` | varchar(16) |  | NOT NULL | person | organization | software (enum agent_type). |
| `agent_name` | varchar(128) |  | NOT NULL | Human-readable agent name. |
| `agent_ref` | varchar(256) |  |  | Stable identifier (email |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `prov_activity`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per derivation activity that produced an adopted value.

A single activity that produced or modified an adopted value: import, fit, impute, override, derive, calibrate. Captures the code/tool that ran it and the agent responsible. Multi-step activity chains are not modeled as separate entities at this maturity — only the terminal adopted-value ↔ source-observation link is retained (see docs/decisions/ADR-provenance.md).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `activity_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `activity_type` | varchar(16) |  | NOT NULL | import | fit | impute | override | derive | calibrate (enum activity_type). |
| `method` | varchar(128) |  |  | Method name/reference (e.g., MLE, class-default, manual-edit). |
| `description` | text |  |  | Free-text description of what the activity did. |
| `agent_id` | bigint | FK → `prov_agent.agent_id` | NOT NULL | Agent responsible for running the activity. |
| `code_ref` | varchar(128) |  |  | Tool + commit / package version (e.g., rqm-fit@a1b2c3). |
| `started_at` | timestamptz |  |  | Activity start time. |
| `ended_at` | timestamptz |  |  | Activity end time. |

### `source_registry`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per named source dataset referenced anywhere in the model.

Single policy surface for every source referenced by source_observation. Classification is MANDATORY; stewardship (a real prov_agent) is MANDATORY. Restricted classification requires a restricted_reason and forces all citing observations to be redacted (observed_value=null, is_redacted=true). policy_uri points at external policy documents (DUAs, agency memos) — never an in-repo path (Prompt 11 rule 7). No raw restricted values live in this schema; only references, classifications, and derivatives (see ADR-restricted-sources). Deliberate varchar PK parallels distribution_registry.family_name (readability at reference sites).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `source_ref` | varchar(128) | PK | NOT NULL | Natural key. Matches source_observation.source_ref (e.g., NSI-2024.1, COMMERCIAL-PARCEL-VENDOR-2024Q2, HAZUS-TMv6). |
| `source_name` | varchar(128) |  | NOT NULL | Human-readable dataset name. |
| `source_kind` | varchar(32) |  | NOT NULL | national_inventory | class_default | field_survey | remote_sensing | commercial_parcel | tribal | proprietary | agency_restricted | other (enum source_kind). |
| `access_classification` | varchar(16) |  | NOT NULL | public | internal | restricted (enum access_classification). No default — every source must be classified. |
| `steward_agent_id` | bigint | FK → `prov_agent.agent_id` | NOT NULL | Prov_agent that owns access decisions for this source (no ownerless sources; Prompt 11 rule 4). |
| `policy_uri` | varchar(256) |  |  | External pointer to the access/handling policy (DUA, agency memo). Must be an external scheme — in-repo paths are rejected at CI (Prompt 11 rule 7). Locked invariant: no policy content is embedded here. |
| `restricted_reason` | text |  |  | Required when access_classification=restricted (Prompt 11 rule 3). Rationale (PII / proprietary / security-sensitive / tribal-owned / …). Contains rationale, not protected values. |
| `description` | text |  |  | Free-text; may not include restricted content. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Versioning row. Re-classification bumps a new version rather than mutating in place |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `source_observation`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per atomic raw observation from an external source.

Atomic raw observation from a named source (NSI, HAZUS class default, field survey, remote sensing, commercial parcel data). One adopted value may cite many source_observations via adopted_value_link. Enables the "observation ≠ adopted value" invariant. source_ref FKs into source_registry — classification and steward live there. When the referenced registry row is access_classification=restricted, the observation is stored as a REDACTED STUB: observed_value=null, is_redacted=true (Prompt 11 rules 3 and 8). Raw restricted values are never stored in this schema.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `observation_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `source_ref` | varchar(128) | FK → `source_registry.source_ref` | NOT NULL | Source identifier. FK into source_registry (ADR-restricted-sources) — classification and steward resolve there. |
| `attribute_name` | varchar(48) |  | NOT NULL | Attribute observed (matches uncertainty_spec.attribute_name where applicable). |
| `observed_value` | jsonb |  |  | Raw observed value (scalar, sample, geometry ref, etc.), JSON-encoded. MUST be null when the referenced source is restricted (Prompt 11 rule 8). Null with is_redacted=false means "no value observed"; null with is_redacted=true means "value withheld by policy." |
| `is_redacted` | boolean |  | NOT NULL | True when the raw observed_value has been withheld per source policy. Required true for every observation citing a restricted source (Prompt 11 rule 3). |
| `units` | varchar(16) |  |  | Physical units of the observed quantity (ft |
| `quality` | real |  |  | Analyst/source quality score in [0,1] (optional). |
| `effective_time` | timestamptz |  | NOT NULL | Real-world time this observation is valid for (valid-time; see Prompt 3 for bitemporality). |
| `recorded_at` | timestamptz |  | NOT NULL | System time when the observation was recorded here (record-time). |
| `agent_id` | bigint | FK → `prov_agent.agent_id` | NOT NULL | Agent that recorded this observation. |
| `notes` | text |  |  | Free-text context. |

### `adopted_value_link`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (adopted value, source observation) support link.

Bridge from any adopted entity (attribute distribution, foundation PMF, DDF uncertainty row, hazard link, or geometry after Prompt 4) to the source observations that support it plus the prov_activity that produced it. Polymorphic parent uses the discriminator-on-relationship pattern: (adopted_entity_type, adopted_entity_id) points at exactly one row in the table named by adopted_entity_type. If no observation supports the value (class default, engineering assumption), a single row with is_default_or_assumed=true and a non-null assumption_rationale is required.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `link_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `adopted_entity_type` | varchar(32) |  | NOT NULL | Parent entity kind (enum adopted_entity_type). Discriminator for the polymorphic parent. |
| `adopted_entity_id` | bigint |  | NOT NULL | Id in the parent table named by adopted_entity_type. Not declared as a FK because the target table varies; validated in application/CI. |
| `observation_id` | bigint | FK → `source_observation.observation_id` |  | Supporting observation. Null only when is_default_or_assumed=true. |
| `activity_id` | bigint | FK → `prov_activity.activity_id` | NOT NULL | Activity that produced the adopted value. |
| `role` | varchar(16) |  |  | primary | supporting | contradictory (enum observation_role); null when is_default_or_assumed=true. |
| `is_default_or_assumed` | boolean |  | NOT NULL | True when the adopted value has no supporting observation (class default, engineering assumption). Requires non-null assumption_rationale. |
| `assumption_rationale` | text |  |  | Required when is_default_or_assumed=true. Free-text rationale (e.g., "RES1 default from HAZUS TM v6 §4.3; no site observation."). |
| `created_at` | timestamptz |  | NOT NULL | Link insertion timestamp. |

## Domain G — Exports & Contracts

### `export_contract`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per versioned export-contract definition.

Versioned contract describing what a compiled export must contain, how alternates are resolved, how missing values are imputed, and (for sampled products) how draws are generated. Keyed to a target engine. Access level gates emission of restricted-derived fields (ties into ADR-restricted- sources, Prompt 6). A contract may back many export_product rows; each product cites exactly one active contract version at bind time.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `export_contract_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `name` | varchar(96) |  | NOT NULL | Human-readable contract name (e.g., inland_consequences_flat_v1). |
| `target_engine` | varchar(64) |  | NOT NULL | Engine this contract feeds (e.g., inland_consequences). |
| `product_kind` | varchar(32) |  | NOT NULL | deterministic | distribution_parameters | pre_sampled_realizations (enum export_product_kind). Sets row grain and required-field shape. |
| `required_fields` | jsonb |  | NOT NULL | Array of {name, type, units, code_enum_ref, description, source_selector, imputation_policy, transform_ref?}. source_selector names the canonical value; imputation_policy names the fill rule. |
| `selection_rules` | jsonb |  | NOT NULL | How alternates resolve (geometry purpose+is_selected_for_export; single uncertainty_ref per consumer; DDF version+peril+occupancy match). |
| `sampling_policy` | jsonb |  |  | Required iff product_kind=pre_sampled_realizations: {n_realizations, seed_strategy, shuffle_scope}. Null otherwise (Prompt 11 rule 3). |
| `access_level` | varchar(16) |  | NOT NULL | public | internal | restricted (enum export_access_level). Restricted-derived fields are only emitted when access_level permits (Prompt 6 tie-in). |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Versioning row (bitemporal registry) for this contract revision. |
| `status` | varchar(16) |  | NOT NULL | active | deprecated (enum export_contract_status). Products may only bind to active contracts at started_at. |
| `description` | text |  |  | Free-text description. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `export_product`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per emitted export artifact.

Manifest + reproducibility record for one artifact produced by the export compiler. Pins the canonical inventory snapshot, the bound contract version, the compiler code version, the source checksum, and (for sampled products) the root RNG seed. A single run may emit multiple products (e.g., deterministic + distribution_parameters) — each is its own row. The output_uri points at the object-store or Iceberg landing location.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `export_product_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `run_id` | bigint | FK → `run_catalog.run_id` | NOT NULL | Producing run. |
| `export_contract_id` | bigint | FK → `export_contract.export_contract_id` | NOT NULL | Bound contract. Exactly one per product; must be status=active at started_at (Prompt 11 rule 1). |
| `inventory_snapshot_id` | bigint | FK → `versioning.version_id` | NOT NULL | Canonical inventory snapshot compiled from. |
| `product_kind` | varchar(32) |  | NOT NULL | Must equal the bound contract's product_kind at bind time (Prompt 11 rule 2). |
| `output_uri` | varchar(256) |  | NOT NULL | Object-store URI (S3/GCS/Iceberg) of the emitted artifact. |
| `source_checksum` | varchar(96) |  | NOT NULL | SHA-256 of concatenated source inputs at compile time; supports the immutable-run-snapshot invariant (Prompt 11 rule 9). |
| `compiler_version` | varchar(32) |  | NOT NULL | Semver of the export compiler that produced this artifact. |
| `seed_root` | bigint |  |  | Root RNG seed. Required iff product_kind=pre_sampled_realizations (Prompt 11 rule 3). Draw seeds derived as seed_root + realization_id per sampling_policy. |
| `n_realizations` | integer |  |  | Draw count. Required iff product_kind=pre_sampled_realizations. |
| `row_count` | bigint |  | NOT NULL | Rows in the emitted artifact. |
| `status` | varchar(16) |  | NOT NULL | queued | running | success | failed (enum export_status). |
| `started_at` | timestamptz |  |  | Compilation start time. |
| `finished_at` | timestamptz |  |  | Compilation completion time. |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |

### `export_lossiness_report`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per canonical entity dropped or collapsed by an export.

Machine-readable record of what the contract chose to leave out. Every alternate geometry not selected, every uncertainty collapsed to a central estimate in a deterministic product, every component below the contract's coverage floor, every value imputed rather than observed, and every redaction of a restricted-derived field lands here. Polymorphic parent: (discarded_entity_type, discarded_entity_id) resolves in the table named by discarded_entity_type; no hard FK because the target varies (validated in application/CI, same pattern as adopted_value_link).

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `lossiness_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `export_product_id` | bigint | FK → `export_product.export_product_id` | NOT NULL | Parent export product. |
| `discarded_entity_type` | varchar(32) |  | NOT NULL | geometries | uncertainty_spec | building_components | ddf_uncertainty | hazard_links | uncertainty_ref (enum lossiness_entity_type). Discriminator for the polymorphic reference. |
| `discarded_entity_id` | bigint |  | NOT NULL | Id in the table named by discarded_entity_type. Not declared as an FK; validated in application/CI. |
| `reason` | varchar(48) |  | NOT NULL | not_selected_for_export | alternative_representation | superseded_by_default | redacted_restricted | out_of_scope_for_contract | collapsed_to_central_estimate (enum lossiness_reason). |
| `description` | text |  |  | Free-text elaboration (e.g., "Allegheny cadastre footprint not selected; NSI footprint chosen per contract §3.2"). |
| `created_at` | timestamptz |  | NOT NULL | Row insertion timestamp. |
