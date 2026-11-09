# Risk Quantification Methodology (RQM) Data Model — Data Dictionary

**Version:** 0.1.0 &nbsp;|&nbsp; **Companion:** `fema-ffrd/inland-consequences (SPHERE core schemas)`

Deterministic building-loss inputs (flood depth, foundation type, first-floor height, depth-damage function) are re-architected as first-class, versioned **distribution** objects for probabilistic risk assessment. Storage tiers are inherited from the FFRD data model: **PostgreSQL** (relational integrity), **Iceberg** (ensemble-scale results), **Icechunk/Zarr** (gridded hazard fields).

## Contents

- **Domain A — Inventory (coverage of building inventory / components)**
  - [`buildings`](#buildings)
  - [`building_components`](#building_components)
  - [`generics`](#generics)
- **Domain B — Attribute Uncertainty (uncertainty quantification)**
  - [`attribute_distributions`](#attribute_distributions)
  - [`foundation_pmf`](#foundation_pmf)
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
- **`entity_type`**: `buildings`, `attribute_distributions`, `foundation_pmf`, `ddf_library`, `events`

## Domain A — Inventory (coverage of building inventory / components)

### `buildings`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per structure.

Anchor structure inventory, sourced from NSI / HAZUS. Holds identity, geometry, and immutable/base attributes. Probabilistic attributes are NOT stored here — they live in attribute_distributions and are resolved per realization. Field aliases align to SPHERE core buildings schema.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `building_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `fd_id` | bigint |  |  | Source NSI/HAZUS structure id (alias id/bldg_id/fd_id). |
| `geom` | geometry(Point |  | NOT NULL | Structure point location (CONUS Albers |
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

### `generics`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per controlled-vocabulary asset/component type.

Extensible "Generics" standard. Lets the same core model host non-building inventories (transportation, utilities) in the future without redesign.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `generic_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `category` | varchar(32) |  | NOT NULL | Top-level class (Buildings |
| `subtype` | varchar(64) |  | NOT NULL | Specific type within the category. |
| `schema_ref` | varchar(128) |  |  | Pointer to the type-specific attribute schema. |

## Domain B — Attribute Uncertainty (uncertainty quantification)

### `attribute_distributions`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (building/component, attribute, version).

The heart of the probabilistic model. Replaces a scalar attribute value with a typed, versioned distribution SPECIFICATION. Covers probabilistic first-floor height, the "shuffled" foundation type, and any other uncertain building attribute. Sampling engine reads these rows to draw per-realization values.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `dist_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `building_id` | bigint | FK → `buildings.building_id` | NOT NULL | Structure the distribution applies to. |
| `component_id` | bigint | FK → `building_components.component_id` |  | Optional component scope (null = whole building). |
| `attribute_name` | varchar(48) |  | NOT NULL | Attribute described |
| `dist_family` | varchar(24) |  | NOT NULL | Distribution family (enum dist_family). |
| `parameters` | jsonb |  | NOT NULL | Family params, e.g. {"mu":1.5,"sigma":0.4,"lower":0} or PMF {"Slab":0.6,"Crawl":0.3}. |
| `conditioning` | jsonb |  |  | Dependencies, e.g. FFH conditioned on sampled foundation_class. |
| `units` | varchar(16) |  |  | Physical units of the sampled quantity (ft |
| `source` | varchar(128) |  |  | Data source / method used to fit the distribution. |
| `confidence` | real |  |  | Analyst/model confidence score in [0,1]. |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Version/lineage of this distribution spec. |

### `foundation_pmf`
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (building, foundation_class) probability-mass entry.

Explicit representation of the "shuffled" foundation type. Stores the probability mass per class plus the shuffle/sampling policy so that draws are reproducible and consistent across a community run.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `pmf_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `building_id` | bigint | FK → `buildings.building_id` | NOT NULL | Structure this PMF applies to. |
| `foundation_class` | varchar(24) |  | NOT NULL | Foundation class (enum foundation_class). |
| `probability` | real |  | NOT NULL | Probability mass for the class; sums to 1 per building. |
| `shuffle_policy` | varchar(24) |  | NOT NULL | independent | region_correlated | inventory_locked (enum shuffle_policy). |
| `version_id` | bigint | FK → `versioning.version_id` | NOT NULL | Version of this PMF. |

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
| `depth_dist_family` | varchar(24) |  | NOT NULL | Distribution family of depth-in-structure uncertainty. |
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
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per (ddf, depth) percentile envelope.

Makes the DDF itself a distribution. Stores per-depth central tendency and percentile spread so a realization can draw a DDF percentile rather than using a single mean curve.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `ddf_unc_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `ddf_id` | bigint | FK → `ddf_library.ddf_id` | NOT NULL | Parent DDF. |
| `depth_ft` | real |  | NOT NULL | Flood depth relative to first floor |
| `damage_mean` | real |  | NOT NULL | Mean damage ratio in [0,1] at this depth. |
| `damage_p10` | real |  |  | 10th percentile damage ratio. |
| `damage_p50` | real |  |  | Median damage ratio. |
| `damage_p90` | real |  |  | 90th percentile damage ratio. |
| `dist_family` | varchar(24) |  |  | Optional parametric family of the damage-ratio spread. |
| `parameters` | jsonb |  |  | Optional parametric spread params at this depth. |

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
**Storage:** PostgreSQL &nbsp;|&nbsp; **Grain:** One row per versioned entity snapshot.

The "Versioning" standard applied to every distribution, PMF, DDF, and inventory snapshot so downstream/third-party users can reference versions and log extensions. Central lineage registry.

| Column | Type | Key | Null | Description |
|---|---|---|---|---|
| `version_id` | bigint | PK | NOT NULL | Surrogate primary key. |
| `entity_type` | varchar(32) |  | NOT NULL | Versioned entity kind (enum entity_type). |
| `entity_ref` | varchar(64) |  | NOT NULL | Natural/business key of the versioned entity. |
| `semver` | varchar(16) |  | NOT NULL | Semantic version string (e.g. 1.2.0). |
| `parent_version_id` | bigint | FK → `versioning.version_id` |  | Lineage pointer to prior version. |
| `author` | varchar(64) |  |  | Author/owner of the version. |
| `created_at` | timestamptz |  | NOT NULL | Version creation timestamp. |
