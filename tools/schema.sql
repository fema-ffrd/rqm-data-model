-- =============================================================================
-- RQM NSI-Minimal Schema — Apache Iceberg v3 DDL (Spark SQL dialect)
-- -----------------------------------------------------------------------------
-- Executed against a Sedona-registered Spark 4.1 session with an Iceberg catalog
-- named `rqm` (configured in tools/ingest_nsi.py / test/conftest.py).
--
-- Tables use Iceberg format-version 3 so `geometries.geom` is a native
-- GEOMETRY column. Partition specs and write orders are not declared here:
-- `apply_schema` appends them at creation from `TableLayout`
-- (DEFAULT_LAYOUTS in tools/ingest_nsi.py).
--
-- Iceberg does not enforce PRIMARY KEY / FOREIGN KEY / NOT NULL in the same
-- way a relational engine does; those are declared here for documentation and
-- enforced by the ingest script + smoke tests. Column names, types, and order
-- mirror data-dictionary/data-dictionary.yaml.
-- =============================================================================
-- Every table lives in the `inventory` namespace of the Iceberg catalog `rqm`
-- (REST catalogs reject tables at the catalog root). `apply_schema` rewrites
-- the `rqm.inventory` prefix to the configured catalog and namespace. The
-- CREATE NAMESPACE is catalog-qualified, so it runs against the Iceberg
-- catalog rather than Spark's session catalog.
--
-- buildings and geometries use merge-on-read row-level writes: a versioned
-- re-ingest (MERGE) or edit records Iceberg v3 deletion vectors instead of
-- rewriting whole data files, so each version stores only what changed.
-- Versions themselves are Iceberg tags; see docs/decisions/ADR-versioning.md.
-- =============================================================================

CREATE NAMESPACE IF NOT EXISTS rqm.inventory;

-- Columns added to an existing table after its creation are migrated by
-- `apply_schema` (ALTER TABLE ADD COLUMN, in DDL position, nullable): Iceberg
-- cannot add a required column, and Spark 4.1 does not support column
-- defaults, so readers coalesce NULLs in migrated columns
-- (`ingest_nsi.conform_buildings`).

-- -----------------------------------------------------------------------------
-- buildings: one row per structure per source inventory (ADR-multi-source).
-- The NSI field set is the shared column schema; every source maps onto it
-- through a YAML spec (tools/sources.py), and unmapped fields are NULL.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rqm.inventory.buildings (
    building_id   BIGINT    NOT NULL COMMENT 'Stable PK. NSI rows: the fd_id. Other sources: xxhash64(source_ref, source_record_id) with the sign bit set (always negative).',
    source_ref    STRING    NOT NULL COMMENT 'FK -> sources.source_ref: the inventory this row came from (NSI for the National Structure Inventory).',
    source_record_id STRING          COMMENT 'Native id of the structure in its source (NSI: fd_id as a string).',
    fd_id         BIGINT             COMMENT 'NSI structure id (external natural key; source of building_id for NSI rows).',
    bid           STRING             COMMENT 'NSI building id (UBID, e.g. 8645XMJC+QG9-2-2-3-1).',
    usastrucid    STRING             COMMENT 'FEMA USA Structures id.',
    ftprntid      STRING             COMMENT 'Footprint id as reported by NSI.',
    ftprntsrc     STRING             COMMENT 'Footprint source tag.',
    source        STRING             COMMENT 'NSI per-structure source code (e.g. P, H); prefixes geometries.source_ref.',
    occtype       STRING    NOT NULL COMMENT 'Occupancy code (enum occtype).',
    st_damcat     STRING             COMMENT 'Damage category (enum st_damcat).',
    bldgtype      STRING             COMMENT 'Construction class (W1, M1, S1, ...).',
    num_story     SMALLINT           COMMENT 'Story count.',
    sqft          DOUBLE             COMMENT 'Reported structure area, square feet.',
    ftprntsqft    DOUBLE             COMMENT 'Footprint-derived area, square feet.',
    bldheight     FLOAT              COMMENT 'Building height, feet.',
    med_yr_blt    SMALLINT           COMMENT 'Median year built.',
    found_type    STRING             COMMENT 'Foundation type code (enum found_type).',
    found_ht      FLOAT              COMMENT 'First-floor height above grade, feet.',
    val_struct    DOUBLE             COMMENT 'Structure replacement value, USD.',
    val_cont      DOUBLE             COMMENT 'Contents replacement value, USD.',
    val_vehic     DOUBLE             COMMENT 'Vehicle value, USD.',
    ornl_low      DOUBLE             COMMENT 'ORNL low-value estimate, USD.',
    ornl_med      DOUBLE             COMMENT 'ORNL median-value estimate, USD.',
    ornl_hgh      DOUBLE             COMMENT 'ORNL high-value estimate, USD.',
    resunits      FLOAT              COMMENT 'Residential units.',
    novehprob     FLOAT              COMMENT 'No-vehicle probability.',
    vehperunit    FLOAT              COMMENT 'Vehicles per residential unit.',
    pop2amu65     FLOAT              COMMENT '2 AM population, under 65.',
    pop2amo65     FLOAT              COMMENT '2 AM population, 65 and over.',
    pop2pmu65     FLOAT              COMMENT '2 PM population, under 65.',
    pop2pmo65     FLOAT              COMMENT '2 PM population, 65 and over.',
    u65disable    FLOAT              COMMENT 'Under-65 disabled population.',
    o65disable    FLOAT              COMMENT '65-and-over disabled population.',
    students      FLOAT              COMMENT 'Student population.',
    firmzone      STRING             COMMENT 'FIRM flood zone code.',
    zone_sub      STRING             COMMENT 'FIRM zone subtype.',
    static_bfe    FLOAT              COMMENT 'Static base flood elevation, feet.',
    ground_elv    FLOAT              COMMENT 'Ground elevation, feet.',
    grnd_elv_m    FLOAT              COMMENT 'Ground elevation, meters.',
    pctlowclr     FLOAT              COMMENT 'NSI pctlowclr indicator.',
    fullrep       FLOAT              COMMENT 'NSI fullrep indicator.',
    depindex      FLOAT              COMMENT 'NSI dependency index.',
    creprcnt      FLOAT              COMMENT 'NSI community-resilience percent.',
    crerank       FLOAT              COMMENT 'NSI community-resilience rank (fractional, 0-1).',
    cbfips        STRING             COMMENT 'Census block GEOID (15-digit FIPS).',
    x             DOUBLE             COMMENT 'Longitude (WGS84) of the structure point, as NSI publishes it; denormalized from the computation geometry.',
    y             DOUBLE             COMMENT 'Latitude (WGS84) of the structure point, as NSI publishes it; denormalized from the computation geometry.',
    ingested_at   TIMESTAMP NOT NULL COMMENT 'When this row was last written (insert, or update by a load that changed it).'
) USING iceberg
TBLPROPERTIES (
    'format-version'    = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read'
);

-- -----------------------------------------------------------------------------
-- geometries: first-class geometry. One row per (building, representation,
-- alternate). Every load creates one row per structure with role='point',
-- purpose='computation'; polygon sources add a role='footprint' row.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rqm.inventory.geometries (
    geometry_id            BIGINT    NOT NULL COMMENT 'Stable PK: xxhash64 of (source_ref, building_id, role, purpose, alt_group_key); same id on every re-ingest.',
    building_id            BIGINT    NOT NULL COMMENT 'FK -> buildings.building_id (logical).',
    role                   STRING    NOT NULL COMMENT 'point | footprint.',
    purpose                STRING    NOT NULL COMMENT 'computation | hazard_sampling | display | qc.',
    is_selected_for_export BOOLEAN   NOT NULL COMMENT 'Exactly one TRUE per (building_id, purpose=computation).',
    alt_group_key          STRING             COMMENT 'Groups alternates at the same (building_id, role).',
    source_ref             STRING             COMMENT 'Source dataset identifier: the building''s sources.source_ref, or <source_ref>-<subcode> (NSI-P, NSI-H).',
    crs                    STRING    NOT NULL COMMENT 'EPSG string for the geometry in this row; must equal the geom column CRS (EPSG:3857).',
    geom                   GEOMETRY(3857) NOT NULL COMMENT 'Native Iceberg v3 geometry, EPSG:3857. Sedona reads via ST_GeomFromWKB(spark_st_asbinary(geom)); writes via spark_st_setsrid(spark_st_geomfromwkb(wkb), 3857).',
    ingested_at            TIMESTAMP NOT NULL COMMENT 'When this row was last written (insert, or update by a load that changed it).'
) USING iceberg
TBLPROPERTIES (
    'format-version'    = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode'  = 'merge-on-read'
);

-- -----------------------------------------------------------------------------
-- sources: registry of the inventories loaded into buildings (ADR-multi-source).
-- Upserted from the source's YAML spec on every load, on main only (never
-- branched), and never deleted, so every version's source_ref values resolve.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rqm.inventory.sources (
    source_ref   STRING    NOT NULL COMMENT 'PK; matches buildings.source_ref. Upper-case letters, digits, ".", "_", "-"; names a dataset lineage, stable across releases.',
    source_name  STRING    NOT NULL COMMENT 'Human-readable name.',
    source_kind  STRING    NOT NULL COMMENT 'Enum source_kind: national_inventory | local_inventory | commercial_inventory | project_inventory | field_survey | remote_sensing | other.',
    description  STRING             COMMENT 'Free text.',
    spec_sha256  STRING             COMMENT 'SHA-256 of the YAML mapping spec last used to load the source.',
    updated_at   TIMESTAMP NOT NULL COMMENT 'When this row was last written.'
) USING iceberg
TBLPROPERTIES ('format-version' = '3');

-- -----------------------------------------------------------------------------
-- inventory_versions: version registry. One row per named inventory version.
-- The version name is also an Iceberg tag on buildings and geometries pointing
-- at the recorded snapshots. The row is written last, so a version exists only
-- once it is registered; tags without a row are leftovers of a failed commit.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rqm.inventory.inventory_versions (
    version_name           STRING    NOT NULL COMMENT 'PK; also the Iceberg tag name on buildings and geometries.',
    parent_version         STRING             COMMENT 'Version this one was derived from (NULL for a root load); for merges, the previous version on the target branch.',
    merged_version         STRING             COMMENT 'For kind=merge: the branch version merged in.',
    branch                 STRING    NOT NULL COMMENT 'Iceberg branch the version was committed on (main or a project branch).',
    kind                   STRING    NOT NULL COMMENT 'release | edit | merge (enum version_kind).',
    source_ref             STRING             COMMENT 'FK -> sources.source_ref: the source a release loaded (NULL for edits, merges, and versions committed before multi-source support).',
    source_uri             STRING             COMMENT 'Input file loaded for this version (NULL for in-place edits and merges).',
    source_sha256          STRING             COMMENT 'SHA-256 of the input file.',
    editor                 STRING    NOT NULL COMMENT 'Who committed the version.',
    reason                 STRING             COMMENT 'Why the version was committed.',
    buildings_snapshot_id  BIGINT    NOT NULL COMMENT 'Iceberg snapshot of buildings tagged with version_name.',
    geometries_snapshot_id BIGINT    NOT NULL COMMENT 'Iceberg snapshot of geometries tagged with version_name.',
    n_buildings            BIGINT    NOT NULL COMMENT 'Buildings in the version.',
    n_added                BIGINT    NOT NULL COMMENT 'Buildings added relative to parent_version (all of them for a root).',
    n_removed              BIGINT    NOT NULL COMMENT 'Buildings removed relative to parent_version.',
    n_changed              BIGINT    NOT NULL COMMENT 'Buildings whose attributes or computation point changed relative to parent_version.',
    created_at             TIMESTAMP NOT NULL COMMENT 'When the version was committed (record time).'
) USING iceberg
TBLPROPERTIES ('format-version' = '3');

-- -----------------------------------------------------------------------------
-- inventory_changes: per-building change log. One row per building that a
-- version added, removed, or modified relative to its parent_version. Root
-- versions have no rows (every building in them is new).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rqm.inventory.inventory_changes (
    version_name    STRING        NOT NULL COMMENT 'FK -> inventory_versions.version_name.',
    building_id     BIGINT        NOT NULL COMMENT 'FK -> buildings.building_id (logical; the row may be gone in later versions).',
    change_kind     STRING        NOT NULL COMMENT 'added | removed | modified (enum change_kind).',
    changed_columns ARRAY<STRING>          COMMENT 'For modified: buildings columns that changed, plus geom when the selected computation point moved.'
) USING iceberg
TBLPROPERTIES ('format-version' = '3');
