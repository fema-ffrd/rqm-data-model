-- =============================================================================
-- RQM NSI-Minimal Schema — Apache Iceberg DDL (Spark SQL dialect)
-- -----------------------------------------------------------------------------
-- Executed against a Sedona-registered Spark session with an Iceberg catalog
-- named `rqm` (configured in tools/ingest_nsi.py / test/conftest.py).
--
-- Iceberg does not enforce PRIMARY KEY / FOREIGN KEY / NOT NULL in the same
-- way a relational engine does; those are declared here for documentation and
-- enforced by the ingest script + smoke tests. Column names, types, and order
-- mirror data-dictionary/data-dictionary.yaml.
-- =============================================================================

CREATE NAMESPACE IF NOT EXISTS rqm;

-- -----------------------------------------------------------------------------
-- buildings: one row per NSI structure, every parquet field mapped 1:1.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rqm.buildings (
    building_id   BIGINT    NOT NULL COMMENT 'Surrogate PK (generated at ingest).',
    fd_id         BIGINT             COMMENT 'NSI structure id (external natural key).',
    bid           BIGINT             COMMENT 'NSI block id.',
    usastrucid    STRING             COMMENT 'FEMA USA Structures id.',
    ftprntid      STRING             COMMENT 'Footprint id as reported by NSI.',
    ftprntsrc     STRING             COMMENT 'Footprint source tag.',
    source        STRING             COMMENT 'NSI release tag.',
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
    crerank       INT                COMMENT 'NSI community-resilience rank.',
    cbfips        STRING             COMMENT 'Census block GEOID (15-digit FIPS).',
    ingested_at   TIMESTAMP NOT NULL COMMENT 'Row insertion timestamp.'
) USING iceberg;

-- -----------------------------------------------------------------------------
-- geometries: first-class geometry. One row per (building, representation,
-- alternate). NSI load creates one row per structure with role='point'.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rqm.geometries (
    geometry_id            BIGINT    NOT NULL COMMENT 'Surrogate PK.',
    building_id            BIGINT    NOT NULL COMMENT 'FK -> rqm.buildings.building_id (logical).',
    role                   STRING    NOT NULL COMMENT 'point | footprint.',
    purpose                STRING    NOT NULL COMMENT 'computation | hazard_sampling | display | qc.',
    is_selected_for_export BOOLEAN   NOT NULL COMMENT 'Exactly one TRUE per (building_id, purpose=computation).',
    alt_group_key          STRING             COMMENT 'Groups alternates at the same (building_id, role).',
    source_ref             STRING             COMMENT 'Source dataset identifier.',
    crs                    STRING    NOT NULL COMMENT 'EPSG string for this row''s geometry.',
    geom                   BINARY    NOT NULL COMMENT 'Well-Known Binary (WKB). Read via ST_GeomFromWKB(geom); writes serialize with ST_AsBinary(geometry).',
    ingested_at            TIMESTAMP NOT NULL COMMENT 'Row insertion timestamp.'
) USING iceberg;
