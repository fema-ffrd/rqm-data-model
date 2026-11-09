"""Smoke tests for the NSI-minimal ingest.

Asserts the six validation rules from ADR-nsi-minimal and the five smoke
queries from the implementation plan. The session-scoped `ingested_spark`
fixture in conftest.py handles the slow Spark boot + initial load.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest


# NSI parquet column names (sans `shape`) that the schema promises to map.
# Kept in sync with tools/ingest_nsi.py :: NSI_SCALAR_FIELDS.
EXPECTED_NSI_COLUMNS = {
    "fd_id", "bid", "usastrucid", "ftprntid", "ftprntsrc", "source",
    "occtype", "st_damcat", "bldgtype", "num_story", "sqft", "ftprntsqft",
    "bldheight", "med_yr_blt", "found_type", "found_ht",
    "val_struct", "val_cont", "val_vehic", "ornl_low", "ornl_med", "ornl_hgh",
    "resunits", "novehprob", "vehperunit",
    "pop2amu65", "pop2amo65", "pop2pmu65", "pop2pmo65",
    "u65disable", "o65disable", "students",
    "firmzone", "zone_sub", "static_bfe", "ground_elv", "grnd_elv_m",
    "pctlowclr", "fullrep", "depindex", "creprcnt", "crerank", "cbfips",
    "shape",
}


def test_field_coverage(nsi_parquet: Path) -> None:
    """Every NSI parquet column is either mapped to buildings or geometries,
    and every expected column is actually present in the parquet.

    Guards against NSI release drift silently dropping fields.
    """
    pf = pq.ParquetFile(str(nsi_parquet))
    parquet_cols = {f.name for f in pf.schema_arrow}
    unmapped = parquet_cols - EXPECTED_NSI_COLUMNS
    missing = EXPECTED_NSI_COLUMNS - parquet_cols
    assert not unmapped, f"NSI columns present but not mapped: {sorted(unmapped)}"
    assert not missing, f"NSI columns expected but not present: {sorted(missing)}"


def test_buildings_row_count_matches_parquet(nsi_parquet: Path, ingested_spark) -> None:
    """Smoke query 1: buildings row count equals parquet row count."""
    spark, cat = ingested_spark
    n_rows_parquet = pq.ParquetFile(str(nsi_parquet)).metadata.num_rows
    n_rows_buildings = spark.table(f"{cat}.buildings").count()
    assert n_rows_buildings == n_rows_parquet


def test_counts_by_occupancy_have_no_null_occtype(ingested_spark) -> None:
    """Smoke query 2: SELECT occtype, COUNT(*) GROUP BY occtype — no NULL occtype."""
    spark, cat = ingested_spark
    counts = spark.sql(
        f"SELECT occtype, COUNT(*) AS n FROM {cat}.buildings GROUP BY occtype ORDER BY n DESC"
    ).collect()
    assert counts, "expected at least one occtype group"
    assert all(row["occtype"] is not None for row in counts), (
        f"NULL occtype present: {[r['occtype'] for r in counts if r['occtype'] is None]}"
    )


def test_geometry_validity_and_srid(ingested_spark) -> None:
    """Smoke query 3: all geometries are valid and in EPSG:3857.

    `geom` is WKB (BINARY) in storage; wrap with ST_GeomFromWKB to validate.
    SRID is set by ST_SetSRID because WKB loses the EPSG metadata at write time;
    we check against the `crs` column instead.
    """
    spark, cat = ingested_spark
    invalid = spark.sql(
        f"""
        SELECT COUNT(*) AS n_invalid
        FROM {cat}.geometries
        WHERE NOT ST_IsValid(ST_GeomFromWKB(geom))
           OR crs != 'EPSG:3857'
        """
    ).collect()[0]["n_invalid"]
    assert invalid == 0, f"{invalid} geometries failed validity / CRS check"


def test_value_totals_by_census_block(ingested_spark) -> None:
    """Smoke query 4: top-cbfips totals have positive value and non-null cbfips."""
    spark, cat = ingested_spark
    top = spark.sql(
        f"""
        SELECT cbfips,
               SUM(val_struct) AS struct_val,
               SUM(val_cont)   AS cont_val,
               COUNT(*)        AS n
        FROM {cat}.buildings
        WHERE cbfips IS NOT NULL
        GROUP BY cbfips
        ORDER BY struct_val DESC NULLS LAST
        LIMIT 10
        """
    ).collect()
    assert top, "expected at least one cbfips group"
    assert top[0]["cbfips"] is not None
    assert top[0]["struct_val"] is not None and top[0]["struct_val"] > 0


def test_selected_geometry_invariant(ingested_spark) -> None:
    """Smoke query 5 (ADR-nsi-minimal rule 2): exactly one
    is_selected_for_export=TRUE per (building_id, purpose='computation').
    """
    spark, cat = ingested_spark
    violators = spark.sql(
        f"""
        SELECT building_id, COUNT(*) AS n
        FROM {cat}.geometries
        WHERE purpose = 'computation' AND is_selected_for_export = TRUE
        GROUP BY building_id
        HAVING COUNT(*) != 1
        """
    ).count()
    assert violators == 0, f"{violators} buildings violate the selected-geometry invariant"


def test_every_building_has_a_computation_point(ingested_spark) -> None:
    """ADR-nsi-minimal rule 1: every buildings row has at least one
    geometries row with role='point', purpose='computation',
    is_selected_for_export=true.
    """
    spark, cat = ingested_spark
    missing = spark.sql(
        f"""
        SELECT b.building_id
        FROM {cat}.buildings b
        LEFT JOIN {cat}.geometries g
          ON g.building_id = b.building_id
         AND g.role        = 'point'
         AND g.purpose     = 'computation'
         AND g.is_selected_for_export = TRUE
        WHERE g.geometry_id IS NULL
        """
    ).count()
    assert missing == 0, f"{missing} buildings lack a selected computation point"


def test_diagnostic_missing_values(ingested_spark, capsys) -> None:
    """Non-asserting diagnostic: print NSI release completeness."""
    spark, cat = ingested_spark
    row = spark.sql(
        f"""
        SELECT
            SUM(CASE WHEN found_type IS NULL THEN 1 ELSE 0 END) AS missing_found_type,
            SUM(CASE WHEN val_struct IS NULL THEN 1 ELSE 0 END) AS missing_val_struct,
            SUM(CASE WHEN bldheight  IS NULL THEN 1 ELSE 0 END) AS missing_height,
            COUNT(*)                                            AS total
        FROM {cat}.buildings
        """
    ).collect()[0]
    with capsys.disabled():
        print(
            f"\n[diagnostic] missing: found_type={row['missing_found_type']}, "
            f"val_struct={row['missing_val_struct']}, "
            f"bldheight={row['missing_height']} / total={row['total']}"
        )
