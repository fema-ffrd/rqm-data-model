"""Smoke tests for the NSI-minimal ingest.

Asserts the seven validation rules from ADR-nsi-minimal and the five smoke
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
    "fd_id",
    "bid",
    "usastrucid",
    "ftprntid",
    "ftprntsrc",
    "source",
    "occtype",
    "st_damcat",
    "bldgtype",
    "num_story",
    "sqft",
    "ftprntsqft",
    "bldheight",
    "med_yr_blt",
    "found_type",
    "found_ht",
    "val_struct",
    "val_cont",
    "val_vehic",
    "ornl_low",
    "ornl_med",
    "ornl_hgh",
    "resunits",
    "novehprob",
    "vehperunit",
    "pop2amu65",
    "pop2amo65",
    "pop2pmu65",
    "pop2pmo65",
    "u65disable",
    "o65disable",
    "students",
    "firmzone",
    "zone_sub",
    "static_bfe",
    "ground_elv",
    "grnd_elv_m",
    "pctlowclr",
    "fullrep",
    "depindex",
    "creprcnt",
    "crerank",
    "cbfips",
    "x",
    "y",
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


def _write_geoparquet(path: Path, extra_columns: dict, covering: dict | None) -> Path:
    import json

    import pyarrow as pa

    geo_column: dict = {"encoding": "WKB", "geometry_types": ["Point"]}
    if covering is not None:
        geo_column["covering"] = covering
    table = pa.table({"fd_id": [1], "shape": [b"\x00"], **extra_columns})
    geo = {
        "version": "1.1.0",
        "primary_column": "shape",
        "columns": {"shape": geo_column},
    }
    table = table.replace_schema_metadata({"geo": json.dumps(geo)})
    pq.write_table(table, path)
    return path


def test_coverage_check_ignores_geoparquet_covering(tmp_path: Path) -> None:
    """GDAL's GeoParquet 1.1 `shape_bbox` covering is derived, not NSI drift."""
    from ingest_nsi import (  # type: ignore[import-not-found]
        diagnostic_coverage_check,
        geoparquet_covering_columns,
    )

    bbox = {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}
    covering = {"bbox": {k: ["shape_bbox", k] for k in bbox}}
    path = _write_geoparquet(
        tmp_path / "covered.parquet", {"shape_bbox": [bbox]}, covering
    )
    assert geoparquet_covering_columns(pq.read_schema(path)) == {"shape_bbox"}
    assert diagnostic_coverage_check(path) == []


def test_coverage_check_still_reports_undeclared_columns(tmp_path: Path) -> None:
    """A bbox-looking column without covering metadata is still release drift."""
    from ingest_nsi import diagnostic_coverage_check  # type: ignore[import-not-found]

    path = _write_geoparquet(
        tmp_path / "drift.parquet",
        {"shape_bbox": [{"xmin": 0.0}], "new_field": [1]},
        covering=None,
    )
    assert diagnostic_coverage_check(path) == ["new_field", "shape_bbox"]


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

    `geom` is a native GEOMETRY(3857) column; Sedona validates it after the
    `spark_st_asbinary` bridge. Both the column SRID and the `crs` column
    must agree on EPSG:3857.
    """
    from ingest_nsi import sedona_geom_sql  # type: ignore[import-not-found]

    spark, cat = ingested_spark
    invalid = spark.sql(
        f"""
        SELECT COUNT(*) AS n_invalid
        FROM {cat}.geometries
        WHERE NOT ST_IsValid({sedona_geom_sql()})
           OR spark_st_srid(geom) != 3857
           OR crs != 'EPSG:3857'
        """
    ).collect()[0]["n_invalid"]
    assert invalid == 0, f"{invalid} geometries failed validity / CRS check"


def test_tables_are_iceberg_v3_with_native_geometry(ingested_spark) -> None:
    """Both tables are Iceberg format-version 3 and `geom` is a native
    GEOMETRY(3857) column (not WKB BINARY).
    """
    spark, cat = ingested_spark
    for table in ("buildings", "geometries"):
        props = {
            r.key: r.value
            for r in spark.sql(f"SHOW TBLPROPERTIES {cat}.{table}").collect()
        }
        assert props.get("format-version") == "3", f"{table}: {props}"
        # Versioned loads and edits write deletion vectors, not file rewrites.
        for mode in ("write.delete.mode", "write.update.mode", "write.merge.mode"):
            assert props.get(mode) == "merge-on-read", (
                f"{table}: {mode}={props.get(mode)}"
            )
    geom_type = spark.table(f"{cat}.geometries").schema["geom"].dataType.simpleString()
    assert geom_type == "geometry(3857)", geom_type


def test_building_id_is_stable_fd_id(ingested_spark) -> None:
    """ADR-nsi-minimal rule 7: `building_id` equals the NSI `fd_id` and is
    unique, so ids survive re-ingest and partition-count changes.
    """
    spark, cat = ingested_spark
    row = spark.sql(
        f"""
        SELECT COUNT(*)                                           AS n,
               COUNT(DISTINCT building_id)                        AS n_distinct,
               SUM(CASE WHEN building_id <=> fd_id THEN 0 ELSE 1 END) AS n_mismatch
        FROM {cat}.buildings
        """
    ).collect()[0]
    assert row["n_mismatch"] == 0, (
        f"{row['n_mismatch']} rows where building_id != fd_id"
    )
    assert row["n_distinct"] == row["n"], "building_id is not unique"


def test_geometry_id_is_stable_natural_key_hash(ingested_spark) -> None:
    """`geometry_id` is the xxhash64 of the natural key, unique, and pinned to
    a known value so a change to the hashing scheme (which would re-key every
    geometry) fails loudly.
    """
    from ingest_nsi import geometry_id_expr  # type: ignore[import-not-found]
    from pyspark.sql import functions as F

    spark, cat = ingested_spark
    geoms = spark.table(f"{cat}.geometries")
    row = geoms.select(
        F.count("*").alias("n"),
        F.countDistinct("geometry_id").alias("n_distinct"),
        F.sum((F.col("geometry_id") != geometry_id_expr()).cast("int")).alias(
            "n_mismatch"
        ),
    ).collect()[0]
    assert row["n_mismatch"] == 0, (
        f"{row['n_mismatch']} rows where geometry_id != hash of natural key"
    )
    assert row["n_distinct"] == row["n"], "geometry_id is not unique"

    pinned = (
        geoms.where("building_id = 120262564")
        .select("source_ref", "geometry_id")
        .collect()
    )
    assert [(r.source_ref, r.geometry_id) for r in pinned] == [
        ("NSI-P", -7205520827729515936)
    ]


def test_geometry_id_collision_guard(ingested_spark) -> None:
    """The ingest refuses rows whose geometry_id matches a different natural
    key, but re-ingesting an identical key is not a collision.
    """
    from ingest_nsi import assert_no_geometry_id_collisions  # type: ignore[import-not-found]

    spark, _ = ingested_spark

    def rows(building_id: int):
        # SQL VALUES keeps this in the JVM (no Python worker processes).
        return spark.sql(
            "SELECT CAST(1 AS BIGINT) AS geometry_id, 'NSI-P' AS source_ref, "
            f"CAST({building_id} AS BIGINT) AS building_id, 'point' AS role, "
            "'computation' AS purpose, CAST(NULL AS STRING) AS alt_group_key"
        )

    existing = rows(10)
    assert_no_geometry_id_collisions(rows(10), existing)
    with pytest.raises(ValueError, match="collision"):
        assert_no_geometry_id_collisions(rows(11), existing)


def test_crerank_keeps_fractional_values(nsi_parquet: Path, ingested_spark) -> None:
    """`crerank` is fractional in NSI (0-1); it is stored as FLOAT, so values
    match the parquet instead of truncating to 0.
    """
    from pyspark.sql import functions as F

    spark, cat = ingested_spark
    raw = spark.read.parquet(str(nsi_parquet)).select(
        F.col("fd_id").alias("building_id"), F.col("crerank").alias("raw_crerank")
    )
    row = (
        spark.table(f"{cat}.buildings")
        .join(raw, "building_id")
        .select(
            F.max(F.abs(F.col("crerank") - F.col("raw_crerank"))).alias("max_abs_diff"),
            F.sum(
                (F.col("crerank").isNull() != F.col("raw_crerank").isNull()).cast("int")
            ).alias("null_mismatch"),
            F.sum((F.col("crerank") != F.floor("crerank")).cast("int")).alias(
                "n_fractional"
            ),
        )
        .collect()[0]
    )
    assert (
        spark.table(f"{cat}.buildings").schema["crerank"].dataType.simpleString()
        == "float"
    )
    assert row["null_mismatch"] == 0
    assert row["max_abs_diff"] is not None and row["max_abs_diff"] < 1e-6, row
    assert row["n_fractional"] > 0, (
        "expected fractional crerank values in the NSI sample"
    )


def test_default_table_layouts(ingested_spark) -> None:
    """`buildings` is partitioned by state (truncate(2, cbfips)) and sorted by
    cbfips; `geometries` keeps the default unpartitioned, unsorted layout.
    """
    spark, cat = ingested_spark

    def partitioning(table: str) -> list[str]:
        rows = spark.sql(f"DESCRIBE TABLE {cat}.{table}").collect()
        return [r.data_type for r in rows if r.col_name.startswith("Part ")]

    def tblprops(table: str) -> dict[str, str]:
        return {
            r.key: r.value
            for r in spark.sql(f"SHOW TBLPROPERTIES {cat}.{table}").collect()
        }

    assert partitioning("buildings") == ["truncate(2, cbfips)"]
    assert tblprops("buildings").get("sort-order") == "cbfips ASC NULLS FIRST"
    assert partitioning("geometries") == []
    assert "sort-order" not in tblprops("geometries")


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
    assert violators == 0, (
        f"{violators} buildings violate the selected-geometry invariant"
    )


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


def test_points_in_polygon_spatial_join(ingested_spark) -> None:
    """A Sedona broadcast spatial join of geometries against a polygon (the
    watershed subset in the demo export) returns exactly the points inside it.

    Without the Sedona JARs on the driver classpath this fails with
    IllegalAccessError while serializing the JTS index.
    """
    from ingest_nsi import sedona_geom_sql  # type: ignore[import-not-found]

    spark, cat = ingested_spark
    pts = f"{cat}.geometries"
    # Inner half of the sample's extent, in EPSG:3857.
    spark.sql(
        f"""
        SELECT ST_PolygonFromEnvelope(
                   x0 + (x1 - x0) / 4, y0 + (y1 - y0) / 4,
                   x1 - (x1 - x0) / 4, y1 - (y1 - y0) / 4) AS area
        FROM (SELECT MIN(ST_X(p)) AS x0, MAX(ST_X(p)) AS x1,
                     MIN(ST_Y(p)) AS y0, MAX(ST_Y(p)) AS y1
              FROM (SELECT {sedona_geom_sql()} AS p FROM {pts}))
        """
    ).createOrReplaceTempView("test_area")

    joined = spark.sql(
        f"""
        SELECT /*+ BROADCAST(a) */ g.building_id
        FROM {pts} g JOIN test_area a ON ST_Contains(a.area, {sedona_geom_sql("g.geom")})
        """
    )
    assert (
        "BroadcastIndexJoin" in joined._jdf.queryExecution().executedPlan().toString()
    )
    expected = spark.sql(
        f"""
        SELECT COUNT(*) AS n
        FROM (SELECT {sedona_geom_sql()} AS p FROM {pts}) CROSS JOIN test_area
        WHERE ST_X(p) > ST_XMin(area) AND ST_X(p) < ST_XMax(area)
          AND ST_Y(p) > ST_YMin(area) AND ST_Y(p) < ST_YMax(area)
        """
    ).collect()[0]["n"]
    assert 0 < joined.count() == expected


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
