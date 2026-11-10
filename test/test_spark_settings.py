"""Unit tests for the session / catalog / layout configuration helpers in
tools/ingest_nsi.py. These are pure functions, so no Spark session is needed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from ingest_nsi import (  # type: ignore[import-not-found]
    DEFAULT_LAYOUTS,
    SPARK_PACKAGES,
    SparkSettings,
    TableLayout,
    apply_schema,
    layout_sql,
    layouts_from_args,
    native_geom_sql,
    sedona_geom_sql,
    spark_conf,
    warehouse_uri,
)


def test_local_defaults(tmp_path: Path) -> None:
    conf = spark_conf(SparkSettings(warehouse=tmp_path))
    assert conf["spark.master"] == "local[*]"
    assert conf["spark.sql.geospatial.enabled"] == "true"
    assert conf["spark.jars.packages"] == ",".join(SPARK_PACKAGES)
    assert conf["spark.sql.catalog.rqm"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.rqm.type"] == "hadoop"
    assert conf["spark.sql.catalog.rqm.warehouse"] == tmp_path.resolve().as_uri()


def test_cluster_style_settings() -> None:
    settings = SparkSettings(
        warehouse="s3://bucket/rqm",
        catalog="lake",
        catalog_type="rest",
        catalog_properties={"uri": "https://catalog.example/api"},
        master=None,
        packages=(),
        conf={
            "spark.sql.shuffle.partitions": "400",
            "spark.sql.catalog.lake.type": "glue",
        },
    )
    conf = spark_conf(settings)
    assert "spark.master" not in conf
    assert "spark.jars.packages" not in conf
    assert conf["spark.sql.catalog.lake.warehouse"] == "s3://bucket/rqm"
    assert conf["spark.sql.catalog.lake.uri"] == "https://catalog.example/api"
    assert conf["spark.sql.shuffle.partitions"] == "400"
    # Explicit `conf` wins over derived values.
    assert conf["spark.sql.catalog.lake.type"] == "glue"


def test_catalog_without_type_or_warehouse() -> None:
    conf = spark_conf(SparkSettings(warehouse=None, catalog_type=None))
    assert "spark.sql.catalog.rqm.type" not in conf
    assert "spark.sql.catalog.rqm.warehouse" not in conf


def test_warehouse_uri(tmp_path: Path) -> None:
    assert warehouse_uri("s3a://bucket/prefix") == "s3a://bucket/prefix"
    assert warehouse_uri("file:///tmp/wh") == "file:///tmp/wh"
    assert warehouse_uri(str(tmp_path)) == tmp_path.resolve().as_uri()


def test_geometry_sql_helpers() -> None:
    assert (
        native_geom_sql("shape")
        == "spark_st_setsrid(spark_st_geomfromwkb(shape), 3857)"
    )
    assert sedona_geom_sql("g.geom") == (
        "ST_SetSRID(ST_GeomFromWKB(spark_st_asbinary(g.geom)), spark_st_srid(g.geom))"
    )


CREATE = "CREATE TABLE IF NOT EXISTS rqm.buildings (a INT) USING iceberg TBLPROPERTIES ('format-version' = '3')"


def test_layout_sql_partition_and_order() -> None:
    layout = TableLayout(
        partition_by=("truncate(2, cbfips)",), write_order=("cbfips", "x")
    )
    assert layout_sql(CREATE, layout) == [
        f"{CREATE}\nPARTITIONED BY (truncate(2, cbfips))",
        "ALTER TABLE rqm.buildings WRITE ORDERED BY cbfips, x",
    ]


def test_layout_sql_default_is_plain_create() -> None:
    assert layout_sql(CREATE, TableLayout()) == [CREATE]


def test_layouts_from_args_overrides_defaults() -> None:
    layouts = layouts_from_args(
        partitions=[("geometries", "bucket(16, building_id)"), ("buildings", "none")],
        write_orders=[("geometries", "building_id")],
    )
    assert layouts["geometries"] == TableLayout(
        partition_by=("bucket(16, building_id)",), write_order=("building_id",)
    )
    assert layouts["buildings"].partition_by == ()
    assert layouts["buildings"].write_order == DEFAULT_LAYOUTS["buildings"].write_order


def test_apply_schema_rejects_unknown_layout_table() -> None:
    with pytest.raises(ValueError, match="not in schema.sql"):
        apply_schema(None, "rqm", {"bulidings": TableLayout()})  # type: ignore[arg-type]
