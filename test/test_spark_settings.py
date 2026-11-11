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
    catalog_properties,
    layout_sql,
    layouts_from_args,
    local_master,
    memory_gib,
    native_geom_sql,
    sedona_geom_sql,
    spark_conf,
    table_name,
    validate_ref_name,
    warehouse_uri,
)


def test_local_defaults(tmp_path: Path) -> None:
    conf = spark_conf(SparkSettings(warehouse=tmp_path))
    assert conf["spark.master"] == "local[*]"
    assert conf["spark.sql.geospatial.enabled"] == "true"
    assert conf["spark.jars.packages"] == ",".join(SPARK_PACKAGES)
    assert conf["spark.sql.catalog.rqm"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.rqm.type"] == "jdbc"
    assert conf["spark.sql.catalog.rqm.warehouse"] == tmp_path.resolve().as_uri()
    # The default JDBC catalog keeps its SQLite database in the warehouse.
    assert (
        conf["spark.sql.catalog.rqm.uri"]
        == f"jdbc:sqlite:{tmp_path.resolve()}/catalog.db"
    )
    assert conf["spark.sql.catalog.rqm.jdbc.schema-version"] == "V1"
    # Sedona JARs share Spark's class loader so spatial joins can serialize JTS indexes.
    assert conf["spark.driver.extraClassPath"].split(":") == [
        str(
            Path.home()
            / ".ivy2.5.2/jars/org.apache.sedona_sedona-spark-shaded-4.1_2.13-1.9.1.jar"
        ),
        str(
            Path.home()
            / ".ivy2.5.2/jars/org.datasyslab_geotools-wrapper-1.9.1-33.5.jar"
        ),
    ]


def test_driver_classpath_follows_ivy_dir(tmp_path: Path) -> None:
    conf = spark_conf(
        SparkSettings(warehouse=tmp_path, conf={"spark.jars.ivy": str(tmp_path)})
    )
    assert all(
        p.startswith(f"{tmp_path}/jars/")
        for p in conf["spark.driver.extraClassPath"].split(":")
    )


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
    assert "spark.driver.extraClassPath" not in conf
    assert conf["spark.sql.catalog.lake.warehouse"] == "s3://bucket/rqm"
    assert conf["spark.sql.catalog.lake.uri"] == "https://catalog.example/api"
    assert conf["spark.sql.shuffle.partitions"] == "400"
    # Explicit `conf` wins over derived values.
    assert conf["spark.sql.catalog.lake.type"] == "glue"


def test_jdbc_catalog_properties(tmp_path: Path) -> None:
    file_uri = SparkSettings(warehouse=tmp_path.as_uri())
    assert catalog_properties(file_uri)["uri"] == f"jdbc:sqlite:{tmp_path}/catalog.db"
    postgres = SparkSettings(
        warehouse="s3://bucket/rqm",
        catalog_properties={"uri": "jdbc:postgresql://db/rqm", "jdbc.user": "rqm"},
    )
    assert catalog_properties(postgres) == {
        "uri": "jdbc:postgresql://db/rqm",
        "jdbc.user": "rqm",
        "jdbc.schema-version": "V1",
    }
    with pytest.raises(ValueError, match="needs catalog_properties"):
        catalog_properties(SparkSettings(warehouse="s3://bucket/rqm"))


def test_hadoop_catalog_still_supported(tmp_path: Path) -> None:
    conf = spark_conf(SparkSettings(warehouse=tmp_path, catalog_type="hadoop"))
    assert conf["spark.sql.catalog.rqm.type"] == "hadoop"
    assert "spark.sql.catalog.rqm.uri" not in conf


def test_table_name_namespace_and_branch() -> None:
    assert table_name("rqm", "buildings") == "rqm.inventory.buildings"
    assert table_name("lake", "geometries", "nsi") == "lake.nsi.geometries"
    assert table_name("rqm", "buildings", branch="main") == "rqm.inventory.buildings"
    assert (
        table_name("rqm", "buildings", branch="cal-2026.q3")
        == "rqm.inventory.buildings.`branch_cal-2026.q3`"
    )


@pytest.mark.parametrize("name", ["nsi-2024.1", "v2", "calibration_q3"])
def test_valid_ref_names(name: str) -> None:
    assert validate_ref_name(name) == name


@pytest.mark.parametrize(
    "name", ["", "-v1", "v 1", "v1'; DROP", "a`b", "branch_x", "x" * 101]
)
def test_invalid_ref_names(name: str) -> None:
    with pytest.raises(ValueError, match="invalid"):
        validate_ref_name(name)


def test_catalog_without_type_or_warehouse() -> None:
    conf = spark_conf(SparkSettings(warehouse=None, catalog_type=None))
    assert "spark.sql.catalog.rqm.type" not in conf
    assert "spark.sql.catalog.rqm.warehouse" not in conf


def test_warehouse_uri(tmp_path: Path) -> None:
    assert warehouse_uri("s3a://bucket/prefix") == "s3a://bucket/prefix"
    assert warehouse_uri("file:///tmp/wh") == "file:///tmp/wh"
    assert warehouse_uri(str(tmp_path)) == tmp_path.resolve().as_uri()


@pytest.mark.parametrize(
    ("size", "gib"),
    [("8g", 8), ("8GB", 8), ("512m", 0.5), ("2048", 2), ("1t", 1024), ("1048576k", 1)],
)
def test_memory_gib(size: str, gib: float) -> None:
    assert memory_gib(size) == gib


def test_memory_gib_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="invalid Spark memory size"):
        memory_gib("eight gigs")


def test_local_master_caps_tasks_by_memory_and_cores() -> None:
    # The nationwide NSI load failed at local[32] with 8g and finished at local[8].
    assert local_master("8g", cores=32) == "local[8]"
    assert local_master("64g", cores=4) == "local[4]"
    assert local_master("512m", cores=32) == "local[1]"
    assert local_master("8g", cores=32, task_memory_gib=2) == "local[4]"


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
