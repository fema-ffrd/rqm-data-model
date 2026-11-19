"""Ingest a National Structure Inventory (NSI) parquet into the NSI-minimal
RQM schema (Apache Iceberg v3 tables accessed via Apache Sedona on Spark 4.1).

Usage
-----
    python tools/ingest_nsi.py \
        --parquet test/data/nsi.parquet \
        --warehouse test/warehouse \
        --version nsi-sample-v1 --editor jdoe --reason "initial load"

Session, catalog, and table-layout options (`--master`, `--catalog-type`,
`--catalog-prop`, `--namespace`, `--conf`, `--partition`, `--write-order`)
are described in `--help`; the defaults run Spark locally against a JDBC
Iceberg catalog kept in `<warehouse>/catalog.db` (SQLite).

The script:

1. Starts a PySpark session with Sedona and Iceberg extensions
   (`SparkSettings` / `build_spark`).
2. Registers an Iceberg catalog at `--warehouse` (JDBC on SQLite by default;
   `--catalog-type hadoop` and REST/Glue catalogs also work).
3. Applies the DDL in `tools/schema.sql` (idempotent CREATE NAMESPACE / CREATE
   TABLE IF NOT EXISTS; tables live in the `inventory` namespace) with the
   partition spec and write order from `TableLayout`, and migrates existing
   tables by adding missing columns (`migrate_table`).
4. Reads the NSI parquet through the built-in source spec
   `tools/sources/nsi.yaml` (the generic engine in `tools/ingest_source.py`),
   sets the stable `building_id` to the NSI `fd_id`, registers the `NSI`
   source, and MERGEs it into `rqm.inventory.buildings` as a full replacement
   of the NSI rows: new structures are inserted, changed ones updated (fresh
   `ingested_at`), unchanged ones left alone, and missing ones deleted from
   the branch head. Rows of other sources (ADR-multi-source) are untouched.
5. Derives `rqm.inventory.geometries` rows from the same source the same way:
   one row per structure with `role='point'`, `purpose='computation'`,
   `is_selected_for_export=true`, `geom` stored as a native Iceberg v3
   GEOMETRY (EPSG:3857), and a stable `geometry_id` hashed from the row's
   natural key (`geometry_id_expr`).
6. With `--version`, commits the result as a named inventory version
   (Iceberg tags + `inventory_versions` registry row; see
   `tools/versioning.py` and docs/decisions/ADR-versioning.md). `--branch`
   loads into a project branch instead of main.

Logical invariants (ADR-nsi-minimal validation rules 1-7) are enforced by
inspection of the input + a post-write sanity check; the full assertion
suite lives in `test/test_nsi_ingest.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SCHEMA_SQL = HERE / "schema.sql"

# Order matches tools/schema.sql rqm.buildings.
BUILDINGS_COLUMNS: list[str] = [
    "building_id",
    "source_ref",
    "source_record_id",
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
    "ingested_at",
]

# All NSI parquet columns the schema maps (every column except `shape`).
NSI_SCALAR_FIELDS: list[str] = [
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
]

# Target types of the mappable `buildings` columns (the NSI field set shared
# by every source); every mapped value is cast to these so Iceberg DDL
# matches exactly.
BUILDINGS_CAST: dict[str, str] = {
    "fd_id": "bigint",
    "bid": "string",
    "usastrucid": "string",
    "ftprntid": "string",
    "ftprntsrc": "string",
    "source": "string",
    "occtype": "string",
    "st_damcat": "string",
    "bldgtype": "string",
    "num_story": "smallint",
    "sqft": "double",
    "ftprntsqft": "double",
    "bldheight": "float",
    "med_yr_blt": "smallint",
    "found_type": "string",
    "found_ht": "float",
    "val_struct": "double",
    "val_cont": "double",
    "val_vehic": "double",
    "ornl_low": "double",
    "ornl_med": "double",
    "ornl_hgh": "double",
    "resunits": "float",
    "novehprob": "float",
    "vehperunit": "float",
    "pop2amu65": "float",
    "pop2amo65": "float",
    "pop2pmu65": "float",
    "pop2pmo65": "float",
    "u65disable": "float",
    "o65disable": "float",
    "students": "float",
    "firmzone": "string",
    "zone_sub": "string",
    "static_bfe": "float",
    "ground_elv": "float",
    "grnd_elv_m": "float",
    "pctlowclr": "float",
    "fullrep": "float",
    "depindex": "float",
    "creprcnt": "float",
    "crerank": "float",
    "cbfips": "string",
    "x": "double",
    "y": "double",
}


# Maven coordinates resolved via spark.jars.packages. Scala 2.13 / Spark 4.1
# builds throughout. The Sedona JAR must match the pinned apache-sedona Python
# package.
#
# Sedona's spatial-index serializers live in JTS's own packages
# (`org.locationtech.jts.index.*.IndexSerde`) and call package-private JTS
# methods. Spark 4.1 ships jts-core on the JVM classpath, so JARs fetched via
# `spark.jars.packages` land in a different class loader and every Sedona
# spatial join (e.g. points in a watershed polygon) fails with
# IllegalAccessError. In local mode `spark_conf` therefore also puts these
# Ivy-cached JARs on `spark.driver.extraClassPath`, beside Spark's JTS.
SEDONA_CLASSPATH_PACKAGES: tuple[str, ...] = (
    "org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.1",
    "org.datasyslab:geotools-wrapper:1.9.1-33.5",
)
# Iceberg >= 1.12 is the first release that maps Iceberg v3 geometry columns
# to Spark 4.1's native GEOMETRY type (apache/iceberg#16851, #17073).
# sqlite-jdbc backs the default JDBC catalog (`catalog.db` in the warehouse).
SPARK_PACKAGES: tuple[str, ...] = (
    *SEDONA_CLASSPATH_PACKAGES,
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0",
    "org.xerial:sqlite-jdbc:3.50.3.0",
)

# Iceberg namespace holding every RQM table (`<catalog>.inventory.buildings`).
# REST catalogs reject tables at the catalog root, so tables never live there.
NAMESPACE = "inventory"

# File name of the SQLite database the default JDBC catalog keeps in a local
# warehouse directory.
JDBC_SQLITE_FILE = "catalog.db"

# Spark 4's default `spark.jars.ivy`.
DEFAULT_IVY_DIR = Path.home() / ".ivy2.5.2"

# EPSG code of `rqm.geometries.geom`. Iceberg v3 stores the CRS per column, and
# Spark 4.1 only maps SRID 0, 3857, and 4326 (OGC:CRS84) to Iceberg.
GEOM_SRID = 3857

# Spark 4.1 built-ins that Sedona shadows with same-named functions; they are
# re-registered with a `spark_` prefix by `register_spark_geo_functions`.
SPARK_GEO_FUNCTIONS: tuple[str, ...] = (
    "st_geomfromwkb",
    "st_asbinary",
    "st_setsrid",
    "st_srid",
)


@dataclass(frozen=True)
class SparkSettings:
    """Session + Iceberg catalog configuration consumed by `build_spark`.

    Defaults target local development: Spark runs in-process on every core
    and the Iceberg catalog is a JDBC catalog whose metadata pointers live in
    a SQLite file (`<warehouse>/catalog.db`). Each commit is a database
    transaction, unlike the Hadoop catalog's file renames, which are unsafe
    on object stores and for concurrent writers. Moving to a cluster is a
    settings change, not a code change:

    - `master=None` defers to spark-submit / the cluster manager.
    - `local[*]` suits small inputs only; for national-scale loads use
      `local_master(driver_memory)` so concurrent tasks fit in the driver heap.
    - `catalog_type` + `catalog_properties` select another Iceberg catalog:
      `"jdbc"` with `{"uri": "jdbc:postgresql://..."}` for a shared
      multi-user catalog (add the Postgres driver to `packages`), `"rest"`
      with `{"uri": ...}`, `"glue"` with
      `{"io-impl": "org.apache.iceberg.aws.s3.S3FileIO"}`, or `"hadoop"`
      (single writer, local file systems only); `warehouse` may then be an
      object-store URI such as `s3://bucket/rqm`.
    - `packages=()` skips Maven resolution when the runtime image already
      ships the JARs; `conf` carries any other Spark setting (driver memory,
      shuffle partitions, S3 credentials provider, ...) and wins over the
      values derived here.
    """

    warehouse: str | Path | None
    catalog: str = "rqm"
    catalog_type: str | None = "jdbc"
    catalog_properties: Mapping[str, str] = field(default_factory=dict)
    master: str | None = "local[*]"
    app_name: str = "rqm-nsi-ingest"
    packages: tuple[str, ...] = SPARK_PACKAGES
    conf: Mapping[str, str] = field(default_factory=dict)


_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")

# Driver heap each concurrent task needs when Spark runs locally. Every task
# holds Parquet read buffers, its share of the write-order sort, and an Iceberg
# Parquet writer that buffers a full row group (128 MB by default). On the
# nationwide NSI file, 32 tasks in an 8 GB driver ran out of memory (heap OOM,
# or ENOMEM from the kernel when reading over a 9p/WSL mount); 8 tasks finished.
LOCAL_TASK_MEMORY_GIB = 1.0

_MEMORY_RE = re.compile(r"(\d+)\s*([bkmgtp]?)b?", re.IGNORECASE)
_GIB_PER_UNIT = {
    "b": 2**-30,
    "k": 2**-20,
    "m": 2**-10,
    "g": 1.0,
    "t": 2.0**10,
    "p": 2.0**20,
}


def memory_gib(size: str) -> float:
    """Parse a Spark memory size (`8g`, `512m`, `2048`) into GiB.

    A bare number is MiB, as for `spark.driver.memory`.
    """
    match = _MEMORY_RE.fullmatch(size.strip())
    if match is None:
        raise ValueError(f"invalid Spark memory size: {size!r}")
    value, unit = match.groups()
    return int(value) * _GIB_PER_UNIT[unit.lower() or "m"]


def local_master(
    driver_memory: str,
    cores: int | None = None,
    task_memory_gib: float = LOCAL_TASK_MEMORY_GIB,
) -> str:
    """Return `local[N]` with N capped by both the CPU count and driver memory.

    In local mode every task runs inside the driver JVM, so `local[*]` on a
    many-core machine splits `driver_memory` too thinly for large loads.
    """
    cores = cores or os.cpu_count() or 1
    tasks = int(memory_gib(driver_memory) // task_memory_gib)
    return f"local[{max(1, min(cores, tasks))}]"


def warehouse_uri(warehouse: str | Path) -> str:
    """Return `warehouse` as a URI; local paths become absolute `file://` URIs."""
    text = str(warehouse)
    if _URI_RE.match(text):
        return text
    return Path(text).resolve().as_uri()


def local_warehouse_path(warehouse: str | Path | None) -> Path | None:
    """Return the local directory of a path or `file://` warehouse, else None."""
    if warehouse is None:
        return None
    text = str(warehouse)
    if text.startswith("file:"):
        return Path(urlparse(text).path)
    if _URI_RE.match(text):
        return None
    return Path(text).resolve()


def catalog_properties(settings: SparkSettings) -> dict[str, str]:
    """Iceberg catalog properties with JDBC defaults filled in.

    A JDBC catalog without a `uri` gets a SQLite database inside a local
    warehouse; object-store warehouses must name their catalog database.
    """
    props = dict(settings.catalog_properties)
    if settings.catalog_type != "jdbc":
        return props
    if "uri" not in props:
        local = local_warehouse_path(settings.warehouse)
        if local is None:
            raise ValueError(
                "a jdbc catalog on a non-local warehouse needs "
                "catalog_properties['uri'] (e.g. jdbc:postgresql://host/db)"
            )
        props["uri"] = f"jdbc:sqlite:{local / JDBC_SQLITE_FILE}"
    # V1 adds view support; V0 is only kept for catalogs created before it.
    props.setdefault("jdbc.schema-version", "V1")
    return props


def table_name(
    catalog: str, table: str, namespace: str = NAMESPACE, branch: str | None = None
) -> str:
    """Fully qualified table identifier, optionally at an Iceberg branch.

    `branch=None` or `"main"` is the table itself; any other branch maps to
    Iceberg's `<table>.branch_<name>` identifier for reads and writes.
    """
    name = f"{catalog}.{namespace}.{table}"
    if branch and branch != "main":
        # Quoted so names with '-' or '.' stay one identifier part.
        name = f"{name}.`branch_{branch}`"
    return name


def ivy_jar(coordinate: str, ivy_dir: str | Path = DEFAULT_IVY_DIR) -> Path:
    """Path where `spark.jars.packages` caches the JAR for a Maven coordinate."""
    group, artifact, version = coordinate.split(":")
    return Path(ivy_dir).expanduser() / "jars" / f"{group}_{artifact}-{version}.jar"


def driver_classpath_jars(settings: SparkSettings) -> list[Path]:
    """Ivy JARs to put on the driver classpath (see `SEDONA_CLASSPATH_PACKAGES`).

    Only applies to local masters, where the driver JVM runs every task; on a
    cluster, ship Sedona with the runtime image instead.
    """
    if not (settings.master or "").startswith("local"):
        return []
    ivy_dir = settings.conf.get("spark.jars.ivy", DEFAULT_IVY_DIR)
    return [
        ivy_jar(coordinate, ivy_dir)
        for coordinate in SEDONA_CLASSPATH_PACKAGES
        if coordinate in settings.packages
    ]


def spark_conf(settings: SparkSettings) -> dict[str, str]:
    """Translate `settings` into the Spark configuration `build_spark` applies."""
    cat = f"spark.sql.catalog.{settings.catalog}"
    conf: dict[str, str] = {
        "spark.app.name": settings.app_name,
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.apache.sedona.sql.SedonaSqlExtensions"
        ),
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
        # Spark 4.1 gates the GEOMETRY/GEOGRAPHY types (and so Iceberg v3
        # geometry columns) behind this flag.
        "spark.sql.geospatial.enabled": "true",
        cat: "org.apache.iceberg.spark.SparkCatalog",
    }
    if settings.master:
        conf["spark.master"] = settings.master
    if settings.packages:
        conf["spark.jars.packages"] = ",".join(settings.packages)
    if jars := driver_classpath_jars(settings):
        conf["spark.driver.extraClassPath"] = os.pathsep.join(map(str, jars))
    if settings.catalog_type:
        conf[f"{cat}.type"] = settings.catalog_type
    if settings.warehouse is not None:
        conf[f"{cat}.warehouse"] = warehouse_uri(settings.warehouse)
    conf.update(
        {f"{cat}.{key}": value for key, value in catalog_properties(settings).items()}
    )
    conf.update(settings.conf)
    return conf


def build_spark(settings: SparkSettings) -> SparkSession:
    """Start (or reuse) a Spark session with Sedona + Iceberg configured.

    The Spark / Sedona / Iceberg packages are resolved from Maven Central at
    first run; subsequent runs reuse the Ivy cache. The driver classpath is
    fixed when the JVM starts, so on the very first run (empty Ivy cache)
    Sedona spatial joins only work after restarting the Python process.
    """
    conf = spark_conf(settings)
    if settings.catalog_type == "jdbc":
        # SQLite creates the database file but not its directory.
        local = local_warehouse_path(settings.warehouse)
        if local is not None:
            local.mkdir(parents=True, exist_ok=True)
    uncached = [
        jar
        for jar in driver_classpath_jars(settings)
        if not jar.exists() and "spark.driver.extraClassPath" not in settings.conf
    ]
    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    if uncached:
        print(
            "warning: Sedona JARs were not in the Ivy cache at JVM start "
            f"({', '.join(j.name for j in uncached)}); restart Python before "
            "running spatial joins.",
            file=sys.stderr,
        )

    # Register Sedona SQL functions (ST_*).
    from sedona.spark import SedonaContext  # imported lazily; package is heavy

    SedonaContext.create(spark)
    register_spark_geo_functions(spark)
    return spark


def register_spark_geo_functions(spark: SparkSession) -> None:
    """Expose Spark's built-in geometry functions as `spark_st_*`.

    Sedona registers `ST_GeomFromWKB`, `ST_AsBinary`, `ST_SetSRID`, and
    `ST_SRID` under the same names as Spark 4.1's built-ins, shadowing them,
    and Sedona functions only accept Sedona's geometry UDT, not the native
    GEOMETRY type that Iceberg v3 geometry columns map to. The aliases bridge
    the two (see `native_geom_sql` / `sedona_geom_sql`).

    Uses the JVM function registry, so it needs a classic (non-Connect)
    PySpark session.
    """
    jvm = spark._jvm
    # `FunctionRegistry.expressions` is Spark's immutable built-in table;
    # `FunctionRegistry.builtin` is mutated by Sedona and would return its
    # overrides.
    expressions = (
        jvm.org.apache.spark.sql.catalyst.analysis.FunctionRegistry.expressions()
    )
    identifier = jvm.org.apache.spark.sql.catalyst.FunctionIdentifier
    registry = spark._jsparkSession.sessionState().functionRegistry()
    for name in SPARK_GEO_FUNCTIONS:
        entry = expressions.get(name)
        if entry.isEmpty():
            raise RuntimeError(
                f"Spark built-in {name!r} not found; Spark 4.1+ with geospatial support is required"
            )
        info_and_builder = entry.get()
        registry.registerFunction(
            identifier.apply(f"spark_{name}"),
            info_and_builder._1(),
            info_and_builder._2(),
        )


def native_geom_sql(wkb_expr: str, srid: int = GEOM_SRID) -> str:
    """SQL turning a WKB BINARY expression into a native GEOMETRY(srid)."""
    return f"spark_st_setsrid(spark_st_geomfromwkb({wkb_expr}), {srid})"


def sedona_geom_sql(geom_expr: str = "geom") -> str:
    """SQL turning a native GEOMETRY expression into a Sedona geometry (SRID kept)."""
    return f"ST_SetSRID(ST_GeomFromWKB(spark_st_asbinary({geom_expr})), spark_st_srid({geom_expr}))"


# Natural key of a `geometries` row: one row per (source, building,
# representation, alternate). `is_selected_for_export` and `ingested_at` are
# excluded so a selection migration or a re-ingest keeps the id.
GEOMETRY_KEY_COLUMNS: tuple[str, ...] = (
    "source_ref",
    "building_id",
    "role",
    "purpose",
    "alt_group_key",
)

# xxhash64 skips NULL inputs, so (NULL, 'a') and ('a', NULL) would hash alike;
# NULLs are replaced with a sentinel that cannot occur in the key columns.
_NULL_KEY_SENTINEL = "\u0000"


def geometry_id_expr() -> Column:
    """Stable `geometry_id`: xxhash64 of `GEOMETRY_KEY_COLUMNS`.

    Unlike row numbering (`monotonically_increasing_id`), the hash depends only
    on the row's natural key, so the same geometry gets the same id on every
    ingest regardless of Spark partitioning. Changing the key columns, their
    order, or the sentinel changes every id.
    """
    return F.xxhash64(
        *[
            F.coalesce(F.col(c).cast("string"), F.lit(_NULL_KEY_SENTINEL))
            for c in GEOMETRY_KEY_COLUMNS
        ]
    )


def assert_no_geometry_id_collisions(
    new_rows: DataFrame, existing_rows: DataFrame
) -> None:
    """Raise if one `geometry_id` would map to more than one natural key.

    A 64-bit hash has a small but non-zero collision chance at national scale,
    and Iceberg does not enforce primary keys. Re-ingesting identical keys is
    not a collision (same key, same id).
    """
    cols = ["geometry_id", *GEOMETRY_KEY_COLUMNS]
    collisions = (
        new_rows.select(*cols)
        .unionByName(existing_rows.select(*cols))
        .distinct()
        .groupBy("geometry_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if collisions:
        ids = [row["geometry_id"] for row in collisions]
        raise ValueError(
            f"geometry_id hash collision between different natural keys: {ids}"
        )


# `source_ref` of the National Structure Inventory. NSI rows keep
# `building_id = fd_id`; every other source gets hashed ids
# (`source_building_id_expr`).
NSI_SOURCE_REF = "NSI"

# Columns added to `buildings` by the multi-source migration. Rows written
# before it read back NULL (Iceberg cannot add a required column and Spark
# 4.1 has no column defaults) and tags taken before it lack the columns, so
# readers fill them in with these SQL expressions (`conform_buildings`).
# `{p}` is the table alias prefix (`"t."` or `""`).
CONFORMED_BUILDING_COLUMNS: dict[str, str] = {
    "source_ref": f"coalesce({{p}}source_ref, '{NSI_SOURCE_REF}')",
    "source_record_id": "coalesce({p}source_record_id, CAST({p}fd_id AS STRING))",
}
_CONFORMED_MISSING: dict[str, str] = {
    "source_ref": f"'{NSI_SOURCE_REF}'",
    "source_record_id": "CAST({p}fd_id AS STRING)",
}


def conformed_sql(column: str, alias: str | None = None, present: bool = True) -> str:
    """SQL reading `column` of a `buildings` row, with migrated NULLs filled in.

    `alias` qualifies the column (`t` -> `t.source_ref`); `present=False`
    gives the value for a table version that predates the column.
    """
    p = f"{alias}." if alias else ""
    if column in CONFORMED_BUILDING_COLUMNS:
        template = (CONFORMED_BUILDING_COLUMNS if present else _CONFORMED_MISSING)[
            column
        ]
        return template.format(p=p)
    return f"{p}{column}" if present else "NULL"


def conform_buildings(df: DataFrame) -> DataFrame:
    """`buildings` rows in the current column order, with migrated columns filled.

    Rows and tags written before the multi-source migration have NULL (or no)
    `source_ref` / `source_record_id`; they are NSI rows, so they read as
    `NSI` / the `fd_id` string, and diffs across the migration show no
    spurious changes.
    """
    present = set(df.columns)
    exprs = []
    for column in BUILDINGS_COLUMNS:
        sql = conformed_sql(column, present=column in present)
        if sql == column:
            exprs.append(F.col(column))
            continue
        dtype = BUILDINGS_CAST.get(column, "string")
        exprs.append(F.expr(sql).cast(dtype).alias(column))
    return df.select(*exprs)


def source_building_id_sql(source_ref_sql: str, source_record_id_sql: str) -> str:
    """SQL for the `building_id` of a non-NSI row (see `source_building_id_expr`)."""
    return (
        f"(xxhash64({source_ref_sql}, {source_record_id_sql}) "
        "| shiftleft(CAST(1 AS BIGINT), 63))"
    )


def source_building_id_expr(source_ref: Column, source_record_id: Column) -> Column:
    """`building_id` of a non-NSI row: xxhash64(source_ref, source_record_id)
    with the sign bit set.

    The id is always negative, so it never collides with NSI's positive
    `fd_id`s, and it depends only on the source lineage and the source's
    native id, so it is stable across reloads and releases. Changing it
    re-keys every non-NSI building.
    """
    sign_bit = F.shiftleft(F.lit(1).cast("bigint"), 63)
    return F.xxhash64(source_ref, source_record_id).bitwiseOR(sign_bit)


def assert_no_building_id_collisions(
    new_rows: DataFrame, existing_rows: DataFrame
) -> None:
    """Raise if one hashed `building_id` maps to more than one
    (source_ref, source_record_id) pair. Mirrors
    `assert_no_geometry_id_collisions`.
    """
    cols = ["building_id", "source_ref", "source_record_id"]
    collisions = (
        new_rows.select(*cols)
        .unionByName(existing_rows.select(*cols))
        .distinct()
        .groupBy("building_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
        .collect()
    )
    if collisions:
        ids = [row["building_id"] for row in collisions]
        raise ValueError(
            f"building_id hash collision between different source records: {ids}"
        )


@dataclass(frozen=True)
class TableLayout:
    """Physical layout applied when `apply_schema` creates a table.

    `partition_by` holds Iceberg partition transforms (`truncate(2, cbfips)`,
    `bucket(16, building_id)`, `days(ingested_at)`, ...); `write_order` holds
    the columns of the table's `WRITE ORDERED BY` sort order.
    """

    partition_by: tuple[str, ...] = ()
    write_order: tuple[str, ...] = ()


DEFAULT_LAYOUTS: Mapping[str, TableLayout] = {
    # State partitions (first two cbfips digits; ~56 nationally), so filters
    # on cbfips prune whole states. Ordering by the hierarchical block GEOID
    # keeps nearby structures in the same files, which lets x/y min/max file
    # statistics skip files for bounding-box queries.
    "buildings": TableLayout(
        partition_by=("truncate(2, cbfips)",), write_order=("cbfips",)
    ),
    "geometries": TableLayout(),
}


def split_sql_statements(sql_text: str) -> list[str]:
    """Split a SQL script into statements, dropping `--` and `/* */` comments.

    Only `;` outside of comments, string literals, and quoted identifiers ends
    a statement, so semicolons in comment prose or `COMMENT '...'` text are
    preserved.
    """
    statements: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql_text)
    while i < n:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            end = sql_text.find("\n", i)
            i = n if end == -1 else end
        elif ch == "/" and nxt == "*":
            end = sql_text.find("*/", i + 2)
            i = n if end == -1 else end + 2
            buf.append(" ")
        elif ch in ("'", '"', "`"):
            j = i + 1
            while j < n:
                if sql_text[j] == "\\" and ch != "`":
                    j += 2
                    continue
                if sql_text[j] == ch:
                    if j + 1 < n and sql_text[j + 1] == ch:  # doubled-quote escape
                        j += 2
                        continue
                    break
                j += 1
            buf.append(sql_text[i : j + 1])
            i = j + 1
        elif ch == ";":
            statements.append("".join(buf).strip())
            buf = []
            i += 1
        else:
            buf.append(ch)
            i += 1
    statements.append("".join(buf).strip())
    return [s for s in statements if s]


_CREATE_TABLE_RE = re.compile(
    r"^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([\w.]+)", re.IGNORECASE
)


def created_table(statement: str) -> str | None:
    """Return the table a `CREATE TABLE IF NOT EXISTS` statement creates, else None."""
    match = _CREATE_TABLE_RE.match(statement)
    return match.group(1) if match else None


@dataclass(frozen=True)
class ColumnDef:
    """One column of a `CREATE TABLE` statement (see `table_columns`)."""

    name: str
    type_sql: str
    not_null: bool
    comment_sql: str | None  # the quoted literal, e.g. "'Stable PK.'"


_COLUMN_DEF_RE = re.compile(
    r"^(\w+)\s+(.+?)(\s+NOT\s+NULL)?(?:\s+COMMENT\s+('(?:[^'\\]|\\.|'')*'))?$",
    re.IGNORECASE | re.DOTALL,
)


def _split_top_level(text: str, sep: str = ",") -> list[str]:
    """Split on `sep` outside quotes, (), and <>."""
    parts, buf, depth, quote = [], [], 0, ""
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == "\\" and quote != "`" and i + 1 < len(text):
                buf.append(text[i + 1])
                i += 1
            elif ch == quote:
                quote = ""
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif ch in "(<":
            depth += 1
            buf.append(ch)
        elif ch in ")>":
            depth -= 1
            buf.append(ch)
        elif ch == sep and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf).strip())
    return [p for p in parts if p]


def table_columns(create_statement: str) -> list[ColumnDef]:
    """Columns declared by a `CREATE TABLE IF NOT EXISTS` statement, in order."""
    match = _CREATE_TABLE_RE.match(create_statement)
    if match is None:
        raise ValueError("table_columns expects a CREATE TABLE IF NOT EXISTS statement")
    start = create_statement.index("(", match.end())
    depth, quote, end = 0, "", None
    for i in range(start, len(create_statement)):
        ch = create_statement[i]
        if quote:
            if ch == quote:
                quote = ""
        elif ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        raise ValueError("unbalanced parentheses in CREATE TABLE statement")
    columns = []
    for part in _split_top_level(create_statement[start + 1 : end]):
        col = _COLUMN_DEF_RE.match(part)
        if col is None:
            raise ValueError(f"cannot parse column definition: {part!r}")
        name, type_sql, not_null, comment = col.groups()
        columns.append(ColumnDef(name, type_sql.strip(), bool(not_null), comment))
    return columns


def migrate_table(spark: SparkSession, create_statement: str) -> list[str]:
    """Add the columns of `create_statement` that the existing table lacks.

    Each missing column is added in its DDL position (`AFTER` its
    predecessor) with its comment. Iceberg cannot add a required column to a
    populated table and Spark 4.1 does not support column defaults, so
    added columns are nullable and existing rows read NULL (see
    `conform_buildings`). Columns are never dropped or retyped. Returns the
    added column names.
    """
    table = created_table(create_statement)
    if table is None:
        raise ValueError("migrate_table expects a CREATE TABLE IF NOT EXISTS statement")
    existing = {c.lower() for c in spark.table(table).columns}
    added: list[str] = []
    previous: str | None = None
    for col in table_columns(create_statement):
        if col.name.lower() not in existing:
            position = f"AFTER {previous}" if previous else "FIRST"
            comment = f" COMMENT {col.comment_sql}" if col.comment_sql else ""
            spark.sql(
                f"ALTER TABLE {table} ADD COLUMN {col.name} {col.type_sql}"
                f"{comment} {position}"
            )
            added.append(col.name)
        previous = col.name
    return added


def layout_sql(create_statement: str, layout: TableLayout) -> list[str]:
    """Return the statements that create a table with `layout` applied."""
    table = created_table(create_statement)
    if table is None:
        raise ValueError("layout_sql expects a CREATE TABLE IF NOT EXISTS statement")
    statements = [create_statement]
    if layout.partition_by:
        # Spark accepts table clauses in any order, so PARTITIONED BY can
        # follow the TBLPROPERTIES clause in schema.sql.
        statements[0] = (
            f"{create_statement}\nPARTITIONED BY ({', '.join(layout.partition_by)})"
        )
    if layout.write_order:
        statements.append(
            f"ALTER TABLE {table} WRITE ORDERED BY {', '.join(layout.write_order)}"
        )
    return statements


def apply_schema(
    spark: SparkSession,
    catalog: str,
    layouts: Mapping[str, TableLayout] = DEFAULT_LAYOUTS,
    namespace: str = NAMESPACE,
) -> dict[str, list[str]]:
    """Apply the CREATE NAMESPACE / CREATE TABLE statements from schema.sql.

    `layouts` maps unqualified table names to the partition spec and write
    order used when the table is created; tables without an entry are
    unpartitioned and unsorted. Layouts are only applied at creation —
    existing tables are left untouched, so changing the layout of a populated
    table is an explicit Iceberg partition evolution (`ALTER TABLE ... ADD /
    DROP PARTITION FIELD`, `WRITE ORDERED BY`).

    Existing tables are migrated idempotently: columns the DDL declares but
    the table lacks are added (`migrate_table`). When `buildings` gains
    `source_ref`, its existing rows are NSI rows, so the built-in NSI source
    is registered in `sources` for them to resolve. Returns the added
    columns per migrated table.

    Spark SQL accepts one statement per `spark.sql(...)` call, so the script
    is split with `split_sql_statements`.
    """
    sql_text = SCHEMA_SQL.read_text(encoding="utf-8")
    # Qualify identifiers with the configured catalog and namespace.
    sql_text = re.sub(r"\brqm\.inventory\b", f"{catalog}.{namespace}", sql_text)
    statements = split_sql_statements(sql_text)

    schema_tables = {t.split(".")[-1] for t in map(created_table, statements) if t}
    unknown = sorted(set(layouts) - schema_tables)
    if unknown:
        raise ValueError(f"layouts given for tables not in schema.sql: {unknown}")

    migrated: dict[str, list[str]] = {}
    for statement in statements:
        table = created_table(statement)
        if table is None:
            spark.sql(statement)
        elif not spark.catalog.tableExists(table):
            layout = layouts.get(table.split(".")[-1], TableLayout())
            for sql in layout_sql(statement, layout):
                spark.sql(sql)
        elif added := migrate_table(spark, statement):
            migrated[table.split(".")[-1]] = added

    if "source_ref" in migrated.get("buildings", []):
        from ingest_source import register_source
        from sources import nsi_spec

        register_source(spark, nsi_spec(), catalog, namespace)
    return migrated


# Branch and version (tag) names are interpolated into SQL identifiers and
# literals, so they are restricted to a safe alphabet.
REF_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")


def validate_ref_name(name: str, what: str = "ref") -> str:
    """Return `name` if it is a valid branch / version name, else raise."""
    if not REF_NAME_RE.match(name or "") or name.startswith("branch_"):
        raise ValueError(
            f"invalid {what} name {name!r}: use letters, digits, '.', '_', '-' "
            "(not starting with 'branch_')"
        )
    return name


def ref_snapshot_id(spark: SparkSession, table: str, ref: str) -> int | None:
    """Snapshot id an Iceberg branch or tag of `table` points at, or None."""
    if ref != "main":
        validate_ref_name(ref)
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.refs WHERE name = '{ref}'"
    ).collect()
    return rows[0]["snapshot_id"] if rows else None


def require_branch(
    spark: SparkSession, catalog: str, branch: str | None, namespace: str = NAMESPACE
) -> None:
    """Raise unless `branch` exists on buildings (writes would create it)."""
    if not branch or branch == "main":
        return
    validate_ref_name(branch, "branch")
    if (
        ref_snapshot_id(spark, table_name(catalog, "buildings", namespace), branch)
        is None
    ):
        raise ValueError(
            f"branch {branch!r} does not exist; create it from a version first "
            "(versioning.create_branch / rqm_version.py branch create)"
        )


# Geometries derived by the NSI load carry this source_ref prefix
# (`NSI-<source>`, e.g. NSI-P); a reload only deletes stale geometries in its
# own source's scope, so footprints from other sources survive.
NSI_SOURCE_PREFIX = f"{NSI_SOURCE_REF}-"

# Columns that decide whether a re-loaded building changed. `ingested_at`
# records when a row was last written, so it is not compared.
BUILDING_COMPARE_COLUMNS: list[str] = [
    c for c in BUILDINGS_COLUMNS if c not in ("building_id", "ingested_at")
]


def changed_sql(columns: list[str], left: str = "t", right: str = "s") -> str:
    """SQL predicate: any of `columns` differs between `left` and `right` (NULL-safe)."""
    return " OR ".join(f"NOT ({left}.{c} <=> {right}.{c})" for c in columns)


def assert_unique_key(df: DataFrame, key: str, what: str) -> None:
    """Raise if `key` repeats in `df` (MERGE needs one source row per key)."""
    dupes = df.groupBy(key).count().filter(F.col("count") > 1).limit(5).collect()
    if dupes:
        raise ValueError(f"{what}: duplicate {key} values {[r[key] for r in dupes]}")


def load_buildings(
    spark: SparkSession,
    parquet_path: Path,
    catalog: str,
    namespace: str = NAMESPACE,
    branch: str | None = None,
) -> int:
    """Upsert an NSI parquet into `buildings` as a full replacement of NSI.

    The parquet is the complete NSI inventory for `branch` (default main):
    rows match on `building_id` (= NSI `fd_id`); new ids are inserted, rows
    whose values changed are updated with a fresh `ingested_at`, unchanged
    rows are left alone, and NSI buildings missing from the parquet are
    deleted from the branch head. Buildings of other sources are never
    touched. Tagged versions keep the deleted rows. Also registers the NSI
    source in `sources`.

    Delegates to the generic engine (`ingest_source`) with the built-in spec
    `tools/sources/nsi.yaml`. Returns the NSI row count at the branch head.
    """
    from ingest_source import merge_buildings, register_source
    from sources import nsi_spec

    spec = nsi_spec()
    require_branch(spark, catalog, branch, namespace)
    register_source(spark, spec, catalog, namespace)
    return merge_buildings(spark, spec, parquet_path, catalog, namespace, branch)


def load_geometries(
    spark: SparkSession,
    parquet_path: Path,
    catalog: str,
    namespace: str = NAMESPACE,
    branch: str | None = None,
) -> int:
    """Upsert the NSI 'point' geometry of every NSI building on `branch`.

    One row per NSI structure, `building_id` = `fd_id`, kept only for
    buildings present on the branch (logical FK). `geometry_id` is the stable
    hash from `geometry_id_expr()`; the write is refused if it collides with
    a different natural key. Existing rows are updated only when the point or
    CRS changed, and `is_selected_for_export` is never overwritten, so a
    selection migration survives a reload. NSI points of NSI buildings that
    are missing from the parquet are deleted, and so are geometries from any
    source whose building is gone. `geom` is a native GEOMETRY(3857); query
    it with Sedona via `sedona_geom_sql()`
    (`ST_GeomFromWKB(spark_st_asbinary(geom))`).

    Run after `load_buildings` for the same parquet and branch. Delegates to
    `ingest_source.merge_geometries` with the built-in NSI spec. Returns the
    count of NSI-derived geometries at the branch head.
    """
    from ingest_source import merge_geometries
    from sources import nsi_spec

    require_branch(spark, catalog, branch, namespace)
    return merge_geometries(spark, nsi_spec(), parquet_path, catalog, namespace, branch)


def geoparquet_covering_columns(schema: pa.Schema) -> set[str]:
    """Return the columns a GeoParquet 1.1 file declares as geometry coverings.

    Writers such as GDAL add a bbox struct per geometry column (`shape_bbox`)
    and record it under `geo.columns.<geom>.covering`. A covering is derived
    from the geometry to speed up spatial filters; it is not a source field.
    """
    raw = (schema.metadata or {}).get(b"geo")
    if not raw:
        return set()
    columns: set[str] = set()
    for column_meta in json.loads(raw).get("columns", {}).values():
        # e.g. {"bbox": {"xmin": ["shape_bbox", "xmin"], ...}}
        for covering in column_meta.get("covering", {}).values():
            columns.update(path[0] for path in covering.values())
    return columns


def diagnostic_coverage_check(parquet_path: Path) -> list[str]:
    """Return a list of NSI columns that the schema does not map.

    Guards against NSI release drift silently dropping fields: any column
    present in the parquet but absent from NSI_SCALAR_FIELDS + {'shape'} is
    reported here. GeoParquet covering columns (see
    `geoparquet_covering_columns`) are derived from `shape`, so they are not
    reported.
    """
    schema = pq.ParquetFile(str(parquet_path)).schema_arrow
    parquet_cols = {f.name for f in schema}
    known = set(NSI_SCALAR_FIELDS) | {"shape"} | geoparquet_covering_columns(schema)
    return sorted(parquet_cols - known)


def _key_value(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep or not key.strip():
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got {text!r}")
    return key.strip(), value.strip()


def layouts_from_args(
    partitions: list[tuple[str, str]],
    write_orders: list[tuple[str, str]],
    base: Mapping[str, TableLayout] = DEFAULT_LAYOUTS,
) -> dict[str, TableLayout]:
    """Apply `--partition` / `--write-order` overrides on top of `base`.

    Values are the comma-separated clause body (`"bucket(16, building_id)"`,
    `"cbfips, x"`); an empty value or `none` clears the default.
    """
    layouts = dict(base)

    def _parts(value: str) -> tuple[str, ...]:
        return () if value.lower() in ("", "none") else (value,)

    for table, value in partitions:
        layouts[table] = replace(
            layouts.get(table, TableLayout()), partition_by=_parts(value)
        )
    for table, value in write_orders:
        layouts[table] = replace(
            layouts.get(table, TableLayout()), write_order=_parts(value)
        )
    return layouts


def add_session_arguments(ap: argparse.ArgumentParser) -> None:
    """Add the Spark session / Iceberg catalog options shared by the CLIs."""
    ap.add_argument(
        "--warehouse",
        default=str(REPO / "test" / "warehouse"),
        help="Iceberg warehouse: a local directory (created if missing) or a URI "
        "such as s3://bucket/prefix.",
    )
    ap.add_argument(
        "--catalog", default="rqm", help="Iceberg catalog name (default: rqm)."
    )
    ap.add_argument(
        "--namespace",
        default=NAMESPACE,
        help=f"Iceberg namespace holding the tables (default: {NAMESPACE}).",
    )
    ap.add_argument(
        "--catalog-type",
        default="jdbc",
        help="Iceberg catalog type: jdbc (default; SQLite file <warehouse>/"
        f"{JDBC_SQLITE_FILE} unless --catalog-prop uri=... is given), hadoop, "
        "rest, glue, hive, ...",
    )
    ap.add_argument(
        "--catalog-prop",
        type=_key_value,
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra Iceberg catalog property (spark.sql.catalog.<catalog>.KEY); repeatable.",
    )
    ap.add_argument(
        "--master",
        default=None,
        help="Spark master (default: local[N], N = min(cores, driver memory in GiB) "
        "so concurrent tasks fit in the driver heap); pass '' to defer to spark-submit.",
    )
    ap.add_argument(
        "--conf",
        type=_key_value,
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra Spark config, e.g. spark.driver.memory=8g; repeatable.",
    )


def settings_from_args(args: argparse.Namespace) -> SparkSettings:
    """Build `SparkSettings` from the options of `add_session_arguments`."""
    if not _URI_RE.match(args.warehouse):
        Path(args.warehouse).mkdir(parents=True, exist_ok=True)
    conf = dict(args.conf)
    master = args.master
    if master is None:
        # Spark's default driver heap is 1g.
        master = local_master(conf.get("spark.driver.memory", "1g"))
    return SparkSettings(
        warehouse=args.warehouse,
        catalog=args.catalog,
        catalog_type=args.catalog_type or None,
        catalog_properties=dict(args.catalog_prop),
        master=master or None,
        conf=conf,
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--parquet", type=Path, required=True, help="Path to the NSI parquet file."
    )
    add_session_arguments(ap)
    ap.add_argument(
        "--partition",
        type=_key_value,
        action="append",
        default=[],
        metavar="TABLE=SPEC",
        help="Override a table's partition spec at creation, e.g. "
        "'geometries=bucket(16, building_id)' or 'buildings=none'; repeatable.",
    )
    ap.add_argument(
        "--write-order",
        type=_key_value,
        action="append",
        default=[],
        metavar="TABLE=COLUMNS",
        help="Override a table's write sort order at creation, e.g. "
        "'buildings=cbfips, x'; repeatable.",
    )
    versioning = ap.add_argument_group(
        "versioning", "Record the load as a named inventory version (Iceberg tag)."
    )
    versioning.add_argument(
        "--version",
        help="Version name to commit after the load, e.g. nsi-2024.1. Without it "
        "the tables are updated but no version is recorded.",
    )
    versioning.add_argument(
        "--branch",
        default="main",
        help="Branch to load into (default: main). Project branches are created "
        "with `rqm_version.py branch create`.",
    )
    versioning.add_argument(
        "--editor", default=None, help="Who is committing the version (default: $USER)."
    )
    versioning.add_argument(
        "--reason", default=None, help="Why the version is committed."
    )
    versioning.add_argument(
        "--parent",
        default=None,
        help="Parent version (default: the nearest version in the branch history).",
    )
    versioning.add_argument(
        "--kind",
        default="release",
        choices=["release", "edit"],
        help="release: a new source dataset (default); edit: a revised copy.",
    )
    args = ap.parse_args()

    if not args.parquet.exists():
        print(f"error: parquet not found: {args.parquet}", file=sys.stderr)
        return 2
    if args.version:
        validate_ref_name(args.version, "version")
    if args.branch != "main":
        validate_ref_name(args.branch, "branch")

    unmapped = diagnostic_coverage_check(args.parquet)
    if unmapped:
        print(
            "warning: NSI columns present in parquet but not mapped by this "
            f"schema (release drift?): {unmapped}",
            file=sys.stderr,
        )

    settings = settings_from_args(args)
    layouts = layouts_from_args(args.partition, args.write_order)
    cat, ns, branch = args.catalog, args.namespace, args.branch

    spark = build_spark(settings)
    try:
        apply_schema(spark, cat, layouts, ns)
        if args.version:
            from versioning import ensure_version_available

            # Fail before touching the tables rather than after the load.
            ensure_version_available(spark, cat, args.version, ns)
        n_buildings = load_buildings(spark, args.parquet, cat, ns, branch)
        n_geoms = load_geometries(spark, args.parquet, cat, ns, branch)
        parquet_rows = pq.ParquetFile(str(args.parquet)).metadata.num_rows
        print(
            f"OK: loaded {n_buildings} buildings, {n_geoms} geometries "
            f"from {parquet_rows} parquet rows into branch {branch!r} "
            f"of the Iceberg warehouse at {args.warehouse}."
        )
        # Both counts cover NSI rows only, so other loaded sources don't count.
        if n_buildings != parquet_rows or n_geoms != parquet_rows:
            print("warning: row-count mismatch; smoke tests will fail", file=sys.stderr)
            return 1
        if args.version:
            from versioning import commit_version, file_sha256

            info = commit_version(
                spark,
                cat,
                args.version,
                editor=args.editor or default_editor(),
                reason=args.reason,
                branch=branch,
                kind=args.kind,
                parent=args.parent,
                source_ref=NSI_SOURCE_REF,
                source_uri=str(args.parquet.resolve()),
                source_sha256=file_sha256(args.parquet),
                namespace=ns,
            )
            print(
                f"OK: committed version {info['version_name']!r} on {branch!r} "
                f"(parent {info['parent_version']!r}): +{info['n_added']} added, "
                f"-{info['n_removed']} removed, ~{info['n_changed']} changed."
            )
        else:
            print("note: no --version given; this load is not recorded as a version.")
    finally:
        spark.stop()
    return 0


def default_editor() -> str:
    """Editor recorded when none is given: the OS user name."""
    import getpass

    try:
        return getpass.getuser()
    except (KeyError, OSError):  # no user database entry (some containers)
        return "unknown"


if __name__ == "__main__":
    sys.exit(main())
