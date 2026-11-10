"""Ingest a National Structure Inventory (NSI) parquet into the NSI-minimal
RQM schema (Apache Iceberg v3 tables accessed via Apache Sedona on Spark 4.1).

Usage
-----
    python tools/ingest_nsi.py \
        --parquet test/data/nsi.parquet \
        --warehouse test/warehouse \
        --catalog rqm

Session, catalog, and table-layout options (`--master`, `--catalog-type`,
`--catalog-prop`, `--conf`, `--partition`, `--write-order`) are described in
`--help`; the defaults run Spark locally against a file-system Iceberg catalog.

The script:

1. Starts a PySpark session with Sedona and Iceberg extensions
   (`SparkSettings` / `build_spark`).
2. Registers an Iceberg catalog at `--warehouse` (Hadoop catalog by default).
3. Applies the DDL in `tools/schema.sql` (idempotent CREATE TABLE IF NOT EXISTS)
   with the partition spec and write order from `TableLayout`.
4. Reads the NSI parquet, projects to the `buildings` column order, sets the
   stable `building_id` to the NSI `fd_id`, adds an `ingested_at` timestamp,
   and appends to `rqm.buildings`.
5. Derives `rqm.geometries` rows from the same source: one row per structure
   with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`,
   `geom` stored as a native Iceberg v3 GEOMETRY (EPSG:3857), and a stable
   `geometry_id` hashed from the row's natural key (`geometry_id_expr`).

Logical invariants (ADR-nsi-minimal validation rules 1-7) are enforced by
inspection of the input + a post-write sanity check; the full assertion
suite lives in `test/test_nsi_ingest.py`.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SCHEMA_SQL = HERE / "schema.sql"

# Order matches tools/schema.sql rqm.buildings.
BUILDINGS_COLUMNS: list[str] = [
    "building_id",
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

# Target types for the write; cast to these from the raw NSI parquet types
# so Iceberg DDL matches exactly.
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
# builds throughout. Iceberg >= 1.12 is the first release that maps Iceberg v3
# geometry columns to Spark 4.1's native GEOMETRY type (apache/iceberg#16851,
# #17073); the Sedona JAR must match the pinned apache-sedona Python package.
SPARK_PACKAGES: tuple[str, ...] = (
    "org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.1",
    "org.datasyslab:geotools-wrapper:1.9.1-33.5",
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0",
)

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
    and the Iceberg catalog is a Hadoop (file-system) catalog rooted at
    `warehouse`. Moving to a cluster is a settings change, not a code change:

    - `master=None` defers to spark-submit / the cluster manager.
    - `catalog_type` + `catalog_properties` select another Iceberg catalog
      (e.g. `"rest"` with `{"uri": ...}`, or `"glue"` with
      `{"io-impl": "org.apache.iceberg.aws.s3.S3FileIO"}`); `warehouse` may
      then be an object-store URI such as `s3://bucket/rqm`.
    - `packages=()` skips Maven resolution when the runtime image already
      ships the JARs; `conf` carries any other Spark setting (driver memory,
      shuffle partitions, S3 credentials provider, ...) and wins over the
      values derived here.
    """

    warehouse: str | Path | None
    catalog: str = "rqm"
    catalog_type: str | None = "hadoop"
    catalog_properties: Mapping[str, str] = field(default_factory=dict)
    master: str | None = "local[*]"
    app_name: str = "rqm-nsi-ingest"
    packages: tuple[str, ...] = SPARK_PACKAGES
    conf: Mapping[str, str] = field(default_factory=dict)


_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


def warehouse_uri(warehouse: str | Path) -> str:
    """Return `warehouse` as a URI; local paths become absolute `file://` URIs."""
    text = str(warehouse)
    if _URI_RE.match(text):
        return text
    return Path(text).resolve().as_uri()


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
    if settings.catalog_type:
        conf[f"{cat}.type"] = settings.catalog_type
    if settings.warehouse is not None:
        conf[f"{cat}.warehouse"] = warehouse_uri(settings.warehouse)
    conf.update(
        {f"{cat}.{key}": value for key, value in settings.catalog_properties.items()}
    )
    conf.update(settings.conf)
    return conf


def build_spark(settings: SparkSettings) -> SparkSession:
    """Start (or reuse) a Spark session with Sedona + Iceberg configured.

    The Spark / Sedona / Iceberg packages are resolved from Maven Central at
    first run; subsequent runs reuse the Ivy cache.
    """
    builder = SparkSession.builder
    for key, value in spark_conf(settings).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()

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
) -> None:
    """Apply the CREATE TABLE statements from schema.sql.

    `layouts` maps unqualified table names to the partition spec and write
    order used when the table is created; tables without an entry are
    unpartitioned and unsorted. Layouts are only applied at creation —
    existing tables are left untouched, so changing the layout of a populated
    table is an explicit Iceberg partition evolution (`ALTER TABLE ... ADD /
    DROP PARTITION FIELD`, `WRITE ORDERED BY`).

    Spark SQL accepts one statement per `spark.sql(...)` call, so the script
    is split with `split_sql_statements`.
    """
    sql_text = SCHEMA_SQL.read_text(encoding="utf-8")
    # Qualify table identifiers with the catalog name.
    sql_text = sql_text.replace("rqm.", f"{catalog}.")
    statements = split_sql_statements(sql_text)

    schema_tables = {t.split(".")[-1] for t in map(created_table, statements) if t}
    unknown = sorted(set(layouts) - schema_tables)
    if unknown:
        raise ValueError(f"layouts given for tables not in schema.sql: {unknown}")

    for statement in statements:
        table = created_table(statement)
        if table is None:
            spark.sql(statement)
        elif not spark.catalog.tableExists(table):
            layout = layouts.get(table.split(".")[-1], TableLayout())
            for sql in layout_sql(statement, layout):
                spark.sql(sql)


def load_buildings(spark: SparkSession, parquet_path: Path, catalog: str) -> int:
    """Read the NSI parquet, project to buildings columns, append to Iceberg.

    Returns the row count that was written.
    """
    raw = spark.read.parquet(str(parquet_path))
    projected = raw.select(
        *[F.col(c).cast(t).alias(c) for c, t in BUILDINGS_CAST.items()]
    )
    # NSI fd_id is unique per structure and stable across loads, so it is the
    # identity; any future non-NSI source needs its own id scheme.
    with_id = projected.withColumn("building_id", F.col("fd_id"))
    with_ts = with_id.withColumn("ingested_at", F.current_timestamp())
    buildings_df = with_ts.select(*BUILDINGS_COLUMNS)

    target = f"{catalog}.buildings"
    buildings_df.writeTo(target).append()
    return spark.table(target).count()


def load_geometries(spark: SparkSession, parquet_path: Path, catalog: str) -> int:
    """Derive one 'point' geometry row per NSI structure and append to Iceberg.

    `building_id` is the NSI `fd_id`; a semi-join against `buildings` keeps the
    logical FK intact. `geometry_id` is the stable hash from
    `geometry_id_expr()`; the write is refused if it collides with a different
    natural key. `geom` is written as a native GEOMETRY(3857); query it
    with Sedona via `sedona_geom_sql()` (`ST_GeomFromWKB(spark_st_asbinary(geom))`).
    """
    raw = spark.read.parquet(str(parquet_path)).select(
        F.col("fd_id").cast("bigint").alias("building_id"),
        F.col("source").cast("string").alias("source"),
        F.col("shape").alias("wkb_raw"),
    )
    buildings = spark.table(f"{catalog}.buildings").select("building_id")
    joined = raw.join(buildings, on="building_id", how="left_semi")

    geoms = joined.select(
        F.col("building_id"),
        F.lit("point").alias("role"),
        F.lit("computation").alias("purpose"),
        F.lit(True).alias("is_selected_for_export"),
        F.lit(None).cast("string").alias("alt_group_key"),
        F.concat(F.lit("NSI-"), F.coalesce(F.col("source"), F.lit("unknown"))).alias(
            "source_ref"
        ),
        F.lit(f"EPSG:{GEOM_SRID}").alias("crs"),
        F.expr(native_geom_sql("CAST(wkb_raw AS BINARY)")).alias("geom"),
        F.current_timestamp().alias("ingested_at"),
    )
    target = f"{catalog}.geometries"
    geoms = geoms.select(geometry_id_expr().alias("geometry_id"), *geoms.columns)

    assert_no_geometry_id_collisions(geoms, spark.table(target))
    geoms.writeTo(target).append()
    return spark.table(target).count()


def diagnostic_coverage_check(parquet_path: Path) -> list[str]:
    """Return a list of NSI columns that the schema does not map.

    Guards against NSI release drift silently dropping fields: any column
    present in the parquet but absent from NSI_SCALAR_FIELDS + {'shape'} is
    reported here.
    """
    pf = pq.ParquetFile(str(parquet_path))
    parquet_cols = {f.name for f in pf.schema_arrow}
    known = set(NSI_SCALAR_FIELDS) | {"shape"}
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


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--parquet", type=Path, required=True, help="Path to the NSI parquet file."
    )
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
        "--catalog-type",
        default="hadoop",
        help="Iceberg catalog type: hadoop (default), rest, glue, hive, jdbc, ...",
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
        default="local[*]",
        help="Spark master (default: local[*]); pass '' to defer to spark-submit.",
    )
    ap.add_argument(
        "--conf",
        type=_key_value,
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra Spark config, e.g. spark.driver.memory=8g; repeatable.",
    )
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
    args = ap.parse_args()

    if not args.parquet.exists():
        print(f"error: parquet not found: {args.parquet}", file=sys.stderr)
        return 2

    if not _URI_RE.match(args.warehouse):
        Path(args.warehouse).mkdir(parents=True, exist_ok=True)

    unmapped = diagnostic_coverage_check(args.parquet)
    if unmapped:
        print(
            "warning: NSI columns present in parquet but not mapped by this "
            f"schema (release drift?): {unmapped}",
            file=sys.stderr,
        )

    settings = SparkSettings(
        warehouse=args.warehouse,
        catalog=args.catalog,
        catalog_type=args.catalog_type or None,
        catalog_properties=dict(args.catalog_prop),
        master=args.master or None,
        conf=dict(args.conf),
    )
    layouts = layouts_from_args(args.partition, args.write_order)

    spark = build_spark(settings)
    try:
        apply_schema(spark, args.catalog, layouts)
        n_buildings = load_buildings(spark, args.parquet, args.catalog)
        n_geoms = load_geometries(spark, args.parquet, args.catalog)
        parquet_rows = pq.ParquetFile(str(args.parquet)).metadata.num_rows
        print(
            f"OK: ingested {n_buildings} buildings, {n_geoms} geometries "
            f"from {parquet_rows} parquet rows "
            f"into Iceberg warehouse at {args.warehouse}."
        )
        if n_buildings != parquet_rows or n_geoms != parquet_rows:
            print("warning: row-count mismatch; smoke tests will fail", file=sys.stderr)
            return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
