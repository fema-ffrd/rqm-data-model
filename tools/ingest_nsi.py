"""Ingest a National Structure Inventory (NSI) parquet into the NSI-minimal
RQM schema (Apache Iceberg tables accessed via Apache Sedona on Spark).

Usage
-----
    python tools/ingest_nsi.py \
        --parquet test/data/nsi.parquet \
        --warehouse test/warehouse \
        --catalog rqm

The script:

1. Starts a local PySpark session with Sedona and Iceberg extensions.
2. Registers a Hadoop-backed Iceberg catalog at `--warehouse`.
3. Applies the DDL in `tools/schema.sql` (idempotent CREATE TABLE IF NOT EXISTS).
4. Reads the NSI parquet, projects to the `buildings` column order, adds a
   monotonic `building_id` surrogate and an `ingested_at` timestamp, and
   appends to `rqm.buildings`.
5. Derives `rqm.geometries` rows from the same source: one row per structure
   with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`,
   `geom` stored as WKB (BINARY).

Logical invariants (ADR-nsi-minimal validation rules 1-6) are enforced by
inspection of the input + a post-write sanity check; the full assertion
suite lives in `test/test_nsi_ingest.py`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
SCHEMA_SQL = HERE / "schema.sql"

# Order matches tools/schema.sql rqm.buildings.
BUILDINGS_COLUMNS: list[str] = [
    "building_id", "fd_id", "bid", "usastrucid", "ftprntid", "ftprntsrc", "source",
    "occtype", "st_damcat", "bldgtype", "num_story", "sqft", "ftprntsqft",
    "bldheight", "med_yr_blt", "found_type", "found_ht",
    "val_struct", "val_cont", "val_vehic", "ornl_low", "ornl_med", "ornl_hgh",
    "resunits", "novehprob", "vehperunit",
    "pop2amu65", "pop2amo65", "pop2pmu65", "pop2pmo65",
    "u65disable", "o65disable", "students",
    "firmzone", "zone_sub", "static_bfe", "ground_elv", "grnd_elv_m",
    "pctlowclr", "fullrep", "depindex", "creprcnt", "crerank",
    "cbfips", "ingested_at",
]

# All NSI parquet columns the schema maps (every column except `shape`).
NSI_SCALAR_FIELDS: list[str] = [
    "fd_id", "bid", "usastrucid", "ftprntid", "ftprntsrc", "source",
    "occtype", "st_damcat", "bldgtype", "num_story", "sqft", "ftprntsqft",
    "bldheight", "med_yr_blt", "found_type", "found_ht",
    "val_struct", "val_cont", "val_vehic", "ornl_low", "ornl_med", "ornl_hgh",
    "resunits", "novehprob", "vehperunit",
    "pop2amu65", "pop2amo65", "pop2pmu65", "pop2pmo65",
    "u65disable", "o65disable", "students",
    "firmzone", "zone_sub", "static_bfe", "ground_elv", "grnd_elv_m",
    "pctlowclr", "fullrep", "depindex", "creprcnt", "crerank", "cbfips",
]

# Target types for the write; cast to these from the raw NSI parquet types
# so Iceberg DDL matches exactly.
BUILDINGS_CAST: dict[str, str] = {
    "fd_id": "bigint", "bid": "bigint",
    "usastrucid": "string", "ftprntid": "string", "ftprntsrc": "string", "source": "string",
    "occtype": "string", "st_damcat": "string", "bldgtype": "string",
    "num_story": "smallint", "sqft": "double", "ftprntsqft": "double",
    "bldheight": "float", "med_yr_blt": "smallint",
    "found_type": "string", "found_ht": "float",
    "val_struct": "double", "val_cont": "double", "val_vehic": "double",
    "ornl_low": "double", "ornl_med": "double", "ornl_hgh": "double",
    "resunits": "float", "novehprob": "float", "vehperunit": "float",
    "pop2amu65": "float", "pop2amo65": "float", "pop2pmu65": "float", "pop2pmo65": "float",
    "u65disable": "float", "o65disable": "float", "students": "float",
    "firmzone": "string", "zone_sub": "string",
    "static_bfe": "float", "ground_elv": "float", "grnd_elv_m": "float",
    "pctlowclr": "float", "fullrep": "float", "depindex": "float",
    "creprcnt": "float", "crerank": "int",
    "cbfips": "string",
}


def build_spark(warehouse: Path, catalog: str = "rqm") -> SparkSession:
    """Start a local Spark session with Sedona + Iceberg extensions.

    The Spark / Sedona / Iceberg packages are resolved from Maven Central at
    first run; subsequent runs reuse the Ivy cache.
    """
    warehouse_uri = warehouse.resolve().as_uri()
    sedona_pkg = "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1"
    sedona_geo = "org.datasyslab:geotools-wrapper:1.6.1-28.2"
    iceberg_pkg = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

    builder = (
        SparkSession.builder.appName("rqm-nsi-ingest")
        .master("local[*]")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.apache.sedona.sql.SedonaSqlExtensions")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .config("spark.jars.packages", ",".join([sedona_pkg, sedona_geo, iceberg_pkg]))
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse_uri)
    )
    spark = builder.getOrCreate()

    # Register Sedona SQL functions (ST_*).
    from sedona.spark import SedonaContext  # imported lazily; package is heavy
    SedonaContext.create(spark)
    return spark


def apply_schema(spark: SparkSession, catalog: str) -> None:
    """Apply CREATE NAMESPACE / CREATE TABLE statements from schema.sql.

    Spark SQL accepts one statement per `spark.sql(...)` call, so split on
    bare `;` at end-of-statement. Comments are tolerated.
    """
    sql_text = SCHEMA_SQL.read_text(encoding="utf-8")
    # Qualify namespace / table identifiers with the catalog name.
    sql_text = sql_text.replace("rqm.", f"{catalog}.").replace(
        "CREATE NAMESPACE IF NOT EXISTS rqm",
        f"CREATE NAMESPACE IF NOT EXISTS {catalog}",
    )
    for statement in filter(None, (s.strip() for s in sql_text.split(";"))):
        if statement.lstrip().startswith("--"):
            # All leading lines are comments; skip if nothing else remains
            non_comment = "\n".join(
                ln for ln in statement.splitlines() if not ln.lstrip().startswith("--")
            ).strip()
            if not non_comment:
                continue
            statement = non_comment
        spark.sql(statement)


def load_buildings(spark: SparkSession, parquet_path: Path, catalog: str) -> int:
    """Read the NSI parquet, project to buildings columns, append to Iceberg.

    Returns the row count that was written.
    """
    raw = spark.read.parquet(str(parquet_path))
    projected = raw.select(*[F.col(c).cast(t).alias(c) for c, t in BUILDINGS_CAST.items()])
    with_id = projected.withColumn("building_id", F.monotonically_increasing_id().cast("bigint"))
    with_ts = with_id.withColumn("ingested_at", F.current_timestamp())
    buildings_df = with_ts.select(*BUILDINGS_COLUMNS)

    target = f"{catalog}.buildings"
    buildings_df.writeTo(target).append()
    return spark.table(target).count()


def load_geometries(spark: SparkSession, parquet_path: Path, catalog: str) -> int:
    """Derive one 'point' geometry row per NSI structure and append to Iceberg.

    Joins back on `fd_id` so the `building_id` surrogate from the buildings
    load lines up. Geometry is stored as WKB (BINARY); callers that query
    the table wrap `geom` with `ST_GeomFromWKB(geom)`.
    """
    raw = spark.read.parquet(str(parquet_path)).select(
        F.col("fd_id").cast("bigint").alias("fd_id"),
        F.col("source").cast("string").alias("source"),
        F.col("shape").alias("wkb_raw"),
    )
    buildings = spark.table(f"{catalog}.buildings").select("building_id", "fd_id")
    joined = raw.join(buildings, on="fd_id", how="inner")

    geoms = joined.select(
        F.monotonically_increasing_id().cast("bigint").alias("geometry_id"),
        F.col("building_id"),
        F.lit("point").alias("role"),
        F.lit("computation").alias("purpose"),
        F.lit(True).alias("is_selected_for_export"),
        F.lit(None).cast("string").alias("alt_group_key"),
        F.concat(F.lit("NSI-"), F.coalesce(F.col("source"), F.lit("unknown"))).alias("source_ref"),
        F.lit("EPSG:3857").alias("crs"),
        F.col("wkb_raw").cast("binary").alias("geom"),
        F.current_timestamp().alias("ingested_at"),
    )

    target = f"{catalog}.geometries"
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--parquet", type=Path, required=True,
                    help="Path to the NSI parquet file.")
    ap.add_argument("--warehouse", type=Path, default=REPO / "test" / "warehouse",
                    help="Iceberg warehouse directory (created if missing).")
    ap.add_argument("--catalog", default="rqm", help="Iceberg catalog name (default: rqm).")
    args = ap.parse_args()

    if not args.parquet.exists():
        print(f"error: parquet not found: {args.parquet}", file=sys.stderr)
        return 2

    args.warehouse.mkdir(parents=True, exist_ok=True)

    unmapped = diagnostic_coverage_check(args.parquet)
    if unmapped:
        print(
            "warning: NSI columns present in parquet but not mapped by this "
            f"schema (release drift?): {unmapped}",
            file=sys.stderr,
        )

    spark = build_spark(args.warehouse, catalog=args.catalog)
    try:
        apply_schema(spark, args.catalog)
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
