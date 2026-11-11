"""Load any building inventory into the RQM tables through a YAML source
spec (ADR-multi-source).

Usage
-----
    python tools/ingest_source.py \
        --spec test/data/sources/county-assessor.yaml \
        --input /data/rqm/assessor-r1.gpkg \
        --warehouse test/warehouse \
        --version assessor-r1 --editor jdoe --reason "county assessor roll 2025"

Session and catalog options (`--master`, `--catalog-type`, `--catalog-prop`,
`--namespace`, `--conf`) are shared with `ingest_nsi.py`; see `--help`.

Each source keeps its own rows in `buildings`, scoped by `source_ref`, side
by side with every other source (no cross-source matching). A load:

1. validates the spec (`sources.load_spec`) before Spark starts;
2. reads the input (parquet / GeoParquet / CSV through Spark, GeoPackage
   through Sedona with a pyogrio fallback), checks the referenced columns
   exist and the native ids are unique and non-null;
3. applies the field rules (renames, Spark SQL expressions, code
   crosswalks, defaults), casts to the `buildings` types, transforms the
   geometry to EPSG:3857, derives a computation point (`ST_PointOnSurface`
   for polygons) and, unless mapped, `x` / `y` as WGS84 lon / lat;
4. upserts the `sources` registry row (main, never branched);
5. MERGEs the source's buildings as a full replacement *of that source*:
   rows of other sources are never touched;
6. MERGEs the source's geometries (a computation point per building, plus
   a display footprint for polygon sources), deletes the source's stale
   geometries and any geometry whose building is gone.

`building_id` is the native id for NSI (`fd_id`) and
`ingest_nsi.source_building_id_expr(source_ref, source_record_id)` (always
negative) for every other source.
"""

from __future__ import annotations

import argparse
import atexit
import shutil
import sys
import tempfile
from pathlib import Path

from ingest_nsi import (
    BUILDING_COMPARE_COLUMNS,
    BUILDINGS_CAST,
    BUILDINGS_COLUMNS,
    GEOM_SRID,
    NAMESPACE,
    add_session_arguments,
    apply_schema,
    assert_no_building_id_collisions,
    assert_no_geometry_id_collisions,
    build_spark,
    conform_buildings,
    conformed_sql,
    default_editor,
    geometry_id_expr,
    native_geom_sql,
    require_branch,
    settings_from_args,
    source_building_id_expr,
    table_name,
    validate_ref_name,
)
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from sources import SourceSpec, SpecError, load_spec

TARGET_CRS = f"EPSG:{GEOM_SRID}"

# Spark geometry type names (Sedona ST_GeometryType) allowed per geometry
# kind (ADR-nsi-minimal rule 3).
GEOMETRY_TYPES: dict[str, tuple[str, ...]] = {
    "point": ("ST_Point",),
    "polygon": ("ST_Polygon", "ST_MultiPolygon"),
}

# Working columns `prepare_input` adds beside the buildings columns.
POINT_WKB = "_rqm_point_wkb"
FOOTPRINT_WKB = "_rqm_footprint_wkb"
GEOM_REF = "_rqm_geom_ref"
GEOM_TYPE = "_rqm_geom_type"
_SEDONA_GEOM = "_rqm_g"


def quote_ident(name: str) -> str:
    """Spark SQL identifier for an input column name (any characters)."""
    return "`" + name.replace("`", "``") + "`"


def sql_str(value: object) -> str:
    """Spark SQL string literal."""
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


# ---------------------------------------------------------------------------
# Reading inputs
# ---------------------------------------------------------------------------


def _stage_gpkg_with_pyogrio(spec: SourceSpec, path: Path) -> Path:
    """Copy a GeoPackage layer to a temporary parquet (geometry as WKB)."""
    import pyarrow.parquet as pq
    import pyogrio

    meta, table = pyogrio.read_arrow(str(path), layer=spec.input.layer)
    geometry = meta.get("geometry_name") or "wkb_geometry"
    if geometry in table.column_names and geometry != spec.input.geometry.column:
        names = [
            spec.input.geometry.column if n == geometry else n
            for n in table.column_names
        ]
        table = table.rename_columns(names)
    staged = Path(tempfile.mkdtemp(prefix="rqm-gpkg-")) / "layer.parquet"
    atexit.register(shutil.rmtree, staged.parent, True)
    pq.write_table(table, staged)
    return staged


def read_input(
    spark: SparkSession, spec: SourceSpec, input_path: str | Path
) -> tuple[DataFrame, str]:
    """Read the input file; returns the frame and how its geometry is encoded.

    Encodings: `wkb` (binary column), `wkt` (text column), `geometry`
    (Sedona geometry from the GeoParquet / GeoPackage readers), or
    `lonlat` (two coordinate columns).
    """
    fmt, path = spec.input.format, str(input_path)
    options = dict(spec.input.options)
    lonlat = spec.input.geometry.column is None
    if fmt == "parquet":
        return spark.read.options(**options).parquet(path), (
            "lonlat" if lonlat else "wkb"
        )
    if fmt == "geoparquet":
        return spark.read.format("geoparquet").options(**options).load(path), "geometry"
    if fmt == "csv":
        options.setdefault("header", "true")
        return spark.read.options(**options).csv(path), "lonlat" if lonlat else "wkt"
    if fmt == "gpkg":
        try:
            df = (
                spark.read.format("geopackage")
                .option("tableName", spec.input.layer)
                .options(**options)
                .load(path)
            )
            if spec.input.geometry.column in df.columns:
                return df, "geometry"
        except Exception as err:  # Sedona reader unavailable or layer unsupported
            print(
                f"note: Sedona GeoPackage reader failed ({type(err).__name__}); "
                "staging the layer through pyogrio",
                file=sys.stderr,
            )
        staged = _stage_gpkg_with_pyogrio(spec, Path(path))
        return spark.read.parquet(str(staged)), "wkb"
    raise ValueError(f"unsupported input format {fmt!r}")


def _geometry_sql(spec: SourceSpec, encoding: str) -> str:
    """Sedona geometry of the input row, in the input CRS."""
    g = spec.input.geometry
    if encoding == "lonlat":
        return (
            f"ST_Point(CAST({quote_ident(g.lon)} AS DOUBLE), "
            f"CAST({quote_ident(g.lat)} AS DOUBLE))"
        )
    col = quote_ident(g.column)
    if encoding == "wkb":
        return f"ST_GeomFromWKB({col})"
    if encoding == "wkt":
        return f"ST_GeomFromWKT({col})"
    return col


def _with_geometry(df: DataFrame, spec: SourceSpec, encoding: str) -> DataFrame:
    """Add the computation point / footprint as EPSG:3857 WKB, plus its type.

    WKB points already in EPSG:3857 (NSI) pass through byte for byte, so
    their geometries compare equal across reloads; everything else goes
    through Sedona (`ST_Transform`, `ST_PointOnSurface`). The geometry type
    is checked for every source except the built-in NSI spec, whose
    national-scale load skips the extra WKB parse.
    """
    kind, crs = spec.input.geometry.kind, spec.input.crs
    if encoding == "wkb" and kind == "point" and crs == TARGET_CRS:
        column = quote_ident(spec.input.geometry.column)
        geom_type = (
            F.lit(None).cast("string")
            if spec.builtin
            else F.expr(f"ST_GeometryType(ST_GeomFromWKB({column}))")
        )
        return (
            df.withColumn(POINT_WKB, F.expr(f"CAST({column} AS BINARY)"))
            .withColumn(FOOTPRINT_WKB, F.lit(None).cast("binary"))
            .withColumn(GEOM_TYPE, geom_type)
        )
    geom = _geometry_sql(spec, encoding)
    if crs != TARGET_CRS:
        geom = f"ST_Transform({geom}, {sql_str(crs)}, {sql_str(TARGET_CRS)})"
    df = df.withColumn(_SEDONA_GEOM, F.expr(geom)).withColumn(
        GEOM_TYPE, F.expr(f"ST_GeometryType({_SEDONA_GEOM})")
    )
    if kind == "polygon":
        df = df.withColumn(FOOTPRINT_WKB, F.expr(f"ST_AsBinary({_SEDONA_GEOM})"))
        point = f"ST_PointOnSurface({_SEDONA_GEOM})"
    else:
        df = df.withColumn(FOOTPRINT_WKB, F.lit(None).cast("binary"))
        point = _SEDONA_GEOM
    return df.withColumn(POINT_WKB, F.expr(f"ST_AsBinary({point})")).drop(_SEDONA_GEOM)


# ---------------------------------------------------------------------------
# Field mapping
# ---------------------------------------------------------------------------


# Integral target types. Text such as "2.0" (common in CSV exports) does not
# cast to them directly under ANSI mode, so text goes through DECIMAL first.
_INTEGRAL_TYPES = {"smallint", "int", "bigint"}


def rule_column(
    spec: SourceSpec, target: str, text_columns: frozenset[str] = frozenset()
) -> Column:
    """The value of buildings column `target`, cast to its type (NULL if unmapped).

    `text_columns` are the input's string columns; a rename of one of them
    into an integral column is cast through DECIMAL(38, 6).
    """
    dtype = BUILDINGS_CAST[target]
    rule = spec.rules.get(target)
    if rule is None:
        return F.lit(None).cast(dtype).alias(target)
    if rule.expr is not None:
        value = F.expr(rule.expr)
    elif rule.source is not None and rule.mapping is not None:
        code = F.trim(F.col(quote_ident(rule.source)).cast("string"))
        value = F.lit(None)
        for key, mapped in rule.mapping.items():
            value = F.when(code == F.lit(key.strip()), F.lit(mapped)).otherwise(value)
    elif rule.source is not None:
        value = F.col(quote_ident(rule.source))
        if rule.source in text_columns and dtype in _INTEGRAL_TYPES:
            value = F.trim(value).cast("decimal(38,6)")
    else:
        value = F.lit(None)
    value = value.cast(dtype)
    if rule.default is not None:
        value = F.coalesce(value, F.lit(rule.default).cast(dtype))
    return value.alias(target)


def prepare_input(
    spark: SparkSession, spec: SourceSpec, input_path: str | Path
) -> DataFrame:
    """The input mapped onto `buildings` (every column except `ingested_at`)
    plus the working geometry columns `_rqm_point_wkb`, `_rqm_footprint_wkb`
    (polygon sources), `_rqm_geom_ref` (geometries.source_ref), and
    `_rqm_geom_type`.

    Raises if the input lacks a column the spec reads by name.
    """
    raw, encoding = read_input(spark, spec, input_path)
    missing = sorted(spec.input_columns - set(raw.columns))
    if missing:
        raise ValueError(
            f"{input_path}: columns {missing} named by spec {spec.source_ref!r} "
            f"are not in the input (it has {sorted(raw.columns)})"
        )
    staged = _with_geometry(raw, spec, encoding)
    text_columns = frozenset(
        f.name for f in raw.schema.fields if f.dataType.typeName() == "string"
    )

    id_col = F.col(quote_ident(spec.id.column))
    if spec.id.scheme == "native":
        record_id = id_col.cast("bigint").cast("string")
        building_id = id_col.cast("bigint")
    else:
        record_id = id_col.cast("string")
        building_id = None
    subcode = spec.input.geometry.subcode
    geom_ref = (
        F.concat(
            F.lit(f"{spec.source_ref}-"),
            F.coalesce(F.col(quote_ident(subcode)).cast("string"), F.lit("unknown")),
        )
        if subcode
        else F.lit(spec.source_ref)
    )
    mapped = staged.select(
        F.lit(spec.source_ref).alias("source_ref"),
        record_id.alias("source_record_id"),
        *([building_id.alias("building_id")] if building_id is not None else []),
        *[rule_column(spec, c, text_columns) for c in BUILDINGS_CAST],
        F.col(POINT_WKB),
        F.col(FOOTPRINT_WKB),
        geom_ref.alias(GEOM_REF),
        F.col(GEOM_TYPE),
    )
    if building_id is None:
        mapped = mapped.withColumn(
            "building_id",
            source_building_id_expr(F.col("source_ref"), F.col("source_record_id")),
        )
    if "x" not in spec.rules:
        # NSI publishes x / y as WGS84 lon / lat; other sources follow suit.
        lonlat = f"ST_Transform(ST_GeomFromWKB({POINT_WKB}), {sql_str(TARGET_CRS)}, 'EPSG:4326')"
        mapped = mapped.withColumn("x", F.expr(f"ST_X({lonlat})")).withColumn(
            "y", F.expr(f"ST_Y({lonlat})")
        )
    return mapped.select(
        *[c for c in BUILDINGS_COLUMNS if c != "ingested_at"],
        POINT_WKB,
        FOOTPRINT_WKB,
        GEOM_REF,
        GEOM_TYPE,
    )


def validate_input(
    spark: SparkSession, spec: SourceSpec, input_path: str | Path
) -> dict:
    """Load-time checks; raises ValueError listing every failed check.

    Checks: native ids are non-null and unique; every row has a geometry of
    the spec's kind (ADR-nsi-minimal rules 1 and 3); `occtype` is non-null
    (rule 5). Every mapped value is computed, so a value that does not cast
    to its column type fails here rather than mid-load. Returns `n_input`
    and `n_null_cbfips` (rows that will land in the NULL-state partition of
    `buildings`).
    """
    prepared = prepare_input(spark, spec, input_path)
    allowed = ", ".join(sql_str(t) for t in GEOMETRY_TYPES[spec.input.geometry.kind])
    bad_type = F.expr(f"{GEOM_TYPE} IS NOT NULL AND {GEOM_TYPE} NOT IN ({allowed})")
    row = prepared.agg(
        F.count("*").alias("n"),
        F.sum(F.col("source_record_id").isNull().cast("int")).alias("null_ids"),
        F.countDistinct("source_record_id").alias("distinct_ids"),
        F.sum(F.col(POINT_WKB).isNull().cast("int")).alias("null_geom"),
        F.sum(bad_type.cast("int")).alias("bad_geom"),
        F.sum(F.col("occtype").isNull().cast("int")).alias("null_occtype"),
        F.sum(F.col("cbfips").isNull().cast("int")).alias("null_cbfips"),
        # Forces every mapped column (and its cast) to be evaluated.
        F.max(F.xxhash64(*BUILDINGS_CAST)).alias("_all_columns"),
    ).collect()[0]
    n = row["n"]
    problems = []
    if not n:
        problems.append("the input has no rows")
    if row["null_ids"]:
        problems.append(f"{row['null_ids']} rows have a NULL id ({spec.id.column})")
    n_dupes = n - (row["null_ids"] or 0) - row["distinct_ids"]
    if n_dupes:
        dupes = (
            prepared.groupBy("source_record_id")
            .count()
            .where("count > 1")
            .limit(5)
            .collect()
        )
        problems.append(
            f"{n_dupes} duplicate ids in {spec.id.column}, e.g. "
            f"{[r['source_record_id'] for r in dupes]}"
        )
    if row["null_geom"]:
        problems.append(f"{row['null_geom']} rows have no geometry (rule 1)")
    if row["bad_geom"]:
        kinds = ", ".join(GEOMETRY_TYPES[spec.input.geometry.kind])
        problems.append(
            f"{row['bad_geom']} geometries are not {kinds} (rule 3; "
            f"input.geometry.kind={spec.input.geometry.kind})"
        )
    if row["null_occtype"]:
        problems.append(
            f"{row['null_occtype']} rows map to a NULL occtype (rule 5): "
            "add the codes to the crosswalk or give a default"
        )
    if problems:
        raise ValueError(
            f"{input_path} does not load as {spec.source_ref!r}:\n"
            + "\n".join(f"  - {p}" for p in problems)
        )
    return {"n_input": n, "n_null_cbfips": row["null_cbfips"] or 0}


def crosswalk_gaps(
    spark: SparkSession, spec: SourceSpec, input_path: str | Path, limit: int = 10
) -> dict[str, dict[str, int]]:
    """Input codes that a `map` rule does not cover, with their row counts.

    Such rows get the rule's `default` (or NULL); the most frequent `limit`
    codes are reported per target column.
    """
    raw, _ = read_input(spark, spec, input_path)
    gaps: dict[str, dict[str, int]] = {}
    for rule in spec.fields:
        if rule.mapping is None:
            continue
        code = F.trim(F.col(quote_ident(rule.source)).cast("string"))
        keys = [k.strip() for k in rule.mapping]
        rows = (
            raw.select(code.alias("code"))
            .where(F.col("code").isNotNull() & ~F.col("code").isin(keys))
            .groupBy("code")
            .count()
            .orderBy(F.col("count").desc(), "code")
            .limit(limit)
            .collect()
        )
        if rows:
            gaps[rule.target] = {r["code"]: r["count"] for r in rows}
    return gaps


# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------


def register_source(
    spark: SparkSession, spec: SourceSpec, catalog: str, namespace: str = NAMESPACE
) -> None:
    """Upsert the `sources` row of `spec` (always on main; never deleted)."""
    target = table_name(catalog, "sources", namespace)
    row = spark.sql(
        "SELECT :source_ref AS source_ref, :source_name AS source_name, "
        ":source_kind AS source_kind, CAST(:description AS STRING) AS description, "
        "CAST(:spec_sha256 AS STRING) AS spec_sha256, current_timestamp() AS updated_at",
        args={
            "source_ref": spec.source_ref,
            "source_name": spec.source_name,
            "source_kind": spec.source_kind,
            "description": spec.description,
            "spec_sha256": spec.sha256,
        },
    )
    row.createOrReplaceTempView("_rqm_source_row")
    try:
        spark.sql(
            f"""
            MERGE INTO {target} t USING _rqm_source_row s
            ON t.source_ref = s.source_ref
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView("_rqm_source_row")


def list_sources(
    spark: SparkSession, catalog: str, namespace: str = NAMESPACE
) -> DataFrame:
    """The `sources` registry, ordered by source_ref."""
    return spark.table(table_name(catalog, "sources", namespace)).orderBy("source_ref")


def get_source(
    spark: SparkSession, catalog: str, source_ref: str, namespace: str = NAMESPACE
) -> Row:
    """The `sources` row of `source_ref`; raises KeyError if unregistered."""
    rows = (
        list_sources(spark, catalog, namespace)
        .where(F.col("source_ref") == source_ref)
        .collect()
    )
    if not rows:
        raise KeyError(f"unknown source {source_ref!r}")
    return rows[0]


# ---------------------------------------------------------------------------
# Scoped MERGEs
# ---------------------------------------------------------------------------


def source_scope_sql(spec_or_ref: SourceSpec | str, alias: str | None = None) -> str:
    """SQL predicate: a `buildings` row belongs to the source."""
    ref = spec_or_ref if isinstance(spec_or_ref, str) else spec_or_ref.source_ref
    return f"{conformed_sql('source_ref', alias)} = {sql_str(ref)}"


def geometry_scope_sql(spec: SourceSpec, alias: str = "g") -> str:
    """SQL predicate: a `geometries` row was derived by the source
    (`source_ref` = `<ref>` or `<ref>-<subcode>`)."""
    ref = spec.source_ref
    return (
        f"({alias}.source_ref = {sql_str(ref)} "
        f"OR startswith({alias}.source_ref, {sql_str(ref + '-')}))"
    )


def merge_buildings(
    spark: SparkSession,
    spec: SourceSpec,
    input_path: str | Path,
    catalog: str,
    namespace: str = NAMESPACE,
    branch: str | None = None,
    validate: bool = True,
) -> int:
    """MERGE the source's buildings into `branch` as a full replacement of
    that source.

    Rows match on `building_id`; new ones are inserted, changed ones updated
    with a fresh `ingested_at`, unchanged ones left alone, and buildings of
    this source missing from the input are deleted. Rows of other sources
    are never touched. Returns the source's row count at the branch head.
    """
    if validate:
        validate_input(spark, spec, input_path)
    target = table_name(catalog, "buildings", namespace, branch)
    rows = (
        prepare_input(spark, spec, input_path)
        .withColumn("ingested_at", F.current_timestamp())
        .select(*BUILDINGS_COLUMNS)
    )
    if spec.id.scheme == "hash":
        existing = conform_buildings(spark.table(target)).where("building_id < 0")
        assert_no_building_id_collisions(rows, existing)

    changed = " OR ".join(
        f"NOT ({conformed_sql(c, 't')} <=> s.{c})" for c in BUILDING_COMPARE_COLUMNS
    )
    rows.createOrReplaceTempView("_rqm_load_buildings")
    try:
        spark.sql(
            f"""
            MERGE INTO {target} t
            USING _rqm_load_buildings s
            ON t.building_id = s.building_id
            WHEN MATCHED AND ({changed}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE AND {source_scope_sql(spec, "t")} THEN DELETE
            """
        )
    finally:
        spark.catalog.dropTempView("_rqm_load_buildings")
    return spark.table(target).where(source_scope_sql(spec)).count()


def merge_geometries(
    spark: SparkSession,
    spec: SourceSpec,
    input_path: str | Path,
    catalog: str,
    namespace: str = NAMESPACE,
    branch: str | None = None,
) -> int:
    """MERGE the source's geometries into `branch`; run after `merge_buildings`.

    Per building present on the branch: a `role='point'`,
    `purpose='computation'`, selected row, and for polygon sources a
    `role='footprint'`, `purpose='display'` row. `geometry_id` is the stable
    natural-key hash (`geometry_id_expr`). Existing rows are updated only
    when the geometry or CRS changed; `is_selected_for_export` is never
    overwritten. Afterwards, geometries the source derived for its own
    buildings that are not in this load are deleted, as are geometries of
    any source whose building is gone. Returns the count of geometries the
    source derived for its own buildings (`geometry_scope_sql` on buildings
    of the source) at the branch head.
    """
    target = table_name(catalog, "geometries", namespace, branch)
    buildings_table = table_name(catalog, "buildings", namespace, branch)
    present = spark.table(buildings_table).select("building_id")
    rows = prepare_input(spark, spec, input_path).join(
        present, "building_id", "left_semi"
    )

    def geometry_rows(role: str, purpose: str, selected: bool, wkb: str) -> DataFrame:
        return rows.select(
            F.col("building_id"),
            F.lit(role).alias("role"),
            F.lit(purpose).alias("purpose"),
            F.lit(selected).alias("is_selected_for_export"),
            F.lit(None).cast("string").alias("alt_group_key"),
            F.col(GEOM_REF).alias("source_ref"),
            F.lit(TARGET_CRS).alias("crs"),
            F.expr(native_geom_sql(wkb)).alias("geom"),
            F.current_timestamp().alias("ingested_at"),
        )

    geoms = geometry_rows("point", "computation", True, POINT_WKB)
    if spec.input.geometry.kind == "polygon":
        geoms = geoms.unionByName(
            geometry_rows("footprint", "display", False, FOOTPRINT_WKB)
        )
    geoms = geoms.select(geometry_id_expr().alias("geometry_id"), *geoms.columns)

    assert_no_geometry_id_collisions(geoms, spark.table(target))
    geoms.createOrReplaceTempView("_rqm_load_geometries")
    try:
        spark.sql(
            f"""
            MERGE INTO {target} t
            USING _rqm_load_geometries s
            ON t.geometry_id = s.geometry_id
            WHEN MATCHED AND (
                NOT (spark_st_asbinary(t.geom) <=> spark_st_asbinary(s.geom))
                OR NOT (t.crs <=> s.crs)
            ) THEN UPDATE SET geom = s.geom, crs = s.crs, ingested_at = s.ingested_at
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        # This source's geometries of its own buildings that the load no
        # longer produces, and geometries (any source) of removed buildings.
        stale = f"""
            SELECT g.geometry_id FROM {target} g
            JOIN {buildings_table} b ON g.building_id = b.building_id
            LEFT ANTI JOIN _rqm_load_geometries l ON g.geometry_id = l.geometry_id
            WHERE {source_scope_sql(spec, "b")} AND {geometry_scope_sql(spec, "g")}
            UNION
            SELECT g.geometry_id FROM {target} g
            LEFT ANTI JOIN {buildings_table} b ON g.building_id = b.building_id
        """
        if spark.sql(stale).limit(1).count():
            spark.sql(
                f"""
                MERGE INTO {target} t USING ({stale}) s
                ON t.geometry_id = s.geometry_id
                WHEN MATCHED THEN DELETE
                """
            )
    finally:
        spark.catalog.dropTempView("_rqm_load_geometries")
    return spark.sql(
        f"""
        SELECT COUNT(*) AS n FROM {target} g
        JOIN {buildings_table} b ON g.building_id = b.building_id
        WHERE {source_scope_sql(spec, "b")} AND {geometry_scope_sql(spec, "g")}
        """
    ).collect()[0]["n"]


def load_source(
    spark: SparkSession,
    spec: SourceSpec,
    input_path: str | Path,
    catalog: str,
    namespace: str = NAMESPACE,
    branch: str | None = None,
) -> dict:
    """Load one release of a source into `branch` (default main).

    Validates the input, registers the source, then MERGEs its buildings and
    geometries as a full replacement of that source only. Returns
    `source_ref`, `n_input`, `n_buildings` and `n_geometries` (the source's
    rows at the branch head), `n_null_cbfips`, and `unmapped_codes`
    (`crosswalk_gaps`).
    """
    require_branch(spark, catalog, branch, namespace)
    stats = validate_input(spark, spec, input_path)
    register_source(spark, spec, catalog, namespace)
    n_buildings = merge_buildings(
        spark, spec, input_path, catalog, namespace, branch, validate=False
    )
    n_geometries = merge_geometries(spark, spec, input_path, catalog, namespace, branch)
    return {
        "source_ref": spec.source_ref,
        **stats,
        "n_buildings": n_buildings,
        "n_geometries": n_geometries,
        "unmapped_codes": crosswalk_gaps(spark, spec, input_path),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--spec", type=Path, required=True, help="Source spec (YAML).")
    ap.add_argument(
        "--input", type=Path, required=True, help="Input file (or parquet directory)."
    )
    add_session_arguments(ap)
    versioning = ap.add_argument_group(
        "versioning", "Record the load as a named inventory version (Iceberg tag)."
    )
    versioning.add_argument("--version", help="Version name to commit after the load.")
    versioning.add_argument(
        "--branch", default="main", help="Branch to load into (default: main)."
    )
    versioning.add_argument("--editor", default=None, help="Default: $USER.")
    versioning.add_argument("--reason", default=None)
    versioning.add_argument(
        "--parent",
        default=None,
        help="Parent version (default: the nearest version in the branch history).",
    )
    versioning.add_argument(
        "--kind",
        default="release",
        choices=["release", "edit"],
        help="Default: release.",
    )
    args = ap.parse_args(argv)

    try:
        spec = load_spec(args.spec)
    except SpecError as err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    if not args.input.exists():
        print(f"error: input not found: {args.input}", file=sys.stderr)
        return 2
    if args.version:
        validate_ref_name(args.version, "version")
    if args.branch != "main":
        validate_ref_name(args.branch, "branch")

    spark = build_spark(settings_from_args(args))
    cat, ns, branch = args.catalog, args.namespace, args.branch
    try:
        apply_schema(spark, cat, namespace=ns)
        if args.version:
            from versioning import ensure_version_available

            ensure_version_available(spark, cat, args.version, ns)
        result = load_source(spark, spec, args.input, cat, ns, branch)
        print(
            f"OK: loaded {result['n_buildings']} {spec.source_ref} buildings and "
            f"{result['n_geometries']} geometries from {result['n_input']} input rows "
            f"into branch {branch!r}."
        )
        if result["n_null_cbfips"]:
            print(
                f"warning: {result['n_null_cbfips']} rows have no cbfips and land in "
                "the NULL state partition.",
                file=sys.stderr,
            )
        for target, codes in result["unmapped_codes"].items():
            print(
                f"warning: {target} crosswalk does not cover input codes {codes} "
                "(default or NULL used).",
                file=sys.stderr,
            )
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
                source_ref=spec.source_ref,
                source_uri=str(args.input.resolve()),
                source_sha256=file_sha256(args.input),
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


if __name__ == "__main__":
    sys.exit(main())
