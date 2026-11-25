"""Inventory versioning for the RQM Iceberg tables (ADR-versioning).

A *version* is a named, immutable state of the inventory:

- an Iceberg **tag** of the same name on every versioned table
  (`VERSIONED_TABLES`: `buildings`, `geometries`, and the sidecar tables
  `source_attributes` and `building_lineage` of
  ADR-source-sidecar-attributes), pointing at one snapshot of each table;
- one row in `inventory_versions` (who, when, why, source file, snapshot ids,
  change counts), written last, so a version exists only once registered;
- per-building rows in `inventory_changes` describing what changed relative
  to the parent version.

Loads are MERGE upserts that fully replace one source's rows
(`ingest_source.load_source`; `ingest_nsi.load_buildings` /
`load_geometries` for NSI), so each Iceberg snapshot holds only the rows that
changed (merge-on-read deletion vectors) and every earlier version stays
readable with `VERSION AS OF '<version>'`. Several sources sit side by side
in the same tables (ADR-multi-source); diffs, merges, and branches key on
`building_id` and work across them unchanged.

Project inventories are Iceberg **branches** of the same name on every
versioned table, created from a version. A branch is merged back by
fast-forward when the target has not moved, otherwise by a building-level
three-way merge that refuses to proceed when both sides touched the same
building. The sidecar tables follow their buildings: a commit deletes
sidecar rows of buildings that are gone, and a merge replays the branch's
sidecar changes except for buildings the target changed.

Exports pin a version: `export_version` writes the data read at the
version's snapshots plus a `_rqm_version.json` manifest naming them.

All functions take the Iceberg catalog name and the namespace (default
`inventory`) the tables live in. Version and branch names are validated by
`ingest_nsi.validate_ref_name`.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from ingest_nsi import (
    BUILDING_COMPARE_COLUMNS,
    BUILDINGS_COLUMNS,
    NAMESPACE,
    NSI_SOURCE_REF,
    conform_buildings,
    ref_snapshot_id,
    source_building_id_sql,
    table_name,
    validate_ref_name,
)
from ingest_source import get_source
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

# Tables whose state a version pins; each gets a tag named after the version
# and every branch. The sidecar tables (ADR-source-sidecar-attributes) are
# keyed by building_id and follow the buildings rows they describe.
CORE_TABLES: tuple[str, ...] = ("buildings", "geometries")
SIDECAR_TABLES: tuple[str, ...] = ("source_attributes", "building_lineage")
VERSIONED_TABLES: tuple[str, ...] = (*CORE_TABLES, *SIDECAR_TABLES)
VERSION_KINDS: tuple[str, ...] = ("release", "edit", "merge")
CHANGE_KINDS: tuple[str, ...] = ("added", "removed", "modified")

# Columns of the sidecar tables, and the ones a diff or merge compares
# (`attributes_json` is the comparable form of the `attributes` map).
SIDECAR_COLUMNS: dict[str, tuple[str, ...]] = {
    "source_attributes": (
        "building_id", "source_ref", "source_record_id", "attributes", "ingested_at",
    ),
    "building_lineage": (
        "building_id", "source_ref", "source_record_id", "source_version_name",
        "spec_sha256", "ingested_at",
    ),
}  # fmt: skip
SIDECAR_COMPARE_COLUMNS: dict[str, tuple[str, ...]] = {
    "source_attributes": ("source_ref", "source_record_id", "attributes_json"),
    "building_lineage": (
        "source_ref", "source_record_id", "source_version_name", "spec_sha256",
    ),
}  # fmt: skip

# Geometry row columns compared by a merge; `geom` is compared as WKB.
GEOMETRY_COLUMNS: tuple[str, ...] = (
    "geometry_id",
    "building_id",
    "role",
    "purpose",
    "is_selected_for_export",
    "alt_group_key",
    "source_ref",
    "crs",
    "geom",
    "ingested_at",
)
GEOMETRY_COMPARE_COLUMNS: list[str] = [
    "building_id",
    "role",
    "purpose",
    "is_selected_for_export",
    "alt_group_key",
    "source_ref",
    "crs",
    "geom_wkb",
]

# Building-level diff columns: every buildings attribute plus `geom`, the WKB
# of the building's selected computation geometry.
DIFF_COLUMNS: list[str] = [*BUILDING_COMPARE_COLUMNS, "geom"]

MANIFEST_NAME = "_rqm_version.json"
# Sidecar files of an export; `_`-prefixed so Spark, pandas, and pyarrow
# readers of the export directory skip them, like the manifest.
SIDECAR_EXPORTS: dict[str, str] = {
    "source_attributes": "_source_attributes.parquet",
    "building_lineage": "_building_lineage.parquet",
    "source_specs": "_source_specs.json",
}

# Sphere radius of EPSG:3857 (Web Mercator), for converting moved points.
WEB_MERCATOR_RADIUS_M = 6378137.0

Snapshots = Mapping[str, int]


class MergeConflictError(ValueError):
    """A three-way merge found buildings changed on both sides.

    `conflicts` holds up to 50 rows of (building_id, source_change,
    source_columns, target_change, target_columns); `n_conflicts` is the
    total.
    """

    def __init__(
        self, branch: str, into: str, conflicts: list[Row], n_conflicts: int
    ) -> None:
        self.branch = branch
        self.into = into
        self.conflicts = conflicts
        self.n_conflicts = n_conflicts
        ids = ", ".join(str(r["building_id"]) for r in conflicts[:10])
        more = "" if n_conflicts <= 10 else f", ... ({n_conflicts} total)"
        super().__init__(
            f"cannot merge {branch!r} into {into!r}: {n_conflicts} building(s) "
            f"changed on both since they diverged: {ids}{more}"
        )


# ---------------------------------------------------------------------------
# Registry and snapshot helpers
# ---------------------------------------------------------------------------


def _fq(catalog: str, table: str, namespace: str, branch: str | None = None) -> str:
    return table_name(catalog, table, namespace, branch)


def _registry(spark: SparkSession, catalog: str, namespace: str) -> DataFrame:
    return spark.table(_fq(catalog, "inventory_versions", namespace))


def list_versions(
    spark: SparkSession, catalog: str, namespace: str = NAMESPACE
) -> DataFrame:
    """All registered versions, oldest first."""
    return _registry(spark, catalog, namespace).orderBy("created_at", "version_name")


def find_version(
    spark: SparkSession, catalog: str, version: str, namespace: str = NAMESPACE
) -> Row | None:
    """The registry row of `version`, or None if it is not registered."""
    validate_ref_name(version, "version")
    rows = (
        _registry(spark, catalog, namespace)
        .where(F.col("version_name") == version)
        .collect()
    )
    if len(rows) > 1:
        raise RuntimeError(f"version {version!r} is registered {len(rows)} times")
    return rows[0] if rows else None


def get_version(
    spark: SparkSession, catalog: str, version: str, namespace: str = NAMESPACE
) -> Row:
    """The registry row of `version`; raises KeyError if it is not registered."""
    row = find_version(spark, catalog, version, namespace)
    if row is None:
        raise KeyError(f"unknown version {version!r}")
    return row


def version_snapshots(
    spark: SparkSession, catalog: str, version: str, namespace: str = NAMESPACE
) -> dict[str, int]:
    """Snapshot id of each versioned table recorded for `version`."""
    row = get_version(spark, catalog, version, namespace)
    return {t: row[f"{t}_snapshot_id"] for t in VERSIONED_TABLES}


def head_snapshots(
    spark: SparkSession, catalog: str, branch: str = "main", namespace: str = NAMESPACE
) -> dict[str, int]:
    """Current snapshot id of each versioned table on `branch`."""
    heads = {}
    for table in VERSIONED_TABLES:
        sid = ref_snapshot_id(spark, _fq(catalog, table, namespace), branch)
        if sid is None:
            raise ValueError(f"{table} has no branch {branch!r} (or no data yet)")
        heads[table] = sid
    return heads


def ensure_version_available(
    spark: SparkSession, catalog: str, version: str, namespace: str = NAMESPACE
) -> None:
    """Raise if `version` is not a usable new version name.

    The name must be unregistered and must not be a branch. A tag with the
    name but no registry row is the leftover of a failed commit and is
    replaced by the next commit.
    """
    validate_ref_name(version, "version")
    if find_version(spark, catalog, version, namespace) is not None:
        raise ValueError(f"version {version!r} already exists")
    for table in VERSIONED_TABLES:
        fq = _fq(catalog, table, namespace)
        branch = spark.sql(
            f"SELECT 1 FROM {fq}.refs WHERE name = '{version}' AND type = 'BRANCH'"
        ).collect()
        if branch:
            raise ValueError(f"{version!r} is a branch of {table}, not a free name")


def _ancestry(spark: SparkSession, table_fq: str, snapshot_id: int) -> list[int]:
    """`snapshot_id` and its ancestors, newest first."""
    parents = {
        r["snapshot_id"]: r["parent_id"]
        for r in spark.sql(
            f"SELECT snapshot_id, parent_id FROM {table_fq}.snapshots"
        ).collect()
    }
    chain: list[int] = []
    sid: int | None = snapshot_id
    while sid is not None and sid in parents and sid not in chain:
        chain.append(sid)
        sid = parents[sid]
    return chain


def nearest_version(
    spark: SparkSession,
    catalog: str,
    snapshots: Snapshots,
    namespace: str = NAMESPACE,
) -> str | None:
    """The most recent registered version in the history of `snapshots`.

    Walks the buildings snapshot ancestry from `snapshots` back and returns
    the first registered version whose geometries snapshot is also an
    ancestor; ties go to the latest commit. This is the default parent of a
    new version and works on any branch: a branch's first version gets the
    version it was created from.
    """
    registered = _registry(spark, catalog, namespace).collect()
    if not registered:
        return None
    by_buildings: dict[int, list[Row]] = defaultdict(list)
    for row in registered:
        by_buildings[row["buildings_snapshot_id"]].append(row)
    geometries_history = set(
        _ancestry(spark, _fq(catalog, "geometries", namespace), snapshots["geometries"])
    )
    buildings_fq = _fq(catalog, "buildings", namespace)
    for sid in _ancestry(spark, buildings_fq, snapshots["buildings"]):
        candidates = [
            r
            for r in by_buildings.get(sid, [])
            if r["geometries_snapshot_id"] in geometries_history
        ]
        if candidates:
            return max(candidates, key=lambda r: r["created_at"])["version_name"]
    return None


def version_at(
    spark: SparkSession,
    catalog: str,
    snapshots: Snapshots,
    namespace: str = NAMESPACE,
) -> str | None:
    """The latest registered version pinned exactly to `snapshots`, if any."""
    pinned = F.lit(True)
    for table in VERSIONED_TABLES:
        if table in snapshots:
            pinned &= F.col(f"{table}_snapshot_id") == snapshots[table]
    rows = (
        _registry(spark, catalog, namespace)
        .where(pinned)
        .orderBy(F.col("created_at").desc())
        .limit(1)
        .collect()
    )
    return rows[0]["version_name"] if rows else None


def table_at(
    spark: SparkSession,
    catalog: str,
    table: str,
    snapshot_id: int | None,
    namespace: str = NAMESPACE,
) -> DataFrame:
    """`table` as of an Iceberg snapshot id.

    Iceberg reads a snapshot with the schema it was written with, so
    `buildings` snapshots from before the multi-source migration lack
    `source_ref` / `source_record_id`; they are conformed to the current
    columns (`ingest_nsi.conform_buildings`). A `snapshot_id` of None reads
    as the empty table.
    """
    fq = _fq(catalog, table, namespace)
    if snapshot_id is None:
        return spark.table(fq).limit(0)
    df = spark.sql(f"SELECT * FROM {fq} VERSION AS OF {int(snapshot_id)}")
    return conform_buildings(df) if table == "buildings" else df


def read_version(
    spark: SparkSession,
    catalog: str,
    version: str,
    table: str = "buildings",
    namespace: str = NAMESPACE,
) -> DataFrame:
    """A versioned table (`VERSIONED_TABLES`) exactly as recorded for `version`.

    Equivalent to `SELECT * FROM <table> VERSION AS OF '<version>'`, but
    reads the snapshot id from the registry, so it only works for
    registered versions.
    """
    if table not in VERSIONED_TABLES:
        raise ValueError(f"{table!r} is not versioned; use one of {VERSIONED_TABLES}")
    sid = version_snapshots(spark, catalog, version, namespace)[table]
    return table_at(spark, catalog, table, sid, namespace)


# ---------------------------------------------------------------------------
# Diffs
# ---------------------------------------------------------------------------


def row_changes(
    old: DataFrame,
    new: DataFrame,
    key: str,
    columns: Iterable[str],
    keep: Iterable[str] = (),
) -> DataFrame:
    """Rows added, removed, or modified between `old` and `new`, matched on `key`.

    Returns `key`, `change_kind` (added | removed | modified),
    `changed_columns` (for modified rows: the `columns` whose values differ,
    NULL-safe), and each `keep` column taken from `new`, falling back to `old`
    for removed rows.
    """
    columns = list(columns)
    keep = list(keep)
    wanted = list(dict.fromkeys([*columns, *keep]))
    o = old.select(
        key, *[F.col(c).alias(f"old_{c}") for c in wanted], F.lit(True).alias("_old")
    )
    n = new.select(
        key, *[F.col(c).alias(f"new_{c}") for c in wanted], F.lit(True).alias("_new")
    )
    joined = o.join(n, key, "full_outer")
    changed = F.array_compact(
        F.array(
            *[
                F.when(~F.col(f"old_{c}").eqNullSafe(F.col(f"new_{c}")), F.lit(c))
                for c in columns
            ]
        )
    )
    kind = (
        F.when(F.col("_old").isNull(), F.lit("added"))
        .when(F.col("_new").isNull(), F.lit("removed"))
        .when(F.size(changed) > 0, F.lit("modified"))
    )
    return joined.select(
        key,
        kind.alias("change_kind"),
        F.when(kind == "modified", changed).alias("changed_columns"),
        *[F.coalesce(F.col(f"new_{c}"), F.col(f"old_{c}")).alias(c) for c in keep],
    ).where(F.col("change_kind").isNotNull())


def _with_computation_geom(buildings: DataFrame, geometries: DataFrame) -> DataFrame:
    """Attach `geom`: WKB of each building's selected computation geometry."""
    selected = geometries.where(
        "purpose = 'computation' AND is_selected_for_export"
    ).select("building_id", F.expr("spark_st_asbinary(geom)").alias("geom"))
    return buildings.join(selected, "building_id", "left")


def _geometries_with_wkb(geometries: DataFrame) -> DataFrame:
    return geometries.withColumn("geom_wkb", F.expr("spark_st_asbinary(geom)"))


def diff_snapshots(
    spark: SparkSession,
    catalog: str,
    old: Snapshots,
    new: Snapshots,
    namespace: str = NAMESPACE,
) -> DataFrame:
    """Building-level diff between two states given as snapshot ids per table.

    One row per building added, removed, or modified: `building_id`,
    `change_kind`, and `changed_columns` (buildings columns that differ, plus
    `geom` when the selected computation geometry changed). `ingested_at` is
    ignored.
    """

    def state(snaps: Snapshots) -> DataFrame:
        return _with_computation_geom(
            table_at(spark, catalog, "buildings", snaps["buildings"], namespace),
            table_at(spark, catalog, "geometries", snaps["geometries"], namespace),
        )

    return row_changes(state(old), state(new), "building_id", DIFF_COLUMNS)


def diff_versions(
    spark: SparkSession,
    catalog: str,
    old_version: str,
    new_version: str,
    namespace: str = NAMESPACE,
) -> DataFrame:
    """Building-level diff between two registered versions (see `diff_snapshots`)."""
    return diff_snapshots(
        spark,
        catalog,
        version_snapshots(spark, catalog, old_version, namespace),
        version_snapshots(spark, catalog, new_version, namespace),
        namespace,
    )


def _comparable(table: str, df: DataFrame) -> DataFrame:
    """Sidecar rows with the comparable columns of `SIDECAR_COMPARE_COLUMNS`."""
    if table == "source_attributes":
        return df.withColumn(
            "attributes_json", F.to_json(F.array_sort(F.map_entries("attributes")))
        )
    return df


def sidecar_changes(old: DataFrame, new: DataFrame, table: str) -> DataFrame:
    """Rows of a sidecar table added, removed, or modified between two states.

    Like `row_changes` keyed on `building_id`, plus `source_ref`. For
    `source_attributes`, `changed_columns` of a modified row lists the
    attribute keys whose value changed (or appeared, or went away) instead of
    `attributes_json`.
    """
    columns = SIDECAR_COMPARE_COLUMNS[table]
    changes = row_changes(
        _comparable(table, old),
        _comparable(table, new),
        "building_id",
        columns,
        keep=["source_ref"],
    )
    if table != "source_attributes":
        return changes
    maps = old.select("building_id", F.col("attributes").alias("_old_map")).join(
        new.select("building_id", F.col("attributes").alias("_new_map")),
        "building_id",
        "full_outer",
    )
    keys = F.array_union(
        F.coalesce(F.map_keys("_old_map"), F.array().cast("array<string>")),
        F.coalesce(F.map_keys("_new_map"), F.array().cast("array<string>")),
    )
    changed_keys = F.filter(
        keys,
        lambda k: (
            ~F.try_element_at("_old_map", k).eqNullSafe(F.try_element_at("_new_map", k))
        ),
    )
    other = F.filter(
        F.coalesce(F.col("changed_columns"), F.array().cast("array<string>")),
        lambda c: c != F.lit("attributes_json"),
    )
    return (
        changes.join(maps, "building_id", "left")
        .withColumn(
            "changed_columns",
            F.when(
                F.col("change_kind") == "modified",
                F.array_sort(F.array_union(other, F.array_sort(changed_keys))),
            ),
        )
        .drop("_old_map", "_new_map")
    )


def diff_sidecar_versions(
    spark: SparkSession,
    catalog: str,
    old_version: str,
    new_version: str,
    table: str,
    namespace: str = NAMESPACE,
) -> DataFrame:
    """Per-building diff of a sidecar table (`source_attributes` or
    `building_lineage`) between two versions (see `sidecar_changes`)."""
    if table not in SIDECAR_TABLES:
        raise ValueError(f"{table!r} is not one of {SIDECAR_TABLES}")
    return sidecar_changes(
        read_version(spark, catalog, old_version, table, namespace),
        read_version(spark, catalog, new_version, table, namespace),
        table,
    )


def diff_summary(diff: DataFrame) -> dict[str, dict[str, int]]:
    """Counts of a building-level diff: by change kind, and by changed column."""
    by_kind = {k: 0 for k in CHANGE_KINDS}
    for row in diff.groupBy("change_kind").count().collect():
        by_kind[row["change_kind"]] = row["count"]
    by_column = {
        row["column"]: row["count"]
        for row in diff.select(F.explode("changed_columns").alias("column"))
        .groupBy("column")
        .count()
        .orderBy(F.col("count").desc(), "column")
        .collect()
    }
    return {"by_kind": by_kind, "by_column": by_column}


def compare_buildings(
    spark: SparkSession,
    catalog: str,
    old_version: str,
    new_version: str,
    building_ids: Iterable[int],
    columns: Iterable[str],
    namespace: str = NAMESPACE,
) -> DataFrame:
    """Side-by-side `<column>_old` / `<column>_new` values for some buildings."""
    ids = [int(i) for i in building_ids]
    columns = list(columns)

    def side(version: str, suffix: str) -> DataFrame:
        return (
            read_version(spark, catalog, version, "buildings", namespace)
            .where(F.col("building_id").isin(ids))
            .select("building_id", *[F.col(c).alias(f"{c}_{suffix}") for c in columns])
        )

    joined = side(old_version, "old").join(
        side(new_version, "new"), "building_id", "full_outer"
    )
    ordered = [f"{c}_{s}" for c in columns for s in ("old", "new")]
    return joined.select("building_id", *ordered).orderBy("building_id")


# ---------------------------------------------------------------------------
# Sidecar tables (ADR-source-sidecar-attributes)
# ---------------------------------------------------------------------------


def root_snapshot(spark: SparkSession, table_fq: str) -> int | None:
    """The first (parentless) snapshot of a table, or None if it has none."""
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table_fq}.snapshots "
        "WHERE parent_id IS NULL ORDER BY committed_at LIMIT 1"
    ).collect()
    return rows[0]["snapshot_id"] if rows else None


def _refs(spark: SparkSession, table_fq: str, ref_type: str) -> set[str]:
    return {
        r["name"]
        for r in spark.sql(
            f"SELECT name FROM {table_fq}.refs WHERE type = '{ref_type}'"
        ).collect()
    }


def settle_sidecars(
    spark: SparkSession,
    catalog: str,
    branch: str = "main",
    version: str | None = None,
    namespace: str = NAMESPACE,
) -> None:
    """Make the sidecar tables on `branch` follow its buildings before a commit.

    Deletes `source_attributes` / `building_lineage` rows whose building is
    gone (an edit deleted it), and stamps with `version` the lineage rows
    whose `source_version_name` is NULL (a load without a version) or not a
    registered version (a load committed under another name, or never).
    Writes nothing when there is nothing to do.
    """
    buildings = _fq(catalog, "buildings", namespace, branch)
    for table in SIDECAR_TABLES:
        fq = _fq(catalog, table, namespace, branch)
        orphans = (
            f"SELECT s.building_id FROM {fq} s "
            f"LEFT ANTI JOIN {buildings} b ON s.building_id = b.building_id"
        )
        if spark.sql(orphans).limit(1).count():
            spark.sql(
                f"MERGE INTO {fq} t USING ({orphans}) o "
                "ON t.building_id = o.building_id WHEN MATCHED THEN DELETE"
            )
    if version is None:
        return
    validate_ref_name(version, "version")
    lineage = _fq(catalog, "building_lineage", namespace, branch)
    # NULL, or a name that never got registered (a load given a version
    # that was then committed under another name, or not at all).
    unstamped = (
        f"SELECT l.building_id FROM {lineage} l "
        f"LEFT ANTI JOIN {_fq(catalog, 'inventory_versions', namespace)} v "
        f"ON l.source_version_name = v.version_name "
        f"WHERE NOT (l.source_version_name <=> '{version}')"
    )
    if spark.sql(unstamped).limit(1).count():
        spark.sql(
            f"MERGE INTO {lineage} t USING ({unstamped}) s "
            "ON t.building_id = s.building_id "
            f"WHEN MATCHED THEN UPDATE SET source_version_name = '{version}'"
        )


def _backfill_nsi_lineage(
    spark: SparkSession, catalog: str, branches: list[str], namespace: str
) -> int:
    """Best-effort lineage of the NSI buildings already on each branch.

    The NSI rows were loaded by the built-in spec, so they point at its
    current hash (registered in `source_specs`). Their version is the last
    NSI release (`source_ref` NSI, or NULL for releases committed before
    multi-source support) in the branch's history that added or modified
    the building according to `inventory_changes`; buildings no later
    release changed point at the earliest such release (a root load, which
    records no change rows), else NULL (stamped by the branch's next
    commit). Other sources' specs are not in the warehouse, so their
    buildings get lineage on their next load. Returns the rows written.
    """
    from ingest_source import register_source, register_spec
    from sources import nsi_spec

    spec = nsi_spec()
    buildings_fq = _fq(catalog, "buildings", namespace)
    releases = [
        r
        for r in _registry(spark, catalog, namespace).collect()
        if r["kind"] == "release"
        and (r["source_ref"] == NSI_SOURCE_REF or r["source_ref"] is None)
    ]
    written = 0
    for branch in branches:
        head = ref_snapshot_id(spark, buildings_fq, branch)
        if head is None:
            continue
        rows = conform_buildings(
            spark.table(_fq(catalog, "buildings", namespace, branch))
        ).where(F.col("source_ref") == NSI_SOURCE_REF)
        if not rows.limit(1).count():
            continue
        if not written:
            register_spec(spark, spec, catalog, namespace)
            register_source(spark, spec, catalog, namespace)
        history = set(_ancestry(spark, buildings_fq, head))
        in_history = [r for r in releases if r["buildings_snapshot_id"] in history]
        first = (
            min(in_history, key=lambda r: r["created_at"])["version_name"]
            if in_history
            else None
        )
        names = [r["version_name"] for r in in_history]
        last_changed = (
            spark.table(_fq(catalog, "inventory_changes", namespace))
            .where(
                F.col("version_name").isin(names)
                & F.col("change_kind").isin("added", "modified")
            )
            .join(
                _registry(spark, catalog, namespace).select(
                    "version_name", "created_at"
                ),
                "version_name",
            )
            .groupBy("building_id")
            .agg(F.max_by("version_name", "created_at").alias("_last_changed"))
        )
        lineage = rows.join(last_changed, "building_id", "left").select(
            "building_id",
            "source_ref",
            "source_record_id",
            F.coalesce("_last_changed", F.lit(first).cast("string")).alias(
                "source_version_name"
            ),
            F.lit(spec.sha256).alias("spec_sha256"),
            F.current_timestamp().alias("ingested_at"),
        )
        lineage.writeTo(_fq(catalog, "building_lineage", namespace, branch)).append()
        written += lineage.count()
    return written


def prepare_sidecar_tables(
    spark: SparkSession,
    catalog: str,
    namespace: str = NAMESPACE,
    created: Iterable[str] = (),
) -> None:
    """Bring the versioned sidecar tables in line with buildings (idempotent).

    Called by `ingest_nsi.apply_schema`. Each sidecar table gets an empty
    root snapshot, every branch `buildings` has (created at the root), and a
    tag for every registered version recorded before the table existed
    (pointing at the root, with the snapshot id filled in the registry), so
    every version and branch is defined on every versioned table. When
    `building_lineage` is in `created` (just created in a warehouse that may
    hold data), the lineage of NSI buildings is backfilled.
    """
    roots: dict[str, int] = {}
    for table in SIDECAR_TABLES:
        fq = _fq(catalog, table, namespace)
        root = root_snapshot(spark, fq)
        if root is None:
            spark.sql(f"INSERT INTO {fq} SELECT * FROM {fq} WHERE false")
            root = root_snapshot(spark, fq)
        roots[table] = root

    buildings_fq = _fq(catalog, "buildings", namespace)
    branches = sorted(_refs(spark, buildings_fq, "BRANCH") - {"main"})
    for table in SIDECAR_TABLES:
        fq = _fq(catalog, table, namespace)
        existing = _refs(spark, fq, "BRANCH") | _refs(spark, fq, "TAG")
        for branch in branches:
            if branch not in existing:
                spark.sql(
                    f"ALTER TABLE {fq} CREATE BRANCH `{branch}` "
                    f"AS OF VERSION {roots[table]}"
                )

    if "building_lineage" in set(created):
        _backfill_nsi_lineage(spark, catalog, ["main", *branches], namespace)

    columns = {t: f"{t}_snapshot_id" for t in SIDECAR_TABLES}
    missing = (
        _registry(spark, catalog, namespace)
        .where(" OR ".join(f"{c} IS NULL" for c in columns.values()))
        .collect()
    )
    if not missing:
        return
    for row in missing:
        validate_ref_name(row["version_name"], "version")
        for table, column in columns.items():
            if row[column] is None:
                spark.sql(
                    f"ALTER TABLE {_fq(catalog, table, namespace)} CREATE OR REPLACE "
                    f"TAG `{row['version_name']}` AS OF VERSION {roots[table]}"
                )
    spark.sql(
        f"UPDATE {_fq(catalog, 'inventory_versions', namespace)} SET "
        + ", ".join(
            f"{column} = coalesce({column}, {roots[table]})"
            for table, column in columns.items()
        )
        + " WHERE "
        + " OR ".join(f"{c} IS NULL" for c in columns.values())
    )


# ---------------------------------------------------------------------------
# Committing versions
# ---------------------------------------------------------------------------


def commit_version(
    spark: SparkSession,
    catalog: str,
    version: str,
    *,
    editor: str,
    reason: str | None = None,
    branch: str = "main",
    kind: str = "release",
    parent: str | None = None,
    merged_version: str | None = None,
    source_ref: str | None = None,
    source_uri: str | None = None,
    source_sha256: str | None = None,
    namespace: str = NAMESPACE,
) -> dict:
    """Record the current head of `branch` as version `version`.

    `source_ref` names the source a release loaded (it must be registered in
    `sources`); leave it None for edits and merges.

    Steps: settle the sidecar tables on the branch (`settle_sidecars`:
    rows of buildings that are gone are deleted, lineage rows of loads not
    yet committed are stamped with `version`), resolve the parent (default:
    `nearest_version` of the branch head), write the per-building changes
    relative to it into `inventory_changes`, tag every versioned table, and
    finally insert the registry row. A failure before the last step leaves
    no version; re-running the commit replaces the partial tags and change
    rows.

    Returns the registry row as a dict.
    """
    validate_ref_name(version, "version")
    if branch != "main":
        validate_ref_name(branch, "branch")
    if kind not in VERSION_KINDS:
        raise ValueError(f"kind must be one of {VERSION_KINDS}, not {kind!r}")
    if not editor:
        raise ValueError("editor is required (who is committing the version?)")
    ensure_version_available(spark, catalog, version, namespace)

    if parent is not None:
        get_version(spark, catalog, parent, namespace)
    if merged_version is not None:
        get_version(spark, catalog, merged_version, namespace)
    if source_ref is not None:
        get_source(spark, catalog, source_ref, namespace)
    head_snapshots(spark, catalog, branch, namespace)  # the branch must exist
    settle_sidecars(spark, catalog, branch, version, namespace)

    head = head_snapshots(spark, catalog, branch, namespace)
    if parent is None:
        parent = nearest_version(spark, catalog, head, namespace)

    changes_fq = _fq(catalog, "inventory_changes", namespace)
    stale = spark.table(changes_fq).where(F.col("version_name") == version)
    if stale.limit(1).count():
        spark.sql(f"DELETE FROM {changes_fq} WHERE version_name = '{version}'")

    n_buildings = table_at(
        spark, catalog, "buildings", head["buildings"], namespace
    ).count()
    counts = {k: 0 for k in CHANGE_KINDS}
    if parent is None:
        # A root version: every building is new; no per-building rows.
        counts["added"] = n_buildings
    else:
        diff = diff_snapshots(
            spark,
            catalog,
            version_snapshots(spark, catalog, parent, namespace),
            head,
            namespace,
        )
        diff.select(
            F.lit(version).alias("version_name"),
            "building_id",
            "change_kind",
            "changed_columns",
        ).writeTo(changes_fq).append()
        recorded = (
            spark.table(changes_fq)
            .where(F.col("version_name") == version)
            .groupBy("change_kind")
            .count()
            .collect()
        )
        counts.update({r["change_kind"]: r["count"] for r in recorded})

    for table in VERSIONED_TABLES:
        spark.sql(
            f"ALTER TABLE {_fq(catalog, table, namespace)} "
            f"CREATE OR REPLACE TAG `{version}` AS OF VERSION {head[table]}"
        )

    spark.sql(
        f"""
        INSERT INTO {_fq(catalog, "inventory_versions", namespace)}
            (version_name, parent_version, merged_version, branch, kind,
             source_ref, source_uri, source_sha256, editor, reason,
             buildings_snapshot_id, geometries_snapshot_id,
             source_attributes_snapshot_id, building_lineage_snapshot_id,
             n_buildings, n_added, n_removed, n_changed, created_at)
        VALUES (:version_name, :parent_version, :merged_version, :branch, :kind,
                :source_ref, :source_uri, :source_sha256, :editor, :reason,
                :buildings_snapshot_id, :geometries_snapshot_id,
                :source_attributes_snapshot_id, :building_lineage_snapshot_id,
                :n_buildings, :n_added, :n_removed, :n_changed,
                current_timestamp())
        """,
        args={
            "version_name": version,
            "parent_version": parent,
            "merged_version": merged_version,
            "branch": branch,
            "kind": kind,
            "source_ref": source_ref,
            "source_uri": source_uri,
            "source_sha256": source_sha256,
            "editor": editor,
            "reason": reason,
            "buildings_snapshot_id": head["buildings"],
            "geometries_snapshot_id": head["geometries"],
            "source_attributes_snapshot_id": head["source_attributes"],
            "building_lineage_snapshot_id": head["building_lineage"],
            "n_buildings": n_buildings,
            "n_added": counts["added"],
            "n_removed": counts["removed"],
            "n_changed": counts["modified"],
        },
    )
    return get_version(spark, catalog, version, namespace).asDict()


def changes_in_version(
    spark: SparkSession, catalog: str, version: str, namespace: str = NAMESPACE
) -> DataFrame:
    """The `inventory_changes` rows of `version` (empty for a root version)."""
    get_version(spark, catalog, version, namespace)
    return (
        spark.table(_fq(catalog, "inventory_changes", namespace))
        .where(F.col("version_name") == version)
        .orderBy("building_id")
    )


def building_history(
    spark: SparkSession, catalog: str, building_id: int, namespace: str = NAMESPACE
) -> DataFrame:
    """Every version that added, removed, or modified one building, oldest first.

    Columns: version_name, branch, kind, editor, reason, created_at,
    change_kind, changed_columns. Root versions containing the building show
    as `added`.
    """
    registry = _registry(spark, catalog, namespace)
    columns = ["version_name", "branch", "kind", "editor", "reason", "created_at"]
    changed = (
        spark.table(_fq(catalog, "inventory_changes", namespace))
        .where(F.col("building_id") == int(building_id))
        .join(registry, "version_name")
        .select(*columns, "change_kind", "changed_columns")
    )
    roots_with_building = [
        root["version_name"]
        for root in registry.where(F.col("parent_version").isNull()).collect()
        if table_at(
            spark, catalog, "buildings", root["buildings_snapshot_id"], namespace
        )
        .where(F.col("building_id") == int(building_id))
        .limit(1)
        .count()
    ]
    roots = registry.where(F.col("version_name").isin(roots_with_building)).select(
        *columns,
        F.lit("added").alias("change_kind"),
        F.lit(None).cast("array<string>").alias("changed_columns"),
    )
    return changed.unionByName(roots).orderBy("created_at", "version_name")


# ---------------------------------------------------------------------------
# Branches
# ---------------------------------------------------------------------------


def create_branch(
    spark: SparkSession,
    catalog: str,
    branch: str,
    from_version: str,
    namespace: str = NAMESPACE,
) -> dict[str, int]:
    """Create Iceberg branch `branch` on every versioned table at `from_version`.

    Load into it with `ingest_nsi --branch`, or edit it directly through
    `table_name(catalog, table, namespace, branch)`, then `commit_version`
    with `branch=branch`. Returns the branch's starting snapshots.
    """
    validate_ref_name(branch, "branch")
    if branch == "main":
        raise ValueError("'main' always exists")
    snapshots = version_snapshots(spark, catalog, from_version, namespace)
    for table in VERSIONED_TABLES:
        if ref_snapshot_id(spark, _fq(catalog, table, namespace), branch) is not None:
            raise ValueError(f"{table} already has a branch or tag named {branch!r}")
    for table in VERSIONED_TABLES:
        spark.sql(
            f"ALTER TABLE {_fq(catalog, table, namespace)} "
            f"CREATE BRANCH `{branch}` AS OF VERSION {snapshots[table]}"
        )
    return snapshots


def drop_branch(
    spark: SparkSession, catalog: str, branch: str, namespace: str = NAMESPACE
) -> None:
    """Drop `branch` from every versioned table. Versions committed on it stay (tags)."""
    validate_ref_name(branch, "branch")
    if branch == "main":
        raise ValueError("cannot drop 'main'")
    for table in VERSIONED_TABLES:
        spark.sql(
            f"ALTER TABLE {_fq(catalog, table, namespace)} DROP BRANCH IF EXISTS `{branch}`"
        )


def list_branches(
    spark: SparkSession, catalog: str, namespace: str = NAMESPACE
) -> DataFrame:
    """Branches of `buildings` with their head snapshot and registered version."""
    refs = spark.sql(
        f"SELECT name AS branch, snapshot_id AS buildings_snapshot_id "
        f"FROM {_fq(catalog, 'buildings', namespace)}.refs WHERE type = 'BRANCH'"
    )
    registry = _registry(spark, catalog, namespace).select(
        "version_name", "buildings_snapshot_id", "created_at"
    )
    latest = (
        refs.join(registry, "buildings_snapshot_id", "left")
        .groupBy("branch", "buildings_snapshot_id")
        .agg(F.max_by("version_name", "created_at").alias("head_version"))
    )
    return latest.orderBy(F.col("branch") != "main", "branch")


def _merge_base(spark: SparkSession, table_fq: str, a: int, b: int) -> int | None:
    """Nearest common ancestor snapshot of snapshots `a` and `b`."""
    history_b = set(_ancestry(spark, table_fq, b))
    return next(
        (sid for sid in _ancestry(spark, table_fq, a) if sid in history_b), None
    )


def _touched_buildings(
    spark: SparkSession,
    catalog: str,
    base: Snapshots,
    head: Snapshots,
    namespace: str,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Changes from `base` to `head`: (buildings rows, geometry rows, touched ids)."""
    buildings = row_changes(
        table_at(spark, catalog, "buildings", base["buildings"], namespace),
        table_at(spark, catalog, "buildings", head["buildings"], namespace),
        "building_id",
        BUILDING_COMPARE_COLUMNS,
    )
    geometries = row_changes(
        _geometries_with_wkb(
            table_at(spark, catalog, "geometries", base["geometries"], namespace)
        ),
        _geometries_with_wkb(
            table_at(spark, catalog, "geometries", head["geometries"], namespace)
        ),
        "geometry_id",
        GEOMETRY_COMPARE_COLUMNS,
        keep=["building_id"],
    )
    touched = (
        buildings.select("building_id")
        .union(geometries.select("building_id"))
        .distinct()
    )
    return buildings, geometries, touched


def _differing_buildings(
    spark: SparkSession,
    catalog: str,
    a: Snapshots,
    b: Snapshots,
    ids: DataFrame,
    namespace: str,
) -> DataFrame:
    """Building ids from `ids` whose buildings row or geometry rows differ
    between states `a` and `b`.
    """

    def rows(snapshots: Snapshots, table: str) -> DataFrame:
        return table_at(spark, catalog, table, snapshots[table], namespace).join(
            ids, "building_id", "left_semi"
        )

    buildings = row_changes(
        rows(a, "buildings"),
        rows(b, "buildings"),
        "building_id",
        BUILDING_COMPARE_COLUMNS,
    )
    geometries = row_changes(
        _geometries_with_wkb(rows(a, "geometries")),
        _geometries_with_wkb(rows(b, "geometries")),
        "geometry_id",
        GEOMETRY_COMPARE_COLUMNS,
        keep=["building_id"],
    )
    return (
        buildings.select("building_id")
        .union(geometries.select("building_id"))
        .distinct()
    )


def _apply_row_changes(
    spark: SparkSession,
    changes: DataFrame,
    source_rows: DataFrame,
    target_fq: str,
    key: str,
    columns: Iterable[str],
) -> None:
    """Replay `changes` (from `row_changes`) onto `target_fq` with one MERGE.

    Added and modified rows are copied from `source_rows`; removed rows are
    deleted. Nothing is written when there are no changes.
    """
    if not changes.limit(1).count():
        return
    columns = list(columns)
    upserts = (
        changes.where("change_kind <> 'removed'")
        .select(key)
        .join(source_rows, key)
        .select(F.lit("upsert").alias("_op"), *columns)
    )
    deletes = changes.where("change_kind = 'removed'").select(
        F.lit("delete").alias("_op"), key
    )
    view = "_rqm_merge_rows"
    upserts.unionByName(deletes, allowMissingColumns=True).createOrReplaceTempView(view)
    others = [c for c in columns if c != key]
    try:
        spark.sql(
            f"""
            MERGE INTO {target_fq} t USING {view} s ON t.{key} = s.{key}
            WHEN MATCHED AND s._op = 'delete' THEN DELETE
            WHEN MATCHED THEN UPDATE SET {", ".join(f"{c} = s.{c}" for c in others)}
            WHEN NOT MATCHED AND s._op = 'upsert'
                THEN INSERT ({", ".join(columns)}) VALUES ({", ".join(f"s.{c}" for c in columns)})
            """
        )
    finally:
        spark.catalog.dropTempView(view)


def merge_branch(
    spark: SparkSession,
    catalog: str,
    branch: str,
    *,
    version: str,
    editor: str,
    reason: str | None = None,
    into: str = "main",
    namespace: str = NAMESPACE,
) -> dict:
    """Merge `branch` into `into` and commit the result as `version`.

    The branch head must be a committed version. If `into` has not moved
    since the branch diverged, every versioned table fast-forwards. Otherwise the
    changes since the common ancestor are compared building by building
    (attributes and every geometry row). A building changed on both sides is
    a conflict unless both sides ended in the same state; any conflict raises
    `MergeConflictError` and nothing is written. Otherwise the branch's
    changes are replayed onto `into`; sidecar rows (`source_attributes`,
    `building_lineage`) follow, except for buildings `into` changed in any
    versioned table, which keep the target's sidecar rows.

    The new version is `kind='merge'`, its parent is the previous version of
    `into`, `merged_version` is the branch version, and its changes are what
    the merge brought into `into`. Returns the registry row plus `mode`
    (fast-forward | three-way).
    """
    validate_ref_name(branch, "branch")
    ensure_version_available(spark, catalog, version, namespace)
    if branch == into:
        raise ValueError("cannot merge a branch into itself")

    source = head_snapshots(spark, catalog, branch, namespace)
    target = head_snapshots(spark, catalog, into, namespace)
    branch_version = version_at(spark, catalog, source, namespace)
    if branch_version is None:
        raise ValueError(
            f"branch {branch!r} has uncommitted changes; commit_version on it first"
        )
    parent = nearest_version(spark, catalog, target, namespace)

    base = {}
    for table in VERSIONED_TABLES:
        fq = _fq(catalog, table, namespace)
        base[table] = _merge_base(spark, fq, source[table], target[table])
        if base[table] is None:
            raise ValueError(f"{branch!r} and {into!r} share no history on {table}")
    if base == dict(source):
        raise ValueError(f"branch {branch!r} has no changes to merge into {into!r}")

    if base == dict(target):
        mode = "fast-forward"
        for table in VERSIONED_TABLES:
            spark.sql(
                f"CALL {catalog}.system.fast_forward("
                f"'{_fq(catalog, table, namespace)}', '{into}', '{branch}')"
            )
    else:
        mode = "three-way"
        src_buildings, src_geometries, src_touched = _touched_buildings(
            spark, catalog, base, source, namespace
        )
        _, _, dst_touched = _touched_buildings(spark, catalog, base, target, namespace)
        both = src_touched.join(dst_touched, "building_id")
        # Buildings both sides changed to the same state are not conflicts,
        # and the target already has them.
        conflicts = _differing_buildings(
            spark, catalog, source, target, both, namespace
        )
        n_conflicts = conflicts.count()
        if n_conflicts:
            src_diff = diff_snapshots(spark, catalog, base, source, namespace)
            dst_diff = diff_snapshots(spark, catalog, base, target, namespace)
            details = (
                conflicts.join(
                    src_diff.select(
                        "building_id",
                        F.col("change_kind").alias("source_change"),
                        F.col("changed_columns").alias("source_columns"),
                    ),
                    "building_id",
                    "left",
                )
                .join(
                    dst_diff.select(
                        "building_id",
                        F.col("change_kind").alias("target_change"),
                        F.col("changed_columns").alias("target_columns"),
                    ),
                    "building_id",
                    "left",
                )
                .orderBy("building_id")
                .limit(50)
                .collect()
            )
            raise MergeConflictError(branch, into, details, n_conflicts)

        src_buildings = src_buildings.join(both, "building_id", "left_anti")
        src_geometries = src_geometries.join(both, "building_id", "left_anti")
        _apply_row_changes(
            spark,
            src_buildings,
            table_at(spark, catalog, "buildings", source["buildings"], namespace),
            _fq(catalog, "buildings", namespace, into),
            "building_id",
            BUILDINGS_COLUMNS,
        )
        _apply_row_changes(
            spark,
            src_geometries,
            table_at(spark, catalog, "geometries", source["geometries"], namespace),
            _fq(catalog, "geometries", namespace, into),
            "geometry_id",
            GEOMETRY_COLUMNS,
        )
        # Sidecar rows follow their buildings: the branch's changes are
        # replayed except for buildings the target changed in any table
        # (theirs win, for attributes and lineage together).
        target_kept = dst_touched
        for table in SIDECAR_TABLES:
            target_kept = target_kept.union(
                sidecar_changes(
                    table_at(spark, catalog, table, base[table], namespace),
                    table_at(spark, catalog, table, target[table], namespace),
                    table,
                ).select("building_id")
            )
        target_kept = target_kept.distinct()
        for table in SIDECAR_TABLES:
            changes = sidecar_changes(
                table_at(spark, catalog, table, base[table], namespace),
                table_at(spark, catalog, table, source[table], namespace),
                table,
            ).join(target_kept, "building_id", "left_anti")
            _apply_row_changes(
                spark,
                changes,
                table_at(spark, catalog, table, source[table], namespace),
                _fq(catalog, table, namespace, into),
                "building_id",
                SIDECAR_COLUMNS[table],
            )

    info = commit_version(
        spark,
        catalog,
        version,
        editor=editor,
        reason=reason or f"merge {branch} ({branch_version}) into {into}",
        branch=into,
        kind="merge",
        parent=parent,
        merged_version=branch_version,
        namespace=namespace,
    )
    return {**info, "mode": mode}


# ---------------------------------------------------------------------------
# Consistency
# ---------------------------------------------------------------------------


def check_inventory_invariants(
    spark: SparkSession,
    catalog: str,
    snapshots: Snapshots,
    namespace: str = NAMESPACE,
) -> list[str]:
    """ADR-nsi-minimal rules 1, 2, and 7 (plus the geometry FK) and the
    ADR-multi-source identity rules at one state.

    Every `source_ref` must be non-null and registered in `sources` (read at
    the head of main; rows are never deleted). Rule 7 (`building_id =
    fd_id`) applies to NSI rows; every other row's `building_id` must equal
    the hash of (source_ref, source_record_id).

    When `snapshots` includes the sidecar tables
    (ADR-source-sidecar-attributes): each `source_attributes` and
    `building_lineage` row has a unique building_id that resolves in
    buildings with the same `source_ref`; every lineage `spec_sha256`
    resolves in `source_specs` and every non-NULL `source_version_name` in
    `inventory_versions` (both read at the head of main).
    """
    buildings = table_at(spark, catalog, "buildings", snapshots["buildings"], namespace)
    geometries = table_at(
        spark, catalog, "geometries", snapshots["geometries"], namespace
    )
    buildings.createOrReplaceTempView("_rqm_check_b")
    geometries.createOrReplaceTempView("_rqm_check_g")
    spark.table(_fq(catalog, "sources", namespace)).createOrReplaceTempView(
        "_rqm_check_s"
    )
    views = ["_rqm_check_b", "_rqm_check_g", "_rqm_check_s"]
    sidecar_checks = ""
    if all(t in snapshots for t in SIDECAR_TABLES):
        for table, view in (
            ("source_attributes", "_rqm_check_a"),
            ("building_lineage", "_rqm_check_l"),
        ):
            table_at(
                spark, catalog, table, snapshots[table], namespace
            ).createOrReplaceTempView(view)
            views.append(view)
        for view in ("source_specs", "inventory_versions"):
            spark.table(_fq(catalog, view, namespace)).createOrReplaceTempView(
                f"_rqm_check_{view}"
            )
            views.append(f"_rqm_check_{view}")
        for prefix, view in (("sidecar", "_rqm_check_a"), ("lineage", "_rqm_check_l")):
            sidecar_checks += f"""
              , (SELECT COUNT(*) - COUNT(DISTINCT building_id) FROM {view})
                  AS {prefix}_duplicate_ids
              , (SELECT COUNT(*) FROM {view} x LEFT ANTI JOIN _rqm_check_b b
                   ON x.building_id = b.building_id)
                  AS {prefix}_orphans
              , (SELECT COUNT(*) FROM {view} x JOIN _rqm_check_b b
                   ON x.building_id = b.building_id
                 WHERE NOT (x.source_ref <=> b.source_ref))
                  AS {prefix}_source_mismatch
            """
        sidecar_checks += """
              , (SELECT COUNT(*) FROM _rqm_check_l l LEFT ANTI JOIN _rqm_check_source_specs p
                   ON l.spec_sha256 = p.spec_sha256)
                  AS lineage_unknown_spec
              , (SELECT COUNT(*) FROM _rqm_check_l l LEFT ANTI JOIN _rqm_check_inventory_versions v
                   ON l.source_version_name = v.version_name
                 WHERE l.source_version_name IS NOT NULL)
                  AS lineage_unknown_version
        """
    hashed_id = source_building_id_sql("source_ref", "source_record_id")
    try:
        row = spark.sql(
            f"""
            SELECT
              (SELECT COUNT(*) FROM _rqm_check_b b LEFT ANTI JOIN _rqm_check_g g
                 ON g.building_id = b.building_id AND g.role = 'point'
                AND g.purpose = 'computation' AND g.is_selected_for_export)
                AS no_point,
              (SELECT COUNT(*) FROM (
                 SELECT building_id FROM _rqm_check_g
                 WHERE purpose = 'computation' AND is_selected_for_export
                 GROUP BY building_id HAVING COUNT(*) <> 1))
                AS multi_selected,
              (SELECT COUNT(*) - COUNT(DISTINCT building_id) FROM _rqm_check_b)
                AS duplicate_ids,
              (SELECT COUNT(*) FROM _rqm_check_b
                 WHERE source_ref = '{NSI_SOURCE_REF}' AND NOT (building_id <=> fd_id))
                AS id_not_fd_id,
              (SELECT COUNT(*) FROM _rqm_check_b
                 WHERE source_ref <> '{NSI_SOURCE_REF}'
                   AND NOT (building_id <=> {hashed_id}))
                AS id_not_hash,
              (SELECT COUNT(*) FROM _rqm_check_b WHERE source_ref IS NULL)
                AS null_source,
              (SELECT COUNT(*) FROM _rqm_check_b b LEFT ANTI JOIN _rqm_check_s s
                 ON b.source_ref = s.source_ref WHERE b.source_ref IS NOT NULL)
                AS unregistered_source,
              (SELECT COUNT(*) FROM _rqm_check_g g LEFT ANTI JOIN _rqm_check_b b
                 ON g.building_id = b.building_id)
                AS orphan_geometries
              {sidecar_checks}
            """
        ).collect()[0]
    finally:
        for view in views:
            spark.catalog.dropTempView(view)
    messages = {
        "no_point": "buildings without a selected computation point (rule 1)",
        "multi_selected": "buildings without exactly one selected computation geometry (rule 2)",
        "duplicate_ids": "duplicate building_id values (rule 7)",
        "id_not_fd_id": "NSI buildings whose building_id differs from fd_id (rule 7)",
        "id_not_hash": "non-NSI buildings whose building_id is not the hash of "
        "(source_ref, source_record_id)",
        "null_source": "buildings without a source_ref",
        "unregistered_source": "buildings whose source_ref is not in sources",
        "orphan_geometries": "geometries whose building is missing",
        "sidecar_duplicate_ids": "duplicate building_id values in source_attributes",
        "sidecar_orphans": "source_attributes rows whose building is missing",
        "sidecar_source_mismatch": "source_attributes rows whose source_ref "
        "differs from their building's",
        "lineage_duplicate_ids": "duplicate building_id values in building_lineage",
        "lineage_orphans": "building_lineage rows whose building is missing",
        "lineage_source_mismatch": "building_lineage rows whose source_ref "
        "differs from their building's",
        "lineage_unknown_spec": "building_lineage rows whose spec_sha256 is not "
        "in source_specs",
        "lineage_unknown_version": "building_lineage rows whose source_version_name "
        "is not a registered version",
    }
    present = row.asDict()
    return [f"{row[k]} {msg}" for k, msg in messages.items() if present.get(k)]


def check_consistency(
    spark: SparkSession,
    catalog: str,
    namespace: str = NAMESPACE,
    check_invariants: bool = True,
) -> list[str]:
    """Problems with the version registry, tags, change log, and data.

    Checks (ADR-versioning): every version is tagged on every versioned table at the
    recorded snapshots; no tag lacks a registry row (failed commit);
    version names are unique; parent / merged versions resolve without
    cycles; change-log counts match the registry; with `check_invariants`,
    the ADR-nsi-minimal rules and the sidecar invariants hold at every
    version (`check_inventory_invariants`); and every `source_specs`
    document hashes to its `spec_sha256` (`check_spec_registry`). Returns
    an empty list when everything is consistent.
    """
    problems: list[str] = []
    registry = _registry(spark, catalog, namespace).collect()
    by_name: dict[str, Row] = {}
    for row in registry:
        if row["version_name"] in by_name:
            problems.append(f"version {row['version_name']!r} is registered twice")
        by_name[row["version_name"]] = row

    for table in VERSIONED_TABLES:
        refs = {
            r["name"]: (r["type"], r["snapshot_id"])
            for r in spark.sql(
                f"SELECT name, type, snapshot_id FROM {_fq(catalog, table, namespace)}.refs"
            ).collect()
        }
        for name, row in by_name.items():
            expected = ("TAG", row[f"{table}_snapshot_id"])
            if refs.get(name) != expected:
                problems.append(
                    f"version {name!r}: {table} ref is {refs.get(name)}, expected {expected}"
                )
        for name, (ref_type, _) in refs.items():
            if ref_type == "TAG" and name not in by_name:
                problems.append(
                    f"tag {name!r} on {table} has no registry row (incomplete commit)"
                )

    for name, row in by_name.items():
        for col in ("parent_version", "merged_version"):
            if row[col] is not None and row[col] not in by_name:
                problems.append(
                    f"version {name!r}: {col} {row[col]!r} is not registered"
                )
        seen = {name}
        parent = row["parent_version"]
        while parent in by_name:
            if parent in seen:
                problems.append(f"version {name!r}: parent chain has a cycle")
                break
            seen.add(parent)
            parent = by_name[parent]["parent_version"]

    recorded: dict[str, dict[str, int]] = defaultdict(
        lambda: dict.fromkeys(CHANGE_KINDS, 0)
    )
    for r in (
        spark.table(_fq(catalog, "inventory_changes", namespace))
        .groupBy("version_name", "change_kind")
        .count()
        .collect()
    ):
        recorded[r["version_name"]][r["change_kind"]] = r["count"]
    for name in recorded.keys() - by_name.keys():
        problems.append(f"inventory_changes has rows for unregistered version {name!r}")
    for name, row in by_name.items():
        if row["parent_version"] is None:
            continue
        expected = {
            "added": row["n_added"],
            "removed": row["n_removed"],
            "modified": row["n_changed"],
        }
        if recorded[name] != expected:
            problems.append(
                f"version {name!r}: change log {dict(recorded[name])} != registry {expected}"
            )

    if check_invariants:
        for name, row in by_name.items():
            snapshots = {t: row[f"{t}_snapshot_id"] for t in VERSIONED_TABLES}
            problems.extend(
                f"version {name!r}: {p}"
                for p in check_inventory_invariants(
                    spark, catalog, snapshots, namespace
                )
            )
    problems.extend(check_spec_registry(spark, catalog, namespace))
    return problems


def check_spec_registry(
    spark: SparkSession, catalog: str, namespace: str = NAMESPACE
) -> list[str]:
    """Problems with `source_specs`: duplicate hashes, or a `spec_document`
    whose SHA-256 is not its `spec_sha256`."""
    row = spark.sql(
        f"""
        SELECT COUNT(*) - COUNT(DISTINCT spec_sha256) AS duplicates,
               COALESCE(SUM(CASE WHEN sha2(spec_document, 256) <> spec_sha256
                            THEN 1 ELSE 0 END), 0) AS bad_hash
        FROM {_fq(catalog, "source_specs", namespace)}
        """
    ).collect()[0]
    problems = []
    if row["duplicates"]:
        problems.append(f"source_specs: {row['duplicates']} duplicate spec_sha256 rows")
    if row["bad_hash"]:
        problems.append(
            f"source_specs: {row['bad_hash']} rows whose spec_document does not "
            "hash to spec_sha256"
        )
    return problems


# ---------------------------------------------------------------------------
# Pinned exports
# ---------------------------------------------------------------------------


def version_export_frame(
    spark: SparkSession, catalog: str, version: str, namespace: str = NAMESPACE
) -> DataFrame:
    """Flat analysis-ready table of `version`: buildings + computation geometry.

    Every buildings column plus `geom_wkb` (WKB of the selected computation
    geometry) and `geom_crs`, read at the version's snapshots.
    """
    snapshots = version_snapshots(spark, catalog, version, namespace)
    geometries = table_at(
        spark, catalog, "geometries", snapshots["geometries"], namespace
    )
    selected = geometries.where(
        "purpose = 'computation' AND is_selected_for_export"
    ).select(
        "building_id",
        F.expr("spark_st_asbinary(geom)").alias("geom_wkb"),
        F.col("crs").alias("geom_crs"),
    )
    return table_at(
        spark, catalog, "buildings", snapshots["buildings"], namespace
    ).join(selected, "building_id", "left")


def write_export_manifest(
    spark: SparkSession,
    catalog: str,
    version: str,
    out_dir: str | Path,
    *,
    extra: Mapping[str, object] | None = None,
    namespace: str = NAMESPACE,
) -> dict:
    """Write `_rqm_version.json` into the local directory `out_dir`.

    The manifest names the version, its snapshot ids, and its registry
    metadata, so the export can be traced to (and reproduced from) the exact
    inventory state. Spark and pandas parquet readers skip `_`-prefixed
    files, so the manifest can sit beside the data.
    """
    row = get_version(spark, catalog, version, namespace)
    manifest = {
        "version": version,
        "catalog": catalog,
        "namespace": namespace,
        "snapshots": {t: row[f"{t}_snapshot_id"] for t in VERSIONED_TABLES},
        "branch": row["branch"],
        "kind": row["kind"],
        "parent_version": row["parent_version"],
        "source_ref": row["source_ref"],
        "editor": row["editor"],
        "reason": row["reason"],
        "version_created_at": row["created_at"].isoformat(),
        "source_uri": row["source_uri"],
        "source_sha256": row["source_sha256"],
        "exported_at": datetime.now(UTC).isoformat(),
        **(extra or {}),
    }
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def read_export_manifest(out_dir: str | Path) -> dict:
    """The `_rqm_version.json` manifest of an export directory."""
    return json.loads((Path(out_dir) / MANIFEST_NAME).read_text())


def export_version(
    spark: SparkSession,
    catalog: str,
    version: str,
    out_dir: str | Path,
    *,
    where: str | None = None,
    sources: Iterable[str] | None = None,
    include_sidecar: bool | None = None,
    include_lineage: bool = True,
    namespace: str = NAMESPACE,
) -> dict:
    """Export `version` as one parquet file plus a pinning manifest.

    `sources` keeps only buildings of those registered `source_ref`s, and
    `where` is an optional SQL filter on the export columns (e.g.
    `"cbfips LIKE '48113%'"`). Sources are side by side, so one structure
    can appear once per source; select sources explicitly for aggregates
    that must not double count. The data is read at the version's
    snapshots, so exporting the same version again later gives the same rows
    however the inventory has changed since. Returns the manifest, which
    records the source filter and the exported row count per source.

    The sidecar tables of the exported buildings are written beside the data
    (ADR-source-sidecar-attributes), `_`-prefixed so parquet readers of the
    export directory skip them like the manifest: `_source_attributes.parquet`
    (when `include_sidecar`; default: when an exported source declares a
    sidecar), and with `include_lineage` `_building_lineage.parquet` plus
    `_source_specs.json`, the specs the lineage references.
    """
    selected = None
    if sources is not None:
        selected = sorted(set(sources))
        if not selected:
            raise ValueError("sources must name at least one source")
        for ref in selected:
            get_source(spark, catalog, ref, namespace)
    frame = version_export_frame(spark, catalog, version, namespace)
    if selected is not None:
        frame = frame.where(F.col("source_ref").isin(selected))
    if where:
        frame = frame.where(where)
    out = Path(out_dir)
    frame.orderBy("building_id").coalesce(1).write.mode("overwrite").parquet(str(out))
    exported = spark.read.parquet(str(out))
    per_source = {
        r["source_ref"]: r["count"]
        for r in exported.groupBy("source_ref").count().orderBy("source_ref").collect()
    }
    registry = {
        r["source_ref"]: r
        for r in spark.table(_fq(catalog, "sources", namespace)).collect()
    }
    sidecar_schema_by_source = {
        ref: dict(registry[ref]["sidecar_schema"])
        for ref in per_source
        if ref in registry and registry[ref]["sidecar_schema"] is not None
    }
    if include_sidecar is None:
        include_sidecar = bool(sidecar_schema_by_source)

    extra: dict[str, object] = {
        "row_count": sum(per_source.values()),
        "filter": where,
        "sources": selected,
        "rows_by_source": per_source,
    }
    ids = exported.select("building_id")
    if include_sidecar:
        attributes = read_version(
            spark, catalog, version, "source_attributes", namespace
        ).join(ids, "building_id", "left_semi")
        path = out / SIDECAR_EXPORTS["source_attributes"]
        attributes.orderBy("building_id").coalesce(1).write.mode("overwrite").parquet(
            str(path)
        )
        extra["n_source_attributes_rows"] = spark.read.parquet(str(path)).count()
        extra["sidecar_schema_by_source"] = sidecar_schema_by_source
    if include_lineage:
        lineage = read_version(
            spark, catalog, version, "building_lineage", namespace
        ).join(ids, "building_id", "left_semi")
        path = out / SIDECAR_EXPORTS["building_lineage"]
        lineage.orderBy("building_id").coalesce(1).write.mode("overwrite").parquet(
            str(path)
        )
        written = spark.read.parquet(str(path))
        referenced = written.select("spec_sha256").distinct()
        specs = (
            spark.table(_fq(catalog, "source_specs", namespace))
            .join(referenced, "spec_sha256", "left_semi")
            .orderBy("spec_sha256")
            .collect()
        )
        documents = [
            {
                "spec_sha256": s["spec_sha256"],
                "source_ref": s["source_ref"],
                "spec_uri": s["spec_uri"],
                "registered_at": s["registered_at"].isoformat(),
                "spec": json.loads(s["spec_document"]),
                "spec_document": s["spec_document"],
            }
            for s in specs
        ]
        (out / SIDECAR_EXPORTS["source_specs"]).write_text(
            json.dumps(documents, indent=2) + "\n"
        )
        extra["n_lineage_rows"] = written.count()
        extra["n_specs"] = len(documents)
        extra["specs_by_sha256"] = {
            d["spec_sha256"]: d["source_ref"] for d in documents
        }
    return write_export_manifest(
        spark, catalog, version, out, extra=extra, namespace=namespace
    )


def source_counts(
    spark: SparkSession, catalog: str, namespace: str = NAMESPACE
) -> DataFrame:
    """Buildings per source in every registered version, oldest version first.

    Columns: version_name, branch, created_at, source_ref, n_buildings.
    """
    registry = list_versions(spark, catalog, namespace).collect()
    frames = [
        table_at(spark, catalog, "buildings", row["buildings_snapshot_id"], namespace)
        .groupBy("source_ref")
        .agg(F.count("*").alias("n_buildings"))
        .select(
            F.lit(row["version_name"]).alias("version_name"),
            F.lit(row["branch"]).alias("branch"),
            F.lit(row["created_at"]).alias("created_at"),
            "source_ref",
            "n_buildings",
        )
        for row in registry
    ]
    if not frames:
        return spark.sql(
            "SELECT CAST(NULL AS STRING) AS version_name, CAST(NULL AS STRING) AS branch, "
            "CAST(NULL AS TIMESTAMP) AS created_at, CAST(NULL AS STRING) AS source_ref, "
            "CAST(0 AS BIGINT) AS n_buildings"
        ).limit(0)
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result.orderBy("created_at", "version_name", "source_ref")


# ---------------------------------------------------------------------------
# Input helpers
# ---------------------------------------------------------------------------


def file_sha256(path: str | Path) -> str:
    """SHA-256 of a file, or of a directory's files (sorted relative paths + bytes)."""
    path = Path(path)
    digest = hashlib.sha256()
    files = (
        sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]
    )
    for file in files:
        if path.is_dir():
            digest.update(file.relative_to(path).as_posix().encode() + b"\0")
        with file.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class NsiEdits:
    """What `make_modified_nsi` changed, keyed by `fd_id` (= building_id)."""

    removed: tuple[int, ...]
    attribute_edits: dict[int, tuple[str, ...]]
    moved: tuple[int, ...]
    added: tuple[int, ...]
    move_m: float
    seed: int
    changed_columns: dict[int, tuple[str, ...]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


def make_modified_nsi(
    src: str | Path,
    dst: str | Path,
    *,
    n_remove: int = 40,
    n_attr: int = 120,
    n_move: int = 30,
    n_add: int = 15,
    move_m: float = 25.0,
    seed: int = 7,
) -> NsiEdits:
    """Write a modified copy of an NSI parquet: the "v2" of a demo or test.

    Picks distinct structures with a point, `found_ht`, and `val_struct`, then:

    - removes `n_remove` of them;
    - edits `n_attr`: `found_ht` += 1 ft, and every other one also
      `val_struct` x 1.10 (a field-survey correction);
    - moves `n_move` points `move_m` meters east and north in EPSG:3857
      (`shape`, keeping Z) and updates `x` / `y`, which NSI publishes as
      WGS84 longitude / latitude;
    - adds `n_add` new structures copied from others with new `fd_id`s
      (max + 1, ...) and points shifted `2 * move_m` meters.

    Everything else, including the schema and GeoParquet metadata, is kept.
    The selection is deterministic for a given `seed`.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    import shapely

    table = pq.read_table(str(src))
    eligible = pc.and_(
        pc.and_(pc.is_valid(table["shape"]), pc.is_valid(table["found_ht"])),
        pc.is_valid(table["val_struct"]),
    )
    candidates = np.flatnonzero(eligible.to_numpy(zero_copy_only=False))
    need = n_remove + n_attr + n_move + n_add
    if len(candidates) < need:
        raise ValueError(
            f"need {need} eligible rows, the parquet has {len(candidates)}"
        )
    rng = np.random.default_rng(seed)
    picked = rng.choice(candidates, size=need, replace=False)
    rm, attr, mv, add = np.split(picked, np.cumsum([n_remove, n_attr, n_move]))
    rm, attr, mv, add = (np.sort(a) for a in (rm, attr, mv, add))
    fd_id = table["fd_id"].to_numpy()

    def set_values(tbl: pa.Table, name: str, rows: np.ndarray, values) -> pa.Table:
        column = tbl[name].combine_chunks()
        data = column.to_numpy(zero_copy_only=False).copy()
        mask = column.is_null().to_numpy(zero_copy_only=False).copy()
        data[rows] = values
        mask[rows] = False
        index = tbl.schema.get_field_index(name)
        return tbl.set_column(
            index, tbl.schema.field(name), pa.array(data, type=column.type, mask=mask)
        )

    def set_points(tbl: pa.Table, rows: np.ndarray, dxy: float) -> pa.Table:
        # NSI shapes are ISO WKB Point Z in EPSG:3857; x / y hold WGS84 lon / lat.
        points = shapely.from_wkb(tbl["shape"].take(pa.array(rows)).to_pylist())
        coords = shapely.get_coordinates(points, include_z=True)
        coords[:, :2] += dxy
        wkb = shapely.to_wkb(
            shapely.points(coords), byte_order=1, flavor="iso", output_dimension=3
        )
        shape = tbl["shape"].combine_chunks().to_pylist()
        for row, value in zip(rows, wkb):
            shape[row] = bytes(value)
        index = tbl.schema.get_field_index("shape")
        tbl = tbl.set_column(
            index, tbl.schema.field("shape"), pa.array(shape, type=tbl["shape"].type)
        )
        lon = np.degrees(coords[:, 0] / WEB_MERCATOR_RADIUS_M)
        lat = np.degrees(
            2 * np.arctan(np.exp(coords[:, 1] / WEB_MERCATOR_RADIUS_M)) - np.pi / 2
        )
        tbl = set_values(tbl, "x", rows, lon)
        return set_values(tbl, "y", rows, lat)

    attribute_edits: dict[int, tuple[str, ...]] = {}
    found_ht = table["found_ht"].to_numpy(zero_copy_only=False)
    val_struct = table["val_struct"].to_numpy(zero_copy_only=False)
    table = set_values(table, "found_ht", attr, found_ht[attr] + 1.0)
    also_value = attr[::2]
    table = set_values(
        table, "val_struct", also_value, np.round(val_struct[also_value] * 1.10, 2)
    )
    for i, row in enumerate(attr):
        attribute_edits[int(fd_id[row])] = (
            ("found_ht", "val_struct") if i % 2 == 0 else ("found_ht",)
        )

    table = set_points(table, mv, move_m)

    new_rows = table.take(pa.array(add))
    new_ids = np.arange(len(add), dtype=fd_id.dtype) + fd_id.max() + 1
    all_rows = np.arange(len(add))
    new_rows = set_values(new_rows, "fd_id", all_rows, new_ids)
    new_rows = set_points(new_rows, all_rows, 2 * move_m)

    keep = np.ones(table.num_rows, dtype=bool)
    keep[rm] = False
    table = pa.concat_tables([table.filter(pa.array(keep)), new_rows])
    pq.write_table(table, str(dst))

    moved = tuple(int(fd_id[r]) for r in mv)
    changed_columns = {
        **attribute_edits,
        **{i: ("x", "y", "geom") for i in moved},
    }
    return NsiEdits(
        removed=tuple(int(fd_id[r]) for r in rm),
        attribute_edits=attribute_edits,
        moved=moved,
        added=tuple(int(i) for i in new_ids),
        move_m=move_m,
        seed=seed,
        changed_columns=changed_columns,
    )
