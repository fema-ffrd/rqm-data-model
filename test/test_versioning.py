"""Integration tests for inventory versioning (ADR-versioning).

A 5,000-structure slice of the NSI sample is loaded as version `v1`, then a
modified copy from `make_modified_nsi` (removed, edited, moved, and added
structures) as `v2`. Later tests branch, edit, and merge on top of those.
Tests run in file order and share one scratch warehouse
(`test/warehouse-versioning-pytest`, catalog `rqmv`) in the session from
conftest.py.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

TEST_DIR = Path(__file__).resolve().parent
WAREHOUSE = TEST_DIR / "warehouse-versioning-pytest"
CATALOG = "rqmv"
N_ROWS = 5000

pytest.importorskip("pyspark")

from ingest_nsi import (  # type: ignore[import-not-found]
    NAMESPACE,
    SparkSettings,
    apply_schema,
    geometry_id_expr,
    load_buildings,
    load_geometries,
    spark_conf,
    table_name,
)
from versioning import (  # type: ignore[import-not-found]
    MergeConflictError,
    building_history,
    changes_in_version,
    check_consistency,
    commit_version,
    create_branch,
    diff_summary,
    diff_versions,
    drop_branch,
    export_version,
    file_sha256,
    get_version,
    head_snapshots,
    list_versions,
    make_modified_nsi,
    merge_branch,
    nearest_version,
    read_export_manifest,
    read_version,
    version_snapshots,
)


def add_catalog(spark, catalog: str, warehouse: Path) -> None:
    """Register an Iceberg catalog with the default (JDBC/SQLite) settings."""
    warehouse.mkdir(parents=True, exist_ok=True)
    prefix = f"spark.sql.catalog.{catalog}"
    for key, value in spark_conf(
        SparkSettings(warehouse=warehouse, catalog=catalog)
    ).items():
        if key == prefix or key.startswith(f"{prefix}."):
            spark.conf.set(key, value)


def load(v, parquet: Path, version: str | None = None, branch: str = "main", **kw):
    """Load a parquet into `branch` and, if `version` is given, commit it."""
    load_buildings(v.spark, parquet, CATALOG, branch=branch)
    load_geometries(v.spark, parquet, CATALOG, branch=branch)
    if version is None:
        return None
    kw.setdefault("editor", "pytest")
    return commit_version(
        v.spark,
        CATALOG,
        version,
        branch=branch,
        source_uri=str(parquet),
        source_sha256=file_sha256(parquet),
        **kw,
    )


def t(table: str, branch: str | None = None) -> str:
    return table_name(CATALOG, table, NAMESPACE, branch)


def main_version(v) -> str:
    return nearest_version(v.spark, CATALOG, head_snapshots(v.spark, CATALOG))


def values(v, table: str, column: str, ids) -> dict[int, object]:
    rows = v.spark.sql(
        f"SELECT building_id, {column} FROM {table} "
        f"WHERE building_id IN ({', '.join(map(str, ids))})"
    ).collect()
    return {r["building_id"]: r[column] for r in rows}


def commit_edit(v, version: str, branch: str = "main", **kw):
    kw.setdefault("editor", "pytest")
    return commit_version(v.spark, CATALOG, version, branch=branch, kind="edit", **kw)


@pytest.fixture(scope="module")
def v(spark_session, nsi_parquet: Path, tmp_path_factory):
    spark = spark_session
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    add_catalog(spark, CATALOG, WAREHOUSE)
    apply_schema(spark, CATALOG)

    work = tmp_path_factory.mktemp("versioning")
    v1 = work / "v1.parquet"
    pq.write_table(pq.read_table(nsi_parquet).slice(0, N_ROWS), v1)
    v2 = work / "v2.parquet"
    edits = make_modified_nsi(v1, v2, n_remove=20, n_attr=40, n_move=15, n_add=8)
    ctx = SimpleNamespace(spark=spark, v1=v1, v2=v2, edits=edits, work=work)
    load(ctx, v1, "v1", editor="alice", reason="initial NSI sample load")
    load(ctx, v2, "v2", editor="bob", reason="field survey corrections")

    touched = {*edits.removed, *edits.added, *edits.changed_columns}
    ids = sorted(
        r["building_id"]
        for r in read_version(spark, CATALOG, "v2").select("building_id").collect()
        if r["building_id"] not in touched
    )
    ctx.untouched = ids
    return ctx


def test_versions_are_registered_with_attribution(v) -> None:
    rows = {r["version_name"]: r for r in list_versions(v.spark, CATALOG).collect()}
    e = v.edits
    v1, v2 = rows["v1"], rows["v2"]
    assert v1["parent_version"] is None and v1["kind"] == "release"
    assert (v1["editor"], v1["reason"]) == ("alice", "initial NSI sample load")
    assert v1["n_buildings"] == v1["n_added"] == N_ROWS
    assert v1["source_sha256"] == file_sha256(v.v1)

    assert v2["parent_version"] == "v1" and v2["branch"] == "main"
    assert (v2["editor"], v2["reason"]) == ("bob", "field survey corrections")
    assert v2["n_added"] == len(e.added)
    assert v2["n_removed"] == len(e.removed)
    assert v2["n_changed"] == len(e.changed_columns)
    assert v2["n_buildings"] == N_ROWS - len(e.removed) + len(e.added)
    assert v2["created_at"] >= v1["created_at"]


def test_time_travel_reads_each_version(v) -> None:
    spark, e = v.spark, v.edits
    n_v2 = N_ROWS - len(e.removed) + len(e.added)
    assert read_version(spark, CATALOG, "v1").count() == N_ROWS
    assert read_version(spark, CATALOG, "v2").count() == n_v2
    # The tag is plain Iceberg: SQL time travel gives the same rows.
    assert (
        spark.sql(f"SELECT * FROM {t('buildings')} VERSION AS OF 'v1'").count()
        == N_ROWS
    )
    assert spark.table(t("buildings")).count() == n_v2

    gone = e.removed[0]
    assert values(v, f"{t('buildings')} VERSION AS OF 'v1'", "occtype", [gone])
    assert not values(v, t("buildings"), "occtype", [gone])
    assert not values(v, f"{t('geometries')} VERSION AS OF 'v2'", "crs", [gone])

    edited = next(iter(e.attribute_edits))
    old = values(v, f"{t('buildings')} VERSION AS OF 'v1'", "found_ht", [edited])[
        edited
    ]
    new = values(v, f"{t('buildings')} VERSION AS OF 'v2'", "found_ht", [edited])[
        edited
    ]
    assert new == pytest.approx(old + 1.0)


def test_moved_point_changes_geometry_between_versions(v) -> None:
    moved = v.edits.moved[0]

    def x(version: str) -> float:
        return v.spark.sql(
            f"""
            SELECT ST_X(ST_GeomFromWKB(spark_st_asbinary(geom))) AS x
            FROM {t("geometries")} VERSION AS OF '{version}'
            WHERE building_id = {moved}
            """
        ).collect()[0]["x"]

    assert x("v2") - x("v1") == pytest.approx(v.edits.move_m)


def test_diff_matches_the_edits(v) -> None:
    e = v.edits
    diff = diff_versions(v.spark, CATALOG, "v1", "v2")
    rows = diff.collect()
    by_kind: dict[str, set[int]] = {}
    for r in rows:
        by_kind.setdefault(r["change_kind"], set()).add(r["building_id"])
    assert by_kind == {
        "added": set(e.added),
        "removed": set(e.removed),
        "modified": set(e.changed_columns),
    }
    changed = {
        r["building_id"]: set(r["changed_columns"])
        for r in rows
        if r["change_kind"] == "modified"
    }
    assert changed == {k: set(cols) for k, cols in e.changed_columns.items()}

    summary = diff_summary(diff)
    assert summary["by_kind"] == {
        "added": len(e.added),
        "removed": len(e.removed),
        "modified": len(e.changed_columns),
    }
    assert summary["by_column"]["found_ht"] == len(e.attribute_edits)
    assert summary["by_column"]["geom"] == len(e.moved)


def test_change_log_matches_diff(v) -> None:
    logged = {
        (r["building_id"], r["change_kind"], tuple(r["changed_columns"] or ()))
        for r in changes_in_version(v.spark, CATALOG, "v2").collect()
    }
    diffed = {
        (r["building_id"], r["change_kind"], tuple(r["changed_columns"] or ()))
        for r in diff_versions(v.spark, CATALOG, "v1", "v2").collect()
    }
    assert logged == diffed
    assert changes_in_version(v.spark, CATALOG, "v1").count() == 0


def test_reloading_identical_data_rewrites_nothing(v) -> None:
    info = load(v, v.v2, "v2-reload", editor="pytest", reason="idempotence check")
    assert info["parent_version"] == "v2"
    assert (info["n_added"], info["n_removed"], info["n_changed"]) == (0, 0, 0)
    # Unchanged rows keep the timestamp of the load that last changed them.
    stamps = v.spark.sql(
        f"""
        SELECT
          (SELECT MAX(ingested_at) FROM {t("buildings")} VERSION AS OF 'v2') AS b_v2,
          (SELECT MAX(ingested_at) FROM {t("buildings")}) AS b_head,
          (SELECT MAX(ingested_at) FROM {t("geometries")} VERSION AS OF 'v2') AS g_v2,
          (SELECT MAX(ingested_at) FROM {t("geometries")}) AS g_head
        """
    ).collect()[0]
    assert stamps["b_head"] == stamps["b_v2"]
    assert stamps["g_head"] == stamps["g_v2"]


def test_building_history(v) -> None:
    e = v.edits

    def history(building_id: int) -> list[tuple]:
        return [
            (r["version_name"], r["change_kind"], tuple(r["changed_columns"] or ()))
            for r in building_history(v.spark, CATALOG, building_id).collect()
        ]

    moved = e.moved[0]
    assert history(moved) == [
        ("v1", "added", ()),
        ("v2", "modified", ("x", "y", "geom")),
    ]
    assert history(e.removed[0]) == [("v1", "added", ()), ("v2", "removed", ())]
    assert history(e.added[0]) == [("v2", "added", ())]
    row = building_history(v.spark, CATALOG, moved).collect()[-1]
    assert (row["editor"], row["reason"]) == ("bob", "field survey corrections")


def test_version_names_are_unique_and_validated(v) -> None:
    with pytest.raises(ValueError, match="already exists"):
        commit_edit(v, "v1")
    with pytest.raises(ValueError, match="invalid version name"):
        commit_edit(v, "bad name")
    with pytest.raises(KeyError, match="unknown version"):
        read_version(v.spark, CATALOG, "nope")


def test_consistency_after_releases(v) -> None:
    assert check_consistency(v.spark, CATALOG) == []


def test_branch_isolation_and_fast_forward_merge(v) -> None:
    spark = v.spark
    base = main_version(v)
    ids = v.untouched[0:5]
    create_branch(spark, CATALOG, "calib", base)
    before = values(v, t("buildings"), "found_ht", ids)

    spark.sql(
        f"UPDATE {t('buildings', 'calib')} "
        f"SET found_ht = found_ht + 2, ingested_at = current_timestamp() "
        f"WHERE building_id IN ({', '.join(map(str, ids))})"
    )
    assert values(v, t("buildings"), "found_ht", ids) == before
    info = commit_edit(v, "calib-1", "calib", editor="carol", reason="FFE calibration")
    assert (info["parent_version"], info["branch"], info["n_changed"]) == (
        base,
        "calib",
        5,
    )
    assert values(v, t("buildings"), "found_ht", ids) == before

    merged = merge_branch(spark, CATALOG, "calib", version="v3", editor="dave")
    assert merged["mode"] == "fast-forward"
    assert (merged["parent_version"], merged["merged_version"]) == (base, "calib-1")
    assert (merged["kind"], merged["n_changed"]) == ("merge", 5)
    assert head_snapshots(spark, CATALOG) == version_snapshots(
        spark, CATALOG, "calib-1"
    )
    after = values(v, t("buildings"), "found_ht", ids)
    assert after == {i: pytest.approx(before[i] + 2) for i in ids}
    drop_branch(spark, CATALOG, "calib")
    assert read_version(spark, CATALOG, "calib-1").count() > 0
    assert check_consistency(spark, CATALOG) == []


def test_three_way_merge_applies_non_conflicting_edits(v) -> None:
    spark = v.spark
    base = main_version(v)
    branch_ids, main_ids = v.untouched[5:10], v.untouched[10:15]
    removed = v.untouched[15]
    create_branch(spark, CATALOG, "scenario", base)

    spark.sql(
        f"UPDATE {t('buildings', 'scenario')} SET found_ht = 9.5 "
        f"WHERE building_id IN ({', '.join(map(str, branch_ids))})"
    )
    spark.sql(f"DELETE FROM {t('buildings', 'scenario')} WHERE building_id = {removed}")
    spark.sql(
        f"DELETE FROM {t('geometries', 'scenario')} WHERE building_id = {removed}"
    )
    commit_edit(v, "scenario-1", "scenario", reason="demolition + elevation scenario")

    spark.sql(
        f"UPDATE {t('buildings')} SET val_struct = 123456.0 "
        f"WHERE building_id IN ({', '.join(map(str, main_ids))})"
    )
    main_edit = commit_edit(v, "v4", reason="valuation update")
    assert main_edit["parent_version"] == base

    merged = merge_branch(spark, CATALOG, "scenario", version="v5", editor="erin")
    assert merged["mode"] == "three-way"
    assert (merged["parent_version"], merged["merged_version"]) == ("v4", "scenario-1")
    assert (merged["n_changed"], merged["n_removed"], merged["n_added"]) == (5, 1, 0)
    assert set(values(v, t("buildings"), "found_ht", branch_ids).values()) == {9.5}
    assert set(values(v, t("buildings"), "val_struct", main_ids).values()) == {123456.0}
    assert not values(v, t("buildings"), "fd_id", [removed])
    assert not values(v, t("geometries"), "crs", [removed])
    assert check_consistency(spark, CATALOG) == []


def test_conflicting_merge_is_refused(v) -> None:
    spark = v.spark
    building = v.untouched[16]
    create_branch(spark, CATALOG, "conflict", main_version(v))
    spark.sql(
        f"UPDATE {t('buildings', 'conflict')} SET found_ht = 1.0 WHERE building_id = {building}"
    )
    commit_edit(v, "conflict-1", "conflict")
    spark.sql(
        f"UPDATE {t('buildings')} SET val_struct = 1.0 WHERE building_id = {building}"
    )
    commit_edit(v, "v6")
    head = head_snapshots(spark, CATALOG)

    with pytest.raises(MergeConflictError) as err:
        merge_branch(spark, CATALOG, "conflict", version="v7", editor="pytest")
    assert err.value.n_conflicts == 1
    conflict = err.value.conflicts[0]
    assert conflict["building_id"] == building
    assert (conflict["source_change"], list(conflict["source_columns"])) == (
        "modified",
        ["found_ht"],
    )
    assert list(conflict["target_columns"]) == ["val_struct"]
    assert head_snapshots(spark, CATALOG) == head
    with pytest.raises(KeyError):
        get_version(spark, CATALOG, "v7")


def test_identical_changes_on_both_sides_merge_cleanly(v) -> None:
    spark = v.spark
    same, branch_only = v.untouched[20], v.untouched[21]
    create_branch(spark, CATALOG, "same-fix", main_version(v))
    spark.sql(
        f"UPDATE {t('buildings', 'same-fix')} "
        f"SET found_ht = CASE WHEN building_id = {same} THEN 4.25 ELSE 3.0 END "
        f"WHERE building_id IN ({same}, {branch_only})"
    )
    commit_edit(v, "same-fix-1", "same-fix", reason="reviewer A")
    spark.sql(f"UPDATE {t('buildings')} SET found_ht = 4.25 WHERE building_id = {same}")
    commit_edit(v, "v7", reason="reviewer B made the same fix")

    merged = merge_branch(spark, CATALOG, "same-fix", version="v8", editor="pytest")
    assert merged["mode"] == "three-way"
    assert (merged["parent_version"], merged["n_changed"]) == ("v7", 1)
    assert values(v, t("buildings"), "found_ht", [same, branch_only]) == {
        same: 4.25,
        branch_only: 3.0,
    }
    assert check_consistency(spark, CATALOG) == []


def test_merge_requires_a_committed_branch(v) -> None:
    spark = v.spark
    create_branch(spark, CATALOG, "dirty", main_version(v))
    with pytest.raises(ValueError, match="no changes"):
        merge_branch(spark, CATALOG, "dirty", version="v-noop", editor="pytest")
    spark.sql(
        f"UPDATE {t('buildings', 'dirty')} SET found_ht = 0 "
        f"WHERE building_id = {v.untouched[17]}"
    )
    with pytest.raises(ValueError, match="uncommitted"):
        merge_branch(spark, CATALOG, "dirty", version="v-dirty", editor="pytest")
    drop_branch(spark, CATALOG, "dirty")


def test_branch_reload_cascades_to_other_sources(v) -> None:
    """A full reload onto a branch deletes the footprints of removed buildings
    and keeps the footprints of buildings still present.
    """
    from pyspark.sql import functions as F

    spark = v.spark
    gone, kept = v.untouched[18], v.untouched[19]
    create_branch(spark, CATALOG, "footprints", main_version(v))
    geoms = t("geometries", "footprints")
    footprints = (
        spark.table(geoms)
        .where(F.col("building_id").isin([gone, kept]))
        .withColumn("role", F.lit("footprint"))
        .withColumn("purpose", F.lit("display"))
        .withColumn("is_selected_for_export", F.lit(False))
        .withColumn("source_ref", F.lit("MSBF-test"))
    )
    footprints = footprints.withColumn("geometry_id", geometry_id_expr())
    footprints.select(*spark.table(geoms).columns).writeTo(geoms).append()

    head = v.work / "head.parquet"
    rows = read_version(spark, CATALOG, main_version(v)).count()
    v2_table = pq.read_table(v.v2)
    pq.write_table(v2_table.filter(pc.not_equal(v2_table["fd_id"], gone)), head)
    load(v, head, branch="footprints")

    msbf = spark.sql(
        f"SELECT building_id FROM {geoms} WHERE source_ref = 'MSBF-test'"
    ).collect()
    assert [r["building_id"] for r in msbf] == [kept]
    nsi_points = spark.sql(f"SELECT COUNT(*) AS n FROM {geoms} WHERE role = 'point'")
    assert (
        nsi_points.collect()[0]["n"]
        == spark.table(t("buildings", "footprints")).count()
    )
    # The reload reverted main's later edits on the branch; main is untouched.
    assert read_version(spark, CATALOG, main_version(v)).count() == rows
    drop_branch(spark, CATALOG, "footprints")


def test_export_is_pinned_to_its_version(v, tmp_path: Path) -> None:
    spark = v.spark
    out = tmp_path / "export-v1"
    manifest = export_version(spark, CATALOG, "v1", out)
    assert manifest["version"] == "v1"
    assert manifest["snapshots"] == version_snapshots(spark, CATALOG, "v1")
    assert manifest["row_count"] == N_ROWS
    assert manifest["source_sha256"] == file_sha256(v.v1)
    assert read_export_manifest(out) == manifest

    # main has moved on several versions since v1; the export has not.
    exported = spark.read.parquet(str(out))
    assert exported.count() == N_ROWS
    v1_ids = {r["building_id"] for r in read_version(spark, CATALOG, "v1").collect()}
    assert {
        r["building_id"] for r in exported.select("building_id").collect()
    } == v1_ids
    assert exported.where("geom_wkb IS NULL").count() == 0

    filtered = export_version(
        spark, CATALOG, "v2", tmp_path / "export-res1", where="occtype LIKE 'RES1%'"
    )
    expected = read_version(spark, CATALOG, "v2").where("occtype LIKE 'RES1%'").count()
    assert (
        filtered["row_count"] == expected
        and filtered["filter"] == "occtype LIKE 'RES1%'"
    )


def test_incomplete_commit_is_detected_and_recoverable(v) -> None:
    spark = v.spark
    head = head_snapshots(spark, CATALOG)
    # A crash after tagging buildings but before registering the version.
    spark.sql(
        f"ALTER TABLE {t('buildings')} CREATE TAG `partial` AS OF VERSION {head['buildings']}"
    )
    problems = check_consistency(spark, CATALOG, check_invariants=False)
    assert problems == [
        "tag 'partial' on buildings has no registry row (incomplete commit)"
    ]
    commit_edit(v, "partial", reason="retry after a failed commit")
    assert check_consistency(spark, CATALOG) == []
