"""Integration tests for multi-source inventories (ADR-multi-source).

A slice of the NSI sample is loaded as `v1`, then synthetic sources from
`tools/synthetic_sources.py` side by side: a county assessor roll
(GeoPackage polygons in EPSG:2276) as `v2` and a field survey (CSV lon / lat)
as `v3`. Later tests reload sources, branch, merge, and export on top of
those. Tests run in file order and share one scratch warehouse
(`test/warehouse-multisource-pytest`, catalog `rqmm`) in the session from
conftest.py. The last test migrates a separate pre-feature warehouse.
"""

from __future__ import annotations

import csv
import shutil
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

TEST_DIR = Path(__file__).resolve().parent
REPO = TEST_DIR.parent
SPECS = REPO / "test" / "data" / "sources"
WAREHOUSE = TEST_DIR / "warehouse-multisource-pytest"
MIGRATION_WAREHOUSE = TEST_DIR / "warehouse-migration-pytest"
CATALOG = "rqmm"
N_NSI = 3000
N_PARCELS = 150
N_SURVEY = 80

pytest.importorskip("pyspark")

from ingest_nsi import (  # type: ignore[import-not-found]
    BUILDINGS_CAST,
    BUILDINGS_COLUMNS,
    NAMESPACE,
    SparkSettings,
    apply_schema,
    assert_no_building_id_collisions,
    geometry_id_expr,
    load_buildings,
    load_geometries,
    native_geom_sql,
    ref_snapshot_id,
    sedona_geom_sql,
    source_building_id_sql,
    spark_conf,
    table_name,
)
from ingest_source import (  # type: ignore[import-not-found]
    get_source,
    list_sources,
    load_source,
)
from pyspark.sql import functions as F
from sources import load_spec, nsi_spec, parse_spec  # type: ignore[import-not-found]
from synthetic_sources import (  # type: ignore[import-not-found]
    UNMAPPED_LANDUSE,
    generate_all,
)
from versioning import (  # type: ignore[import-not-found]
    SIDECAR_EXPORTS,
    check_consistency,
    check_inventory_invariants,
    check_spec_registry,
    commit_version,
    create_branch,
    diff_sidecar_versions,
    diff_versions,
    drop_branch,
    export_version,
    file_sha256,
    get_version,
    head_snapshots,
    merge_branch,
    read_export_manifest,
    read_version,
    source_counts,
)


def add_catalog(spark, catalog: str, warehouse: Path) -> None:
    """Register an Iceberg catalog with the default (JDBC/SQLite) settings."""
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True, exist_ok=True)
    prefix = f"spark.sql.catalog.{catalog}"
    for key, value in spark_conf(
        SparkSettings(warehouse=warehouse, catalog=catalog)
    ).items():
        if key == prefix or key.startswith(f"{prefix}."):
            spark.conf.set(key, value)


def t(table: str, branch: str | None = None, catalog: str = CATALOG) -> str:
    return table_name(catalog, table, NAMESPACE, branch)


def hashed_id(spark, source_ref: str, record_id: str) -> int:
    sql = source_building_id_sql(f"'{source_ref}'", f"'{record_id}'")
    return spark.sql(f"SELECT {sql} AS id").collect()[0]["id"]


def by_source(spark, table: str) -> dict[str, int]:
    return {
        r["source_ref"]: r["n"]
        for r in spark.sql(
            f"SELECT source_ref, COUNT(*) AS n FROM {table} GROUP BY source_ref"
        ).collect()
    }


def load_nsi(m, parquet: Path, version: str | None = None, **kw) -> dict | None:
    load_buildings(m.spark, parquet, CATALOG, version=version)
    load_geometries(m.spark, parquet, CATALOG)
    if version is None:
        return None
    return commit_version(
        m.spark,
        CATALOG,
        version,
        editor="pytest",
        source_ref="NSI",
        source_uri=str(parquet),
        source_sha256=file_sha256(parquet),
        **kw,
    )


def load(m, spec, path, version: str | None = None, branch: str = "main", **kw):
    result = load_source(m.spark, spec, path, CATALOG, branch=branch, version=version)
    if version is not None:
        kw.setdefault("editor", "pytest")
        result["version"] = commit_version(
            m.spark,
            CATALOG,
            version,
            branch=branch,
            source_ref=spec.source_ref,
            source_uri=str(path),
            source_sha256=file_sha256(path),
            **kw,
        )
    return result


@pytest.fixture(scope="module")
def m(spark_session, nsi_parquet: Path, tmp_path_factory):
    spark = spark_session
    add_catalog(spark, CATALOG, WAREHOUSE)
    apply_schema(spark, CATALOG)
    work = tmp_path_factory.mktemp("multisource")
    nsi = work / "nsi.parquet"
    pq.write_table(pq.read_table(nsi_parquet).slice(0, N_NSI), nsi)
    synthetic = generate_all(
        nsi_parquet, work / "synthetic", max_parcels=N_PARCELS, n_survey=N_SURVEY
    )
    ctx = SimpleNamespace(
        spark=spark,
        nsi=nsi,
        nsi_full=nsi_parquet,
        work=work,
        syn=synthetic,
        assessor=load_spec(SPECS / "county-assessor.yaml"),
        survey=load_spec(SPECS / "field-survey-2025.yaml"),
        riverbend=load_spec(SPECS / "project-riverbend.yaml"),
        milliman=load_spec(SPECS / "milliman-2026q1.yaml"),
    )
    load_nsi(ctx, nsi, "v1", reason="NSI sample")
    ctx.assessor_r1 = load(ctx, ctx.assessor, synthetic.assessor.release1, "v2")
    ctx.survey_r1 = load(ctx, ctx.survey, synthetic.survey, "v3")
    return ctx


def test_sources_load_side_by_side(m) -> None:
    counts = by_source(m.spark, t("buildings"))
    assert counts == {
        "NSI": N_NSI,
        "COUNTY-ASSESSOR": N_PARCELS,
        "FIELD-SURVEY-2025": N_SURVEY,
    }
    assert m.assessor_r1["n_input"] == m.assessor_r1["n_buildings"] == N_PARCELS
    assert m.assessor_r1["n_geometries"] == 2 * N_PARCELS  # point + footprint
    assert m.survey_r1["n_geometries"] == N_SURVEY
    assert m.assessor_r1["unmapped_codes"] == {
        "occtype": {UNMAPPED_LANDUSE: 3},
        "st_damcat": {UNMAPPED_LANDUSE: 3},
    }
    # Each version adds exactly one source on top of its parent.
    v2, v3 = (get_version(m.spark, CATALOG, v) for v in ("v2", "v3"))
    assert (v2["n_added"], v2["n_removed"], v2["n_changed"]) == (N_PARCELS, 0, 0)
    assert (v3["n_added"], v3["n_removed"], v3["n_changed"]) == (N_SURVEY, 0, 0)
    assert [
        get_version(m.spark, CATALOG, v)["source_ref"] for v in ("v1", "v2", "v3")
    ] == [
        "NSI",
        "COUNTY-ASSESSOR",
        "FIELD-SURVEY-2025",
    ]


def test_sources_registry(m) -> None:
    rows = {r["source_ref"]: r for r in list_sources(m.spark, CATALOG).collect()}
    assert set(rows) == {"NSI", "COUNTY-ASSESSOR", "FIELD-SURVEY-2025"}
    assert rows["NSI"]["source_kind"] == "national_inventory"
    assert rows["COUNTY-ASSESSOR"]["source_kind"] == "local_inventory"
    assert rows["COUNTY-ASSESSOR"]["spec_sha256"] == m.assessor.sha256
    assert rows["FIELD-SURVEY-2025"]["source_name"] == m.survey.source_name
    with pytest.raises(KeyError, match="unknown source"):
        get_source(m.spark, CATALOG, "NOPE")


def test_building_ids_are_disjoint_and_stable(m) -> None:
    row = m.spark.sql(
        f"""
        SELECT
          SUM(CASE WHEN source_ref = 'NSI' AND NOT (building_id <=> fd_id) THEN 1 ELSE 0 END) AS nsi_bad,
          SUM(CASE WHEN source_ref = 'NSI' AND building_id <= 0 THEN 1 ELSE 0 END) AS nsi_neg,
          SUM(CASE WHEN source_ref <> 'NSI' AND building_id >= 0 THEN 1 ELSE 0 END) AS other_pos,
          SUM(CASE WHEN source_ref <> 'NSI' AND NOT (building_id <=>
                {source_building_id_sql("source_ref", "source_record_id")}) THEN 1 ELSE 0 END) AS hash_bad,
          SUM(CASE WHEN source_ref = 'NSI' AND NOT (source_record_id <=> CAST(fd_id AS STRING)) THEN 1 ELSE 0 END) AS rec_bad,
          COUNT(*) - COUNT(DISTINCT building_id) AS dupes
        FROM {t("buildings")}
        """
    ).collect()[0]
    assert row.asDict() == dict.fromkeys(row.asDict(), 0)
    parcel = next(iter(m.syn.assessor.nsi_fd_id))
    ids = m.spark.sql(
        f"SELECT building_id FROM {t('buildings')} WHERE source_record_id = '{parcel}'"
    ).collect()
    assert [r["building_id"] for r in ids] == [
        hashed_id(m.spark, "COUNTY-ASSESSOR", parcel)
    ]


def test_assessor_fields_are_mapped(m) -> None:
    import geopandas as gpd

    raw = gpd.read_file(m.syn.assessor.release1).set_index("PARCEL_ID")
    rows = {
        r["source_record_id"]: r
        for r in m.spark.sql(
            f"SELECT * FROM {t('buildings')} WHERE source_ref = 'COUNTY-ASSESSOR'"
        ).collect()
    }
    occupancy = {"A1": "RES1", "A2": "RES2", "F1": "COM1", UNMAPPED_LANDUSE: "RES1"}
    foundation = {"SLAB": "S", "CRAWL": "C", "BASEMENT": "B", "PIER": "P"}
    for parcel, src in raw.iterrows():
        row = rows[parcel]
        if src.LANDUSE in occupancy:
            assert row["occtype"] == occupancy[src.LANDUSE], parcel
        if src.LANDUSE == UNMAPPED_LANDUSE:
            assert row["st_damcat"] is None  # no default for st_damcat
        assert row["found_type"] == foundation.get(src.FOUND), parcel
        if src.FFE_IN == src.FFE_IN:  # not NaN
            assert row["found_ht"] == pytest.approx(src.FFE_IN / 12.0, abs=1e-5)
        assert row["val_struct"] == pytest.approx(src.IMPR_VAL)
        assert row["cbfips"] == src.BLOCK_GEOID
        assert row["fd_id"] is None and row["pop2amu65"] is None


def test_polygons_get_footprint_and_point_on_surface(m) -> None:
    geoms = m.spark.sql(
        f"""
        SELECT g.role, g.purpose, g.is_selected_for_export, COUNT(*) AS n
        FROM {t("geometries")} g JOIN {t("buildings")} b ON g.building_id = b.building_id
        WHERE b.source_ref = 'COUNTY-ASSESSOR' AND g.source_ref = 'COUNTY-ASSESSOR'
        GROUP BY 1, 2, 3
        """
    ).collect()
    assert {(r.role, r.purpose, r.is_selected_for_export, r.n) for r in geoms} == {
        ("point", "computation", True, N_PARCELS),
        ("footprint", "display", False, N_PARCELS),
    }
    contained = m.spark.sql(
        f"""
        SELECT COUNT(*) AS n,
               SUM(CASE WHEN ST_Contains(f.g, p.g) THEN 1 ELSE 0 END) AS inside,
               SUM(CASE WHEN ST_GeometryType(f.g) = 'ST_Polygon' THEN 1 ELSE 0 END) AS polygons
        FROM (SELECT building_id, {sedona_geom_sql()} AS g FROM {t("geometries")}
              WHERE source_ref = 'COUNTY-ASSESSOR' AND role = 'footprint') f
        JOIN (SELECT building_id, {sedona_geom_sql()} AS g FROM {t("geometries")}
              WHERE source_ref = 'COUNTY-ASSESSOR' AND role = 'point') p
          ON f.building_id = p.building_id
        """
    ).collect()[0]
    assert contained["n"] == contained["inside"] == contained["polygons"] == N_PARCELS


def test_crs_transform_matches_the_source_structures(m) -> None:
    """Footprints were drawn in EPSG:2276 around NSI points; after the
    transform to EPSG:3857 their point on surface lands on those points, and
    x / y are WGS84 lon / lat like NSI's.
    """
    import shapely

    nsi = pq.read_table(m.nsi_full, columns=["fd_id", "shape", "x", "y"]).to_pydict()
    by_fd = {
        fd: (shapely.from_wkb(s), x, y)
        for fd, s, x, y in zip(nsi["fd_id"], nsi["shape"], nsi["x"], nsi["y"])
    }
    rows = m.spark.sql(
        f"""
        SELECT b.source_record_id, b.x, b.y, spark_st_asbinary(g.geom) AS wkb
        FROM {t("buildings")} b JOIN {t("geometries")} g ON g.building_id = b.building_id
        WHERE b.source_ref = 'COUNTY-ASSESSOR' AND g.role = 'point'
        """
    ).collect()
    assert len(rows) == N_PARCELS
    for r in rows:
        point, x, y = by_fd[m.syn.assessor.nsi_fd_id[r["source_record_id"]]]
        assert shapely.from_wkb(bytes(r["wkb"])).distance(point) < 0.5  # meters
        assert (r["x"], r["y"]) == (
            pytest.approx(x, abs=1e-5),
            pytest.approx(y, abs=1e-5),
        )


def test_sparse_survey_mapping(m) -> None:
    with open(m.syn.survey, newline="") as fh:
        raw = {r["SURVEY_ID"]: r for r in csv.DictReader(fh)}
    rows = m.spark.sql(
        f"SELECT * FROM {t('buildings')} WHERE source_ref = 'FIELD-SURVEY-2025'"
    ).collect()
    assert len(rows) == N_SURVEY
    for row in rows:
        src = raw[row["source_record_id"]]
        assert row["found_ht"] == pytest.approx(float(src["FFH_FT"]), abs=1e-5)
        assert (row["x"], row["y"]) == (
            pytest.approx(float(src["LON"]), abs=1e-9),
            pytest.approx(float(src["LAT"]), abs=1e-9),
        )
        assert row["occtype"] in ("RES1", "COM1", "IND2")
    mapped = {"found_ht", "found_type", "num_story", "occtype", "st_damcat", "cbfips"}
    unmapped = [c for c in BUILDINGS_CAST if c not in mapped | {"x", "y"}]
    nulls = m.spark.sql(
        f"SELECT {', '.join(f'COUNT({c}) AS {c}' for c in unmapped)} "
        f"FROM {t('buildings')} WHERE source_ref = 'FIELD-SURVEY-2025'"
    ).collect()[0]
    assert nulls.asDict() == dict.fromkeys(unmapped, 0)


def test_scoped_reload_touches_only_its_source(m) -> None:
    truth = m.syn.assessor
    before = by_source(m.spark, t("buildings"))
    result = load(m, m.assessor, truth.release2, "v4", reason="assessor release 2")
    info = result["version"]
    assert (info["parent_version"], info["source_ref"]) == ("v3", "COUNTY-ASSESSOR")
    assert (info["n_added"], info["n_removed"], info["n_changed"]) == (
        len(truth.added),
        len(truth.removed),
        len(truth.edited),
    )
    diff = diff_versions(m.spark, CATALOG, "v3", "v4").collect()
    ids = {
        hashed_id(m.spark, "COUNTY-ASSESSOR", p): p
        for p in (*truth.added, *truth.removed, *truth.edited)
    }
    assert {r["building_id"] for r in diff} == set(ids)
    for r in diff:
        parcel = ids[r["building_id"]]
        expected = (
            "added"
            if parcel in truth.added
            else "removed"
            if parcel in truth.removed
            else "modified"
        )
        assert r["change_kind"] == expected
        if expected == "modified":
            assert "found_ht" in r["changed_columns"]
            assert set(r["changed_columns"]) <= {"found_ht", "val_struct"}

    after = by_source(m.spark, t("buildings"))
    assert after["NSI"] == before["NSI"]
    assert after["FIELD-SURVEY-2025"] == before["FIELD-SURVEY-2025"]
    assert after["COUNTY-ASSESSOR"] == N_PARCELS - len(truth.removed) + len(truth.added)
    gone = [hashed_id(m.spark, "COUNTY-ASSESSOR", p) for p in truth.removed]
    left = m.spark.sql(
        f"SELECT COUNT(*) AS n FROM {t('geometries')} "
        f"WHERE building_id IN ({', '.join(map(str, gone))})"
    ).collect()[0]["n"]
    assert left == 0


def test_nsi_reload_leaves_other_sources_alone(m) -> None:
    trimmed = m.work / "nsi-trimmed.parquet"
    pq.write_table(pq.read_table(m.nsi).slice(0, N_NSI - 10), trimmed)
    before = by_source(m.spark, t("buildings"))
    before_geoms = by_source(m.spark, t("geometries"))
    info = load_nsi(m, trimmed, "v5", reason="NSI release without 10 structures")
    assert (info["n_added"], info["n_removed"], info["n_changed"]) == (0, 10, 0)
    after = by_source(m.spark, t("buildings"))
    assert after == {**before, "NSI": N_NSI - 10}
    geoms = by_source(m.spark, t("geometries"))
    for ref in ("COUNTY-ASSESSOR", "FIELD-SURVEY-2025"):
        assert geoms[ref] == before_geoms[ref]


def test_project_inventory_on_a_branch(m) -> None:
    spark = m.spark
    create_branch(spark, CATALOG, "riverbend", "v5")
    result = load(m, m.riverbend, m.syn.riverbend, "riverbend-1", branch="riverbend")
    assert result["n_buildings"] == len(m.syn.riverbend_lots)
    assert "PROJECT-RIVERBEND" not in by_source(spark, t("buildings"))
    on_branch = by_source(spark, t("buildings", "riverbend"))
    assert on_branch["PROJECT-RIVERBEND"] == len(m.syn.riverbend_lots)

    merged = merge_branch(spark, CATALOG, "riverbend", version="v6", editor="pytest")
    assert merged["mode"] == "fast-forward"
    assert (merged["n_added"], merged["merged_version"]) == (
        len(m.syn.riverbend_lots),
        "riverbend-1",
    )
    assert merged["source_ref"] is None  # merges are not source releases
    assert by_source(spark, t("buildings"))["PROJECT-RIVERBEND"] == len(
        m.syn.riverbend_lots
    )
    drop_branch(spark, CATALOG, "riverbend")
    counts = {
        (r["version_name"], r["source_ref"]): r["n_buildings"]
        for r in source_counts(spark, CATALOG).collect()
    }
    assert counts[("v1", "NSI")] == N_NSI
    assert ("v5", "PROJECT-RIVERBEND") not in counts
    assert counts[("v6", "PROJECT-RIVERBEND")] == len(m.syn.riverbend_lots)


def test_export_selects_sources(m, tmp_path: Path) -> None:
    out = tmp_path / "export"
    manifest = export_version(
        m.spark, CATALOG, "v6", out, sources=["NSI", "COUNTY-ASSESSOR"]
    )
    assert manifest["sources"] == ["COUNTY-ASSESSOR", "NSI"]
    v6 = by_source(m.spark, f"{t('buildings')} VERSION AS OF 'v6'")
    assert manifest["rows_by_source"] == {
        "COUNTY-ASSESSOR": v6["COUNTY-ASSESSOR"],
        "NSI": v6["NSI"],
    }
    assert manifest["row_count"] == v6["COUNTY-ASSESSOR"] + v6["NSI"]
    assert read_export_manifest(out) == manifest
    exported = m.spark.read.parquet(str(out))
    assert {
        r["source_ref"] for r in exported.select("source_ref").distinct().collect()
    } == {
        "NSI",
        "COUNTY-ASSESSOR",
    }
    assert exported.where("geom_wkb IS NULL").count() == 0
    with pytest.raises(KeyError, match="unknown source"):
        export_version(m.spark, CATALOG, "v6", tmp_path / "bad", sources=["NOPE"])


def test_consistency_across_sources(m) -> None:
    assert check_consistency(m.spark, CATALOG) == []


def test_invariants_catch_identity_violations(m) -> None:
    spark = m.spark
    create_branch(spark, CATALOG, "broken", "v6")
    spark.sql(
        f"UPDATE {t('buildings', 'broken')} SET source_record_id = 'tampered' "
        "WHERE source_ref = 'FIELD-SURVEY-2025' AND source_record_id = 'FS25-0001'"
    )
    spark.sql(
        f"UPDATE {t('buildings', 'broken')} SET source_ref = 'UNREGISTERED' "
        "WHERE source_ref = 'FIELD-SURVEY-2025' AND source_record_id = 'FS25-0002'"
    )
    problems = check_inventory_invariants(
        spark, CATALOG, head_snapshots(spark, CATALOG, "broken")
    )
    drop_branch(spark, CATALOG, "broken")
    assert problems == [
        (
            "2 non-NSI buildings whose building_id is not the hash of "
            "(source_ref, source_record_id)"
        ),
        "1 buildings whose source_ref is not in sources",
        "1 building_lineage rows whose source_ref differs from their building's",
    ]


def test_bad_inputs_are_refused_before_writing(m, tmp_path: Path) -> None:
    spark = m.spark
    head = head_snapshots(spark, CATALOG)
    path = tmp_path / "bad.csv"
    path.write_text(
        "ID,LON,LAT,USE\n"
        "a,-96.4,32.9,House\n"
        "a,-96.4,32.9,House\n"
        ",-96.4,32.9,House\n"
        "c,,,House\n"
    )
    base = {
        "source_ref": "BAD-CSV",
        "source_name": "Bad CSV",
        "source_kind": "other",
        "input": {
            "format": "csv",
            "crs": "EPSG:4326",
            "geometry": {"kind": "point", "lon": "LON", "lat": "LAT"},
        },
        "id": {"column": "ID"},
    }
    spec = parse_spec(
        {**base, "fields": {"occtype": {"from": "USE", "map": {"Shop": "COM1"}}}}
    )
    with pytest.raises(ValueError) as err:
        load_source(spark, spec, path, CATALOG)
    message = str(err.value)
    for expected in ("NULL id", "duplicate ids", "no geometry", "NULL occtype"):
        assert expected in message
    missing = parse_spec(
        {**base, "fields": {"occtype": {"default": "RES1"}, "found_ht": "FFH"}}
    )
    with pytest.raises(ValueError, match=r"columns \['FFH'\]"):
        load_source(spark, missing, path, CATALOG)
    assert head_snapshots(spark, CATALOG) == head
    with pytest.raises(KeyError, match="unknown source"):
        get_source(spark, CATALOG, "BAD-CSV")


def test_text_numbers_cast_and_bad_values_fail_before_writing(
    m, tmp_path: Path
) -> None:
    """CSV text such as "2.0" casts to integer columns; text that does not
    cast fails in validation, before the source is registered or written.
    """
    spark = m.spark
    base = {
        "source_name": "Text numbers",
        "source_kind": "other",
        "input": {
            "format": "csv",
            "crs": "EPSG:4326",
            "geometry": {"kind": "point", "lon": "LON", "lat": "LAT"},
        },
        "id": {"column": "ID"},
        "fields": {"occtype": {"default": "RES1"}, "num_story": "STORIES"},
    }
    good = tmp_path / "good.csv"
    good.write_text("ID,LON,LAT,STORIES\na,-96.4,32.9,2.0\nb,-96.41,32.91, 3 \n")
    create_branch(spark, CATALOG, "text-csv", "v6")
    try:
        spec = parse_spec({**base, "source_ref": "TEXT-CSV"})
        load_source(spark, spec, good, CATALOG, branch="text-csv")
        stories = {
            r["source_record_id"]: r["num_story"]
            for r in spark.sql(
                f"SELECT source_record_id, num_story FROM {t('buildings', 'text-csv')} "
                "WHERE source_ref = 'TEXT-CSV'"
            ).collect()
        }
        assert stories == {"a": 2, "b": 3}

        bad = tmp_path / "bad-stories.csv"
        bad.write_text("ID,LON,LAT,STORIES\na,-96.4,32.9,two\n")
        head = head_snapshots(spark, CATALOG, "text-csv")
        spec = parse_spec({**base, "source_ref": "TEXT-CSV-BAD"})
        with pytest.raises(Exception, match="CAST_INVALID_INPUT|cannot be cast"):
            load_source(spark, spec, bad, CATALOG, branch="text-csv")
        assert head_snapshots(spark, CATALOG, "text-csv") == head
        with pytest.raises(KeyError):
            get_source(spark, CATALOG, "TEXT-CSV-BAD")
    finally:
        drop_branch(spark, CATALOG, "text-csv")


def test_wkb_passthrough_still_checks_geometry_type(m, tmp_path: Path) -> None:
    """EPSG:3857 WKB points skip Sedona, but non-NSI sources still get the
    rule 3 geometry-type check.
    """
    import pyarrow as pa
    import shapely

    path = tmp_path / "lines.parquet"
    pq.write_table(
        pa.table(
            {
                "ID": ["a", "b"],
                "geometry": shapely.to_wkb(
                    [shapely.Point(0, 0), shapely.LineString([(0, 0), (1, 1)])]
                ).tolist(),
            }
        ),
        path,
    )
    spec = parse_spec(
        {
            "source_ref": "WKB-LINES",
            "source_name": "Lines",
            "source_kind": "other",
            "input": {
                "format": "parquet",
                "crs": "EPSG:3857",
                "geometry": {"kind": "point", "column": "geometry"},
            },
            "id": {"column": "ID"},
            "fields": {"occtype": {"default": "RES1"}},
        }
    )
    with pytest.raises(ValueError, match="1 geometries are not ST_Point"):
        load_source(m.spark, spec, path, CATALOG)


def test_gpkg_pyogrio_fallback_matches_sedona(m, monkeypatch) -> None:
    """Without Sedona's GeoPackage reader the layer is staged through
    pyogrio and maps to the same rows and geometries.
    """
    from ingest_source import POINT_WKB, prepare_input  # type: ignore[import-not-found]
    from pyspark.sql.readwriter import DataFrameReader

    def rows():
        frame = prepare_input(m.spark, m.assessor, m.syn.assessor.release1)
        return {
            r["building_id"]: (r["occtype"], r["found_ht"], bytes(r[POINT_WKB]))
            for r in frame.collect()
        }

    with_sedona = rows()
    original = DataFrameReader.format

    def no_geopackage(self, source):
        # An unregistered source makes Spark raise DATA_SOURCE_NOT_FOUND on load.
        if source == "geopackage":
            source = "rqm_missing_geopackage"
        return original(self, source)

    monkeypatch.setattr(DataFrameReader, "format", no_geopackage)
    fallback = rows()
    assert fallback.keys() == with_sedona.keys() and len(fallback) == N_PARCELS
    import shapely

    for building_id, (occtype, found_ht, wkb) in fallback.items():
        expected = with_sedona[building_id]
        assert (occtype, found_ht) == expected[:2]
        assert shapely.from_wkb(wkb).distance(shapely.from_wkb(expected[2])) < 1e-6


def test_building_id_collision_guard(m) -> None:
    def rows(record: str):
        return m.spark.sql(
            f"SELECT CAST(-1 AS BIGINT) AS building_id, 'X' AS source_ref, "
            f"'{record}' AS source_record_id"
        )

    assert_no_building_id_collisions(rows("a"), rows("a"))
    with pytest.raises(ValueError, match="collision"):
        assert_no_building_id_collisions(rows("a"), rows("b"))


# ---------------------------------------------------------------------------
# Sidecar attributes and lineage (ADR-source-sidecar-attributes)
# ---------------------------------------------------------------------------

MILLIMAN = "MILLIMAN-2026Q1"


def sidecar_rows(spark, table: str, source_ref: str, version: str | None = None):
    """{source_record_id: row} of a sidecar table for one source."""
    fq = t(table) if version is None else f"{t(table)} VERSION AS OF '{version}'"
    return {
        r["source_record_id"]: r
        for r in spark.sql(
            f"SELECT * FROM {fq} WHERE source_ref = '{source_ref}'"
        ).collect()
    }


def test_existing_sources_have_lineage(m) -> None:
    """Every building has one lineage row naming the spec and the version of
    the last load that changed it: a reload moves only the buildings it
    added or modified (assessor release 2), and a reload that only removes
    buildings (NSI v5) moves none. A source without a sidecar block writes
    no source_attributes rows.
    """
    spark = m.spark
    truth = m.syn.assessor
    moved = {*truth.added, *truth.edited}
    lineage = {
        (r["source_ref"], r["source_version_name"], r["spec_sha256"]): r["n"]
        for r in spark.sql(
            f"SELECT source_ref, source_version_name, spec_sha256, COUNT(*) AS n "
            f"FROM {t('building_lineage')} GROUP BY 1, 2, 3"
        ).collect()
    }
    buildings = by_source(spark, t("buildings"))
    assert lineage == {
        ("NSI", "v1", nsi_spec().sha256): buildings["NSI"],
        ("COUNTY-ASSESSOR", "v2", m.assessor.sha256): buildings["COUNTY-ASSESSOR"]
        - len(moved),
        ("COUNTY-ASSESSOR", "v4", m.assessor.sha256): len(moved),
        ("FIELD-SURVEY-2025", "v3", m.survey.sha256): buildings["FIELD-SURVEY-2025"],
        ("PROJECT-RIVERBEND", "riverbend-1", m.riverbend.sha256): buildings[
            "PROJECT-RIVERBEND"
        ],
    }
    assessor_v4 = {
        r["source_record_id"]
        for r in sidecar_rows(spark, "building_lineage", "COUNTY-ASSESSOR").values()
        if r["source_version_name"] == "v4"
    }
    assert assessor_v4 == moved
    assert spark.table(t("source_attributes")).count() == 0
    specs = {r["spec_sha256"]: r for r in spark.table(t("source_specs")).collect()}
    assert {
        nsi_spec().sha256,
        m.assessor.sha256,
        m.survey.sha256,
        m.riverbend.sha256,
    } <= set(specs)
    assessor = specs[m.assessor.sha256]
    assert assessor["spec_uri"] == "test/data/sources/county-assessor.yaml"
    assert assessor["spec_document"] == m.assessor.document
    assert check_spec_registry(spark, CATALOG) == []


def test_load_with_sidecar(m) -> None:
    spark, truth = m.spark, m.syn.milliman
    result = load(m, m.milliman, truth.release1, "v7", reason="Milliman portfolio")
    raw = pq.read_table(truth.release1).to_pylist()
    assert result["n_buildings"] == result["n_source_attributes"] == len(raw)
    assert result["n_lineage"] == len(raw)

    rows = sidecar_rows(spark, "source_attributes", MILLIMAN)
    assert set(rows) == {r["LOC_ID"] for r in raw}
    for src in raw:
        row = rows[src["LOC_ID"]]
        assert row["building_id"] == hashed_id(spark, MILLIMAN, src["LOC_ID"])
        expected = {
            "policy_term": src["POLICY_TERM"],
            "agent_id": src["AGENT_ID"],
            "premium_usd": src["PREMIUM"],
            "cope_construction": str(src["COPE_CC"]),
            "inception_date": src["INCEPTION_DATE"].isoformat(),
        }
        if src["DEDUCTIBLE_PCT"] is not None:  # NULL values leave the key out
            expected["deductible_pct"] = src["DEDUCTIBLE_PCT"]
        attributes = dict(row["attributes"])
        assert attributes.keys() == expected.keys(), src["LOC_ID"]
        for key, value in expected.items():
            if isinstance(value, float):
                assert float(attributes[key]) == pytest.approx(value), key
            else:
                assert attributes[key] == value, key

    source = get_source(spark, CATALOG, MILLIMAN)
    assert dict(source["sidecar_schema"]) == m.milliman.sidecar_schema
    assert source["spec_sha256"] == m.milliman.sha256
    assert get_source(spark, CATALOG, "NSI")["sidecar_schema"] is None
    lineage = sidecar_rows(spark, "building_lineage", MILLIMAN)
    assert {(r["source_version_name"], r["spec_sha256"]) for r in lineage.values()} == {
        ("v7", m.milliman.sha256)
    }
    # Typed reads cast with the declared type.
    total = spark.sql(
        f"SELECT SUM(CAST(attributes['premium_usd'] AS DOUBLE)) AS premium "
        f"FROM {t('source_attributes')} WHERE source_ref = '{MILLIMAN}'"
    ).collect()[0]["premium"]
    assert total == pytest.approx(sum(r["PREMIUM"] for r in raw))


def test_sidecar_reload_is_scoped(m) -> None:
    spark, truth = m.spark, m.syn.milliman
    other_lineage = (
        f"SELECT building_id, source_version_name, spec_sha256, ingested_at "
        f"FROM {t('building_lineage')} WHERE source_ref <> '{MILLIMAN}' "
        "ORDER BY building_id"
    )
    others = spark.sql(other_lineage).collect()
    before = sidecar_rows(spark, "source_attributes", MILLIMAN)
    load(m, m.milliman, truth.release2, "v8", reason="Milliman revised delivery")
    after = sidecar_rows(spark, "source_attributes", MILLIMAN)
    lineage = sidecar_rows(spark, "building_lineage", MILLIMAN)

    expected_ids = (set(before) - set(truth.removed)) | set(truth.added)
    assert set(after) == set(lineage) == expected_ids
    for loc in truth.renewed:
        assert float(after[loc]["attributes"]["premium_usd"]) == pytest.approx(
            round(float(before[loc]["attributes"]["premium_usd"]) * 1.05, 2)
        )
    for loc in truth.deductible_dropped:
        assert "deductible_pct" in before[loc]["attributes"]
        assert "deductible_pct" not in after[loc]["attributes"]  # key gone, row kept
        assert "premium_usd" in after[loc]["attributes"]
    untouched = set(before) - {
        *truth.removed,
        *truth.renewed,
        *truth.deductible_dropped,
    }
    for loc in untouched:
        assert after[loc]["ingested_at"] == before[loc]["ingested_at"], loc
    # Renewals and dropped deductibles are sidecar-only changes: only the
    # added locations' buildings changed, so only their lineage moved.
    assert {loc: r["source_version_name"] for loc, r in lineage.items()} == {
        loc: "v8" if loc in truth.added else "v7" for loc in expected_ids
    }
    # Other sources' lineage rows were not rewritten.
    assert spark.sql(other_lineage).collect() == others

    diff = {
        r["building_id"]: r
        for r in diff_sidecar_versions(
            spark, CATALOG, "v7", "v8", "source_attributes"
        ).collect()
    }
    ids = {
        hashed_id(spark, MILLIMAN, loc): loc
        for loc in (
            *truth.removed,
            *truth.renewed,
            *truth.deductible_dropped,
            *truth.added,
        )
    }
    assert set(diff) == set(ids)
    for building_id, row in diff.items():
        loc = ids[building_id]
        if loc in truth.removed:
            assert row["change_kind"] == "removed"
        elif loc in truth.added:
            assert row["change_kind"] == "added"
        else:
            expected = set()
            if loc in truth.renewed:
                expected.add("premium_usd")
            if loc in truth.deductible_dropped:
                expected.add("deductible_pct")
            assert set(row["changed_columns"]) == expected
            assert row["change_kind"] == "modified"


def test_reload_with_new_spec_registers_it(m, tmp_path: Path) -> None:
    import yaml

    spark, truth = m.spark, m.syn.milliman
    data = yaml.safe_load((SPECS / "milliman-2026q1.yaml").read_text())
    del data["sidecar"]["fields"]["deductible_pct"]
    # Condos map to another occupancy; every other building keeps its values.
    data["fields"]["occtype"]["map"]["CONDO"] = "RES3B"
    path = tmp_path / "milliman-v2.yaml"
    path.write_text(yaml.safe_dump(data))
    spec = load_spec(path)
    assert spec.sha256 != m.milliman.sha256

    specs_before = spark.table(t("source_specs")).count()
    others = spark.sql(
        f"SELECT * FROM {t('building_lineage')} WHERE source_ref <> '{MILLIMAN}' "
        "ORDER BY building_id"
    ).collect()
    before = sidecar_rows(spark, "building_lineage", MILLIMAN)
    load(m, spec, truth.release2, "v9", reason="spec: condo occupancy, no deductible")
    assert spark.table(t("source_specs")).count() == specs_before + 1
    lineage = sidecar_rows(spark, "building_lineage", MILLIMAN)
    condos = {
        r["LOC_ID"]
        for r in pq.read_table(truth.release2).to_pylist()
        if r["OCCUPANCY"] == "CONDO"
    }
    assert condos and condos < set(lineage)
    # The new spec reaches only the buildings whose values it changed.
    for loc, row in lineage.items():
        if loc in condos:
            assert (row["spec_sha256"], row["source_version_name"]) == (
                spec.sha256,
                "v9",
            )
        else:
            assert row["spec_sha256"] == m.milliman.sha256
            assert row["source_version_name"] == before[loc]["source_version_name"]
            assert row["ingested_at"] == before[loc]["ingested_at"]  # not rewritten
    assert (
        spark.sql(
            f"SELECT * FROM {t('building_lineage')} WHERE source_ref <> '{MILLIMAN}' "
            "ORDER BY building_id"
        ).collect()
        == others
    )
    assert get_source(spark, CATALOG, MILLIMAN)["spec_sha256"] == spec.sha256
    assert "deductible_pct" not in dict(
        get_source(spark, CATALOG, MILLIMAN)["sidecar_schema"]
    )
    attributes = sidecar_rows(spark, "source_attributes", MILLIMAN)
    assert attributes and all(
        "deductible_pct" not in r["attributes"] for r in attributes.values()
    )
    # Reloading with the same spec registers nothing new.
    create_branch(spark, CATALOG, "same-spec", "v9")
    try:
        load(m, spec, truth.release2, branch="same-spec")
    finally:
        drop_branch(spark, CATALOG, "same-spec")
    assert spark.table(t("source_specs")).count() == specs_before + 1

    # Each version reads its own sidecar shape.
    def keys(version: str) -> set[str]:
        return {
            k
            for r in read_version(spark, CATALOG, version, "source_attributes")
            .where(F.col("source_ref") == MILLIMAN)
            .collect()
            for k in r["attributes"]
        }

    assert "deductible_pct" in keys("v8")
    assert "deductible_pct" not in keys("v9")
    assert read_version(spark, CATALOG, "v1", "source_attributes").count() == 0
    assert read_version(spark, CATALOG, "v8", "building_lineage").where(
        f"source_ref = '{MILLIMAN}' AND spec_sha256 = '{m.milliman.sha256}'"
    ).count() == len(lineage)
    m.milliman_condos = condos


def test_dropping_the_sidecar_block_deletes_the_rows(m, tmp_path: Path) -> None:
    """A spec without a sidecar block deletes the source's sidecar rows (on a
    branch, so main keeps them); lineage stays one row per building."""
    import yaml

    spark = m.spark
    data = yaml.safe_load((SPECS / "milliman-2026q1.yaml").read_text())
    del data["sidecar"]
    path = tmp_path / "milliman-nosidecar.yaml"
    path.write_text(yaml.safe_dump(data))
    create_branch(spark, CATALOG, "no-sidecar", "v9")
    try:
        result = load(
            m, load_spec(path), m.syn.milliman.release2, "no-sidecar-1",
            branch="no-sidecar",
        )  # fmt: skip
        assert result["n_source_attributes"] == 0
        assert result["n_lineage"] == result["n_buildings"]
        assert (
            spark.table(t("source_attributes", "no-sidecar"))
            .where(F.col("source_ref") == MILLIMAN)
            .count()
            == 0
        )
        assert sidecar_rows(spark, "source_attributes", MILLIMAN)  # main untouched
    finally:
        drop_branch(spark, CATALOG, "no-sidecar")


def test_sidecar_invariants_catch_violations(m) -> None:
    spark = m.spark
    create_branch(spark, CATALOG, "sidecar-broken", "v9")
    a, lin = (
        t("source_attributes", "sidecar-broken"),
        t("building_lineage", "sidecar-broken"),
    )
    try:
        spark.sql(
            f"INSERT INTO {a} VALUES (CAST(-42 AS BIGINT), '{MILLIMAN}', 'ghost', "
            "map('policy_term', '12M'), current_timestamp())"
        )
        spark.sql(
            f"INSERT INTO {lin} VALUES (CAST(-42 AS BIGINT), '{MILLIMAN}', 'ghost', "
            f"'v9', '{m.milliman.sha256}', current_timestamp())"
        )
        ids = [
            r["building_id"]
            for r in spark.table(lin)
            .where(F.col("source_ref") == MILLIMAN)
            .orderBy("building_id")
            .limit(3)
            .collect()
        ]
        spark.sql(f"UPDATE {a} SET source_ref = 'NSI' WHERE building_id = {ids[0]}")
        spark.sql(f"UPDATE {lin} SET source_ref = 'NSI' WHERE building_id = {ids[0]}")
        spark.sql(
            f"UPDATE {lin} SET spec_sha256 = 'deadbeef' WHERE building_id = {ids[1]}"
        )
        spark.sql(
            f"UPDATE {lin} SET source_version_name = 'nope' WHERE building_id = {ids[2]}"
        )
        problems = check_inventory_invariants(
            spark, CATALOG, head_snapshots(spark, CATALOG, "sidecar-broken")
        )
    finally:
        drop_branch(spark, CATALOG, "sidecar-broken")
    assert problems == [
        "1 source_attributes rows whose building is missing",
        "1 source_attributes rows whose source_ref differs from their building's",
        "1 building_lineage rows whose building is missing",
        "1 building_lineage rows whose source_ref differs from their building's",
        "1 building_lineage rows whose spec_sha256 is not in source_specs",
        "1 building_lineage rows whose source_version_name is not a registered version",
    ]

    specs = t("source_specs")
    spark.sql(
        f"INSERT INTO {specs} VALUES ('0000', '{MILLIMAN}', NULL, '{{}}', current_timestamp())"
    )
    try:
        assert check_spec_registry(spark, CATALOG) == [
            "source_specs: 1 rows whose spec_document does not hash to spec_sha256"
        ]
    finally:
        spark.sql(f"DELETE FROM {specs} WHERE spec_sha256 = '0000'")


def test_edits_and_merges_carry_sidecar_rows(m) -> None:
    """A commit drops sidecar rows of buildings an edit deleted, and a
    three-way merge brings the branch's sidecar changes into main."""
    spark, truth = m.spark, m.syn.milliman
    condos = m.milliman_condos
    at_v9 = sidecar_rows(spark, "building_lineage", MILLIMAN, "v9")
    create_branch(spark, CATALOG, "mlm-review", "v9")
    gone_loc = min(truth.renewed)
    gone = hashed_id(spark, MILLIMAN, gone_loc)
    # The original spec restores the deductibles and the condo occupancy.
    # The load names a version that is never committed; the commit below
    # stamps its own name instead.
    load_source(
        spark, m.milliman, truth.release2, CATALOG, branch="mlm-review",
        version="never-committed",
    )  # fmt: skip
    for table in ("buildings", "geometries"):
        spark.sql(f"DELETE FROM {t(table, 'mlm-review')} WHERE building_id = {gone}")
    info = commit_version(
        spark,
        CATALOG,
        "mlm-review-1",
        editor="pytest",
        branch="mlm-review",
        kind="edit",
    )
    assert info["n_removed"] == 1
    for table in ("source_attributes", "building_lineage"):
        rows = read_version(spark, CATALOG, "mlm-review-1", table)
        assert rows.where(F.col("building_id") == gone).count() == 0
    stamped = read_version(spark, CATALOG, "mlm-review-1", "building_lineage").where(
        F.col("source_ref") == MILLIMAN
    )
    # The reload changed the condos; their lineage was stamped by the commit
    # that recorded it, and the other buildings kept theirs.
    stamped = {r["source_record_id"]: r for r in stamped.collect()}
    assert set(stamped) == set(at_v9) - {gone_loc}
    for loc, row in stamped.items():
        if loc in condos:
            assert (row["source_version_name"], row["spec_sha256"]) == (
                "mlm-review-1", m.milliman.sha256,
            )  # fmt: skip
        else:
            assert row["source_version_name"] == at_v9[loc]["source_version_name"]

    nsi_building = spark.sql(
        f"SELECT building_id FROM {t('buildings')} WHERE source_ref = 'NSI' LIMIT 1"
    ).collect()[0]["building_id"]
    spark.sql(
        f"UPDATE {t('buildings')} SET found_ht = 7.5 WHERE building_id = {nsi_building}"
    )
    # A sidecar-only correction on main: the merge keeps main's rows for it.
    kept_loc = min(truth.deductible_dropped)
    kept = hashed_id(spark, MILLIMAN, kept_loc)
    spark.sql(
        f"UPDATE {t('source_attributes')} SET attributes = map('policy_term', 'corrected') "
        f"WHERE building_id = {kept}"
    )
    commit_version(spark, CATALOG, "v10", editor="pytest", kind="edit")

    merged = merge_branch(spark, CATALOG, "mlm-review", version="v11", editor="pytest")
    assert merged["mode"] == "three-way"
    drop_branch(spark, CATALOG, "mlm-review")
    attributes = sidecar_rows(spark, "source_attributes", MILLIMAN)
    review = sidecar_rows(spark, "source_attributes", MILLIMAN, "mlm-review-1")
    assert dict(attributes.pop(kept_loc)["attributes"]) == {"policy_term": "corrected"}
    review.pop(kept_loc)
    assert {k: dict(r["attributes"]) for k, r in attributes.items()} == {
        k: dict(r["attributes"]) for k, r in review.items()
    }
    assert any("deductible_pct" in r["attributes"] for r in attributes.values())
    lineage = sidecar_rows(spark, "building_lineage", MILLIMAN)
    # main changed kept_loc's sidecar, so main keeps its lineage row too.
    assert (
        lineage.pop(kept_loc)["source_version_name"]
        == at_v9[kept_loc]["source_version_name"]
    )
    assert {loc: r["source_version_name"] for loc, r in lineage.items()} == {
        loc: r["source_version_name"] for loc, r in stamped.items() if loc != kept_loc
    }
    assert check_consistency(spark, CATALOG) == []


def test_export_carries_sidecar_and_lineage(m, tmp_path: Path) -> None:
    import json

    spark = m.spark
    out = tmp_path / "export-sidecar"
    manifest = export_version(spark, CATALOG, "v8", out, sources=[MILLIMAN, "NSI"])
    v8 = by_source(spark, f"{t('buildings')} VERSION AS OF 'v8'")
    # The sidecar files are hidden from readers of the export directory.
    assert spark.read.parquet(str(out)).count() == v8[MILLIMAN] + v8["NSI"]
    attributes = spark.read.parquet(str(out / SIDECAR_EXPORTS["source_attributes"]))
    lineage = spark.read.parquet(str(out / SIDECAR_EXPORTS["building_lineage"]))
    assert manifest["n_source_attributes_rows"] == attributes.count() == v8[MILLIMAN]
    assert manifest["n_lineage_rows"] == lineage.count() == v8[MILLIMAN] + v8["NSI"]
    expected = read_version(spark, CATALOG, "v8", "source_attributes")
    assert sorted(
        (r["building_id"], sorted(r["attributes"].items()))
        for r in attributes.collect()
    ) == sorted(
        (r["building_id"], sorted(r["attributes"].items())) for r in expected.collect()
    )
    specs = json.loads((out / SIDECAR_EXPORTS["source_specs"]).read_text())
    assert (
        {s["spec_sha256"] for s in specs}
        == {
            r["spec_sha256"] for r in lineage.select("spec_sha256").distinct().collect()
        }
        == {m.milliman.sha256, nsi_spec().sha256}
    )
    assert manifest["n_specs"] == 2
    assert manifest["specs_by_sha256"] == {
        m.milliman.sha256: MILLIMAN,
        nsi_spec().sha256: "NSI",
    }
    assert manifest["sidecar_schema_by_source"] == {MILLIMAN: m.milliman.sidecar_schema}
    assert all(json.loads(s["spec_document"]) == s["spec"] for s in specs)

    plain = export_version(
        spark, CATALOG, "v8", tmp_path / "export-nsi", sources=["NSI"]
    )
    assert "n_source_attributes_rows" not in plain  # NSI declares no sidecar
    assert plain["n_lineage_rows"] == v8["NSI"]
    bare = export_version(
        spark, CATALOG, "v8", tmp_path / "export-bare", include_lineage=False,
        include_sidecar=False,
    )  # fmt: skip
    assert "n_lineage_rows" not in bare
    assert not (tmp_path / "export-bare" / SIDECAR_EXPORTS["source_specs"]).exists()


def test_pre_feature_warehouse_is_migrated(m, tmp_path: Path) -> None:
    """A warehouse written before multi-source support (no source columns,
    no `sources` table) migrates in place: old versions read as NSI, diffs
    across the migration are empty, and new sources load beside NSI.
    """
    spark, cat = m.spark, "rqmmig"
    add_catalog(spark, cat, MIGRATION_WAREHOUSE)
    apply_schema(spark, cat)
    b, g, v = (
        t("buildings", catalog=cat),
        t("geometries", catalog=cat),
        t("inventory_versions", catalog=cat),
    )
    # Recreate the pre-feature schema and write it the way the old loader did.
    for column in ("source_ref", "source_record_id"):
        spark.sql(f"ALTER TABLE {b} DROP COLUMN {column}")
    for column in (
        "source_ref",
        "source_attributes_snapshot_id",
        "building_lineage_snapshot_id",
    ):
        spark.sql(f"ALTER TABLE {v} DROP COLUMN {column}")
    for table in ("sources", "source_specs", "source_attributes", "building_lineage"):
        spark.sql(f"DROP TABLE {t(table, catalog=cat)} PURGE")
    parquet = tmp_path / "nsi-500.parquet"
    pq.write_table(pq.read_table(m.nsi).slice(0, 500), parquet)
    raw = spark.read.parquet(str(parquet))
    old_columns = [
        c for c in BUILDINGS_COLUMNS if c not in ("source_ref", "source_record_id")
    ]

    raw.select(
        *[F.col(c).cast(dt).alias(c) for c, dt in BUILDINGS_CAST.items()],
        F.col("fd_id").cast("bigint").alias("building_id"),
        F.current_timestamp().alias("ingested_at"),
    ).select(*old_columns).writeTo(b).append()
    geoms = raw.select(
        F.col("fd_id").cast("bigint").alias("building_id"),
        F.lit("point").alias("role"),
        F.lit("computation").alias("purpose"),
        F.lit(True).alias("is_selected_for_export"),
        F.lit(None).cast("string").alias("alt_group_key"),
        F.concat(F.lit("NSI-"), F.coalesce("source", F.lit("unknown"))).alias(
            "source_ref"
        ),
        F.lit("EPSG:3857").alias("crs"),
        F.expr(native_geom_sql("CAST(shape AS BINARY)")).alias("geom"),
        F.current_timestamp().alias("ingested_at"),
    )
    geoms.select(geometry_id_expr().alias("geometry_id"), *geoms.columns).writeTo(
        g
    ).append()
    snaps = {"buildings": ref_snapshot_id(spark, b, "main"),
             "geometries": ref_snapshot_id(spark, g, "main")}  # fmt: skip
    for table, fq in (("buildings", b), ("geometries", g)):
        spark.sql(f"ALTER TABLE {fq} CREATE TAG `v0` AS OF VERSION {snaps[table]}")
    spark.sql(
        f"""
        INSERT INTO {v} (version_name, parent_version, merged_version, branch, kind,
            source_uri, source_sha256, editor, reason, buildings_snapshot_id,
            geometries_snapshot_id, n_buildings, n_added, n_removed, n_changed, created_at)
        VALUES ('v0', NULL, NULL, 'main', 'release', NULL, NULL, 'legacy', 'pre-feature',
            {snaps["buildings"]}, {snaps["geometries"]}, 500, 500, 0, 0, current_timestamp())
        """
    )
    for table, fq in (("buildings", b), ("geometries", g)):
        spark.sql(
            f"ALTER TABLE {fq} CREATE BRANCH `legacy` AS OF VERSION {snaps[table]}"
        )
    # A second legacy release on main that corrected one building.
    fixed = spark.sql(
        f"SELECT MIN(building_id) AS id FROM {b} WHERE found_ht IS NOT NULL"
    ).collect()[0]["id"]
    spark.sql(f"UPDATE {b} SET found_ht = found_ht + 1 WHERE building_id = {fixed}")
    fixed_snap = ref_snapshot_id(spark, b, "main")
    for fq, sid in ((b, fixed_snap), (g, snaps["geometries"])):
        spark.sql(f"ALTER TABLE {fq} CREATE TAG `v0b` AS OF VERSION {sid}")
    spark.sql(
        f"""
        INSERT INTO {v} (version_name, parent_version, merged_version, branch, kind,
            source_uri, source_sha256, editor, reason, buildings_snapshot_id,
            geometries_snapshot_id, n_buildings, n_added, n_removed, n_changed, created_at)
        VALUES ('v0b', 'v0', NULL, 'main', 'release', NULL, NULL, 'legacy', 'correction',
            {fixed_snap}, {snaps["geometries"]}, 500, 0, 0, 1,
            current_timestamp() + INTERVAL 1 SECOND)
        """
    )
    spark.sql(
        f"INSERT INTO {t('inventory_changes', catalog=cat)} "
        f"VALUES ('v0b', {fixed}, 'modified', array('found_ht'))"
    )

    assert apply_schema(spark, cat) == {
        "buildings": ["source_ref", "source_record_id"],
        "inventory_versions": [
            "source_ref",
            "source_attributes_snapshot_id",
            "building_lineage_snapshot_id",
        ],
    }
    assert apply_schema(spark, cat) == {}  # idempotent
    assert spark.table(b).columns == BUILDINGS_COLUMNS
    assert get_source(spark, cat, "NSI")["source_kind"] == "national_inventory"
    # The NSI spec is registered and the NSI buildings get best-effort
    # lineage on every branch: the last release in the branch's history that
    # changed them (inventory_changes), else the root load. v0 is tagged on
    # the sidecar tables at their empty root.
    nsi = nsi_spec()
    assert [
        r["spec_sha256"] for r in spark.table(t("source_specs", catalog=cat)).collect()
    ] == [nsi.sha256]
    assert get_source(spark, cat, "NSI")["spec_sha256"] == nsi.sha256

    def lineage_versions(branch: str = "main") -> dict[int, str]:
        rows = spark.table(t("building_lineage", branch, cat)).collect()
        assert {(r["source_ref"], r["spec_sha256"]) for r in rows} == {
            ("NSI", nsi.sha256)
        }
        return {r["building_id"]: r["source_version_name"] for r in rows}

    main_lineage = lineage_versions()
    assert len(main_lineage) == 500 and main_lineage.pop(fixed) == "v0b"
    assert set(main_lineage.values()) == {"v0"}
    assert set(lineage_versions("legacy").values()) == {
        "v0"
    }  # v0b is not in its history
    v0_row = get_version(spark, cat, "v0")
    for table in ("source_attributes", "building_lineage"):
        assert v0_row[f"{table}_snapshot_id"] is not None
        assert read_version(spark, cat, "v0", table).count() == 0
    v0 = read_version(spark, cat, "v0")
    assert v0.columns == BUILDINGS_COLUMNS
    assert (
        v0.where(
            "source_ref = 'NSI' AND source_record_id = CAST(fd_id AS STRING)"
        ).count()
        == 500
    )
    assert check_consistency(spark, cat) == []

    # Reloading the NSI rows reverts the correction and rewrites only that
    # building, whose lineage then points at the reload.
    lineage_before = spark.table(t("building_lineage", catalog=cat)).collect()
    load_buildings(spark, parquet, cat)
    load_geometries(spark, parquet, cat)
    info = commit_version(spark, cat, "v1", editor="pytest", source_ref="NSI")
    assert (
        info["parent_version"],
        info["n_added"],
        info["n_removed"],
        info["n_changed"],
    ) == (
        "v0b",
        0,
        0,
        1,
    )
    assert spark.table(b).where("source_ref IS NULL").count() == 499
    after = {
        r["building_id"]: r
        for r in spark.table(t("building_lineage", catalog=cat)).collect()
    }
    assert after[fixed]["source_version_name"] == "v1"
    for row in lineage_before:
        if row["building_id"] != fixed:
            assert after[row["building_id"]] == row  # not rewritten

    load_source(spark, m.survey, m.syn.survey, cat)
    info = commit_version(
        spark, cat, "v2", editor="pytest", source_ref=m.survey.source_ref
    )
    assert (info["n_added"], info["n_removed"], info["n_changed"]) == (N_SURVEY, 0, 0)
    assert diff_versions(spark, cat, "v0", "v1").count() == 0  # v1 reverted v0b
    assert check_consistency(spark, cat) == []
    # Branches created before the migration load sources like any other.
    load_source(spark, m.survey, m.syn.survey, cat, branch="legacy", version="legacy-1")
    commit_version(spark, cat, "legacy-1", editor="pytest", branch="legacy")
    assert (
        read_version(spark, cat, "legacy-1", "building_lineage")
        .where(F.col("source_ref") == m.survey.source_ref)
        .count()
        == N_SURVEY
    )
    assert check_consistency(spark, cat) == []
