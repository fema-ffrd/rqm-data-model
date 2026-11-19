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
from sources import load_spec, parse_spec  # type: ignore[import-not-found]
from synthetic_sources import (  # type: ignore[import-not-found]
    UNMAPPED_LANDUSE,
    generate_all,
)
from versioning import (  # type: ignore[import-not-found]
    check_consistency,
    check_inventory_invariants,
    commit_version,
    create_branch,
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
    load_buildings(m.spark, parquet, CATALOG)
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
    result = load_source(m.spark, spec, path, CATALOG, branch=branch)
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
        "2 non-NSI buildings whose building_id is not the hash of "
        "(source_ref, source_record_id)",
        "1 buildings whose source_ref is not in sources",
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
        if source == "geopackage":
            raise RuntimeError("geopackage reader unavailable")
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
    spark.sql(f"ALTER TABLE {v} DROP COLUMN source_ref")
    spark.sql(f"DROP TABLE {t('sources', catalog=cat)} PURGE")
    parquet = tmp_path / "nsi-500.parquet"
    pq.write_table(pq.read_table(m.nsi).slice(0, 500), parquet)
    raw = spark.read.parquet(str(parquet))
    old_columns = [
        c for c in BUILDINGS_COLUMNS if c not in ("source_ref", "source_record_id")
    ]
    from pyspark.sql import functions as F

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
    snaps = {
        name: head_snapshots(spark, cat)[name] for name in ("buildings", "geometries")
    }
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

    assert apply_schema(spark, cat) == {
        "buildings": ["source_ref", "source_record_id"],
        "inventory_versions": ["source_ref"],
    }
    assert apply_schema(spark, cat) == {}  # idempotent
    assert spark.table(b).columns == BUILDINGS_COLUMNS
    assert get_source(spark, cat, "NSI")["source_kind"] == "national_inventory"
    v0 = read_version(spark, cat, "v0")
    assert v0.columns == BUILDINGS_COLUMNS
    assert (
        v0.where(
            "source_ref = 'NSI' AND source_record_id = CAST(fd_id AS STRING)"
        ).count()
        == 500
    )
    assert check_consistency(spark, cat) == []

    # Reloading the same NSI rows changes nothing and rewrites nothing.
    load_buildings(spark, parquet, cat)
    load_geometries(spark, parquet, cat)
    info = commit_version(spark, cat, "v1", editor="pytest", source_ref="NSI")
    assert (
        info["parent_version"],
        info["n_added"],
        info["n_removed"],
        info["n_changed"],
    ) == (
        "v0",
        0,
        0,
        0,
    )
    assert spark.table(b).where("source_ref IS NULL").count() == 500

    load_source(spark, m.survey, m.syn.survey, cat)
    info = commit_version(
        spark, cat, "v2", editor="pytest", source_ref=m.survey.source_ref
    )
    assert (info["n_added"], info["n_removed"], info["n_changed"]) == (N_SURVEY, 0, 0)
    assert diff_versions(spark, cat, "v0", "v1").count() == 0
    assert check_consistency(spark, cat) == []
