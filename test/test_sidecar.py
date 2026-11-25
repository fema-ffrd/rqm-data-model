"""Reader helpers for sidecar attributes and lineage (tools/sidecar.py;
ADR-source-sidecar-attributes).

A small NSI slice (`s1`) and the synthetic Milliman portfolio (`s2`, a
source with a `sidecar:` block) are loaded into their own scratch warehouse
(`test/warehouse-sidecar-pytest`, catalog `rqms`) in the session from
conftest.py.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

TEST_DIR = Path(__file__).resolve().parent
SPECS = TEST_DIR / "data" / "sources"
WAREHOUSE = TEST_DIR / "warehouse-sidecar-pytest"
CATALOG = "rqms"
N_NSI = 300
MILLIMAN = "MILLIMAN-2026Q1"

pytest.importorskip("pyspark")

from ingest_nsi import (  # type: ignore[import-not-found]
    SparkSettings,
    apply_schema,
    load_buildings,
    load_geometries,
    source_building_id_sql,
    spark_conf,
)
from ingest_source import load_source  # type: ignore[import-not-found]
from sidecar import (  # type: ignore[import-not-found]
    buildings_with_sidecar,
    lineage_for,
    source_of,
)
from sources import load_spec, nsi_spec  # type: ignore[import-not-found]
from synthetic_sources import make_milliman_portfolio  # type: ignore[import-not-found]
from versioning import commit_version  # type: ignore[import-not-found]


@pytest.fixture(scope="module")
def s(spark_session, nsi_parquet: Path, tmp_path_factory):
    spark = spark_session
    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    WAREHOUSE.mkdir(parents=True)
    prefix = f"spark.sql.catalog.{CATALOG}"
    for key, value in spark_conf(
        SparkSettings(warehouse=WAREHOUSE, catalog=CATALOG)
    ).items():
        if key == prefix or key.startswith(f"{prefix}."):
            spark.conf.set(key, value)
    apply_schema(spark, CATALOG)

    work = tmp_path_factory.mktemp("sidecar")
    nsi = work / "nsi.parquet"
    pq.write_table(pq.read_table(nsi_parquet).slice(0, N_NSI), nsi)
    load_buildings(spark, nsi, CATALOG, version="s1")
    load_geometries(spark, nsi, CATALOG)
    commit_version(spark, CATALOG, "s1", editor="pytest", source_ref="NSI")

    milliman = make_milliman_portfolio(nsi_parquet, work)
    spec = load_spec(SPECS / "milliman-2026q1.yaml")
    load_source(spark, spec, milliman.release1, CATALOG)  # stamped by the commit
    commit_version(spark, CATALOG, "s2", editor="pytest", source_ref=MILLIMAN)
    raw = {r["LOC_ID"]: r for r in pq.read_table(milliman.release1).to_pylist()}
    return SimpleNamespace(spark=spark, spec=spec, raw=raw)


def milliman_id(s, loc_id: str) -> int:
    sql = source_building_id_sql(f"'{MILLIMAN}'", f"'{loc_id}'")
    return s.spark.sql(f"SELECT {sql} AS id").collect()[0]["id"]


def nsi_id(s) -> int:
    return s.spark.sql(
        f"SELECT MIN(building_id) AS id FROM {CATALOG}.inventory.buildings "
        "WHERE source_ref = 'NSI'"
    ).collect()[0]["id"]


def test_buildings_with_sidecar(s) -> None:
    rows = buildings_with_sidecar(s.spark, CATALOG).collect()
    by_source: dict[str, list] = {}
    for r in rows:
        by_source.setdefault(r["source_ref"], []).append(r)
    assert len(by_source["NSI"]) == N_NSI
    assert all(r["attributes"] is None for r in by_source["NSI"])  # no sidecar
    assert len(by_source[MILLIMAN]) == len(s.raw)
    for r in by_source[MILLIMAN]:
        src = s.raw[r["source_record_id"]]
        assert r["attributes"]["agent_id"] == src["AGENT_ID"]
        assert r["val_struct"] == pytest.approx(src["BLDG_RPL_COST"])

    only = buildings_with_sidecar(s.spark, CATALOG, "s1", MILLIMAN)
    assert only.count() == 0  # s1 predates the Milliman load
    assert buildings_with_sidecar(s.spark, CATALOG, "s2", MILLIMAN).count() == len(
        s.raw
    )


def test_lineage_for(s) -> None:
    loc = min(s.raw)
    row = lineage_for(s.spark, CATALOG, milliman_id(s, loc))
    assert (row["source_ref"], row["source_record_id"]) == (MILLIMAN, loc)
    assert (row["source_version_name"], row["spec_sha256"]) == ("s2", s.spec.sha256)
    assert row["spec_document"] == s.spec.document
    assert row["spec"]["fields"]["val_struct"] == {"from": "BLDG_RPL_COST"}
    assert row["spec_uri"] == "test/data/sources/milliman-2026q1.yaml"

    nsi = lineage_for(s.spark, CATALOG, nsi_id(s), version="s1")
    assert (nsi["source_version_name"], nsi["spec_sha256"]) == ("s1", nsi_spec().sha256)
    assert lineage_for(s.spark, CATALOG, milliman_id(s, loc), version="s1") is None
    assert lineage_for(s.spark, CATALOG, -1) is None


def test_source_of(s) -> None:
    loc = sorted(s.raw)[1]
    building = milliman_id(s, loc)
    found = source_of(s.spark, CATALOG, building, "val_struct")
    assert found["rule"] == {"from": "BLDG_RPL_COST"}
    assert found["source_columns"] == ["BLDG_RPL_COST"]
    assert (found["source_record_id"], found["spec_sha256"]) == (loc, s.spec.sha256)

    occtype = source_of(s.spark, CATALOG, building, "occtype")
    assert occtype["source_columns"] == ["OCCUPANCY"]
    assert occtype["rule"]["default"] == "COM1" and "map" in occtype["rule"]
    assert source_of(s.spark, CATALOG, building, "x")["rule"] is None
    assert "computation point" in source_of(s.spark, CATALOG, building, "x")["note"]
    assert source_of(s.spark, CATALOG, building, "fd_id")["note"].startswith("unmapped")
    record = source_of(s.spark, CATALOG, building, "source_record_id")
    assert "LOC_ID" in record["note"]

    nsi = source_of(s.spark, CATALOG, nsi_id(s), "val_struct", version="s2")
    assert nsi["source_columns"] == ["val_struct"]  # NSI's identity mapping
    with pytest.raises(ValueError, match="not a buildings column"):
        source_of(s.spark, CATALOG, building, "premium_usd")
