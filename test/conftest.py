"""Shared pytest fixtures: a Spark + Sedona session over an Iceberg warehouse
seeded with the NSI sample parquet.

The session is created once per pytest session (expensive to start Spark)
and the ingest runs once against a scratch warehouse under
`test/warehouse-pytest/`. Individual tests get a ready-to-query
`(spark, catalog)` tuple.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

TEST_DIR = Path(__file__).resolve().parent
REPO = TEST_DIR.parent
TOOLS = REPO / "tools"
SAMPLE_PARQUET = TEST_DIR / "data" / "nsi.parquet"
WAREHOUSE = TEST_DIR / "warehouse-pytest"
CATALOG = "rqm"

# Make tools/ importable so tests can call build_spark / apply_schema / etc.
sys.path.insert(0, str(TOOLS))


@pytest.fixture(scope="session")
def nsi_parquet() -> Path:
    if not SAMPLE_PARQUET.exists():
        pytest.skip(f"NSI sample parquet not present at {SAMPLE_PARQUET}")
    return SAMPLE_PARQUET


@pytest.fixture(scope="session")
def ingested_spark(nsi_parquet: Path):
    """Boot Spark + Sedona, create the Iceberg tables, load the NSI sample."""
    pytest.importorskip("pyspark")
    pytest.importorskip("sedona")

    from ingest_nsi import (  # type: ignore[import-not-found]
        SparkSettings,
        apply_schema,
        build_spark,
        load_buildings,
        load_geometries,
    )

    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    WAREHOUSE.mkdir(parents=True, exist_ok=True)

    spark = build_spark(SparkSettings(warehouse=WAREHOUSE, catalog=CATALOG))
    try:
        apply_schema(spark, CATALOG)
        load_buildings(spark, nsi_parquet, CATALOG)
        load_geometries(spark, nsi_parquet, CATALOG)
        yield spark, CATALOG
    finally:
        spark.stop()
