"""Shared pytest fixtures: a Spark + Sedona session over an Iceberg warehouse
seeded with the NSI sample parquet.

The session is created once per pytest session (expensive to start Spark)
and the ingest runs once against a scratch warehouse under
`test/warehouse-pytest/` (default JDBC catalog: a SQLite `catalog.db` in the
warehouse). Individual tests get a ready-to-query `(spark, prefix)` tuple,
where `prefix` is `<catalog>.<namespace>` (`rqm.inventory`), so
`f"{prefix}.buildings"` names a table.
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
def spark_session():
    """One Spark + Sedona session for the whole run (catalog `rqm` -> WAREHOUSE).

    Other test modules add catalogs to this session (Spark's packages and
    driver classpath are fixed when the JVM starts, so a second session
    could not differ anyway).
    """
    pytest.importorskip("pyspark")
    pytest.importorskip("sedona")

    from ingest_nsi import SparkSettings, build_spark  # type: ignore[import-not-found]

    if WAREHOUSE.exists():
        shutil.rmtree(WAREHOUSE)
    WAREHOUSE.mkdir(parents=True, exist_ok=True)

    spark = build_spark(SparkSettings(warehouse=WAREHOUSE, catalog=CATALOG))
    try:
        yield spark
    finally:
        spark.stop()


@pytest.fixture(scope="session")
def ingested_spark(spark_session, nsi_parquet: Path):
    """Create the Iceberg tables and load the NSI sample into catalog `rqm`."""
    from ingest_nsi import (  # type: ignore[import-not-found]
        NAMESPACE,
        apply_schema,
        load_buildings,
        load_geometries,
    )

    apply_schema(spark_session, CATALOG)
    load_buildings(spark_session, nsi_parquet, CATALOG)
    load_geometries(spark_session, nsi_parquet, CATALOG)
    yield spark_session, f"{CATALOG}.{NAMESPACE}"
