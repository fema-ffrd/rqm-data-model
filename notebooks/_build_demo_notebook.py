"""One-shot generator for notebooks/nsi_demo.ipynb.

Run from the repo root (or anywhere; paths resolve from this file):
    python notebooks/_build_demo_notebook.py

Rebuild whenever the demo cells change. The committed artefact is the
generated .ipynb; this generator is kept so edits stay diff-friendly
(JSON diffs of .ipynb files are noisy; .py diffs are not).
"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "nsi_demo.ipynb"


def md(*lines: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": _lines(lines),
    }


def code(*lines: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _lines(lines),
    }


def _lines(blocks: tuple[str, ...]) -> list[str]:
    # Join each block to a single string, then split into newline-terminated
    # lines so the .ipynb JSON `source` array is one-line-per-entry (the
    # format Jupyter / nbdime render cleanly).
    text = "\n".join(blocks)
    parts = text.split("\n")
    return [p + "\n" for p in parts[:-1]] + [parts[-1]] if parts else []


cells: list[dict] = [

    md(
        "# NSI Ingest + Analysis-Ready Export Demo",
        "",
        "End-to-end walk-through of the RQM NSI-minimal workflow:",
        "",
        "1. Boot a local Apache Sedona session over Apache Iceberg storage.",
        "2. Load `test/data/nsi.parquet` into `rqm.buildings` and `rqm.geometries`.",
        "3. Validate the ingest against the ADR invariants.",
        "4. Run a handful of example analytical queries.",
        "5. Export a **flat, analysis-ready** table joining buildings with their",
        "   computation-selected geometry, including WGS84 lat/lon and the fields a",
        "   downstream flood-loss engine typically consumes.",
        "",
        "**Prerequisites.** Run inside the devcontainer (`rqm-base` env) or install",
        "`tools/requirements.txt` with Java 17 on `PATH`.",
    ),

    md("## 0. Setup"),

    code(
        "import sys",
        "from pathlib import Path",
        "",
        "# Locate the repo root by walking up from the notebook's cwd.",
        "REPO = Path.cwd().resolve()",
        "while REPO != REPO.parent and not (REPO / 'data-dictionary' / 'data-dictionary.yaml').exists():",
        "    REPO = REPO.parent",
        "assert (REPO / 'data-dictionary' / 'data-dictionary.yaml').exists(), f'could not locate repo root from {Path.cwd()}'",
        "",
        "sys.path.insert(0, str(REPO / 'tools'))",
        "",
        "PARQUET   = REPO / 'test' / 'data' / 'nsi.parquet'",
        "WAREHOUSE = REPO / 'test' / 'warehouse-notebook'",
        "OUT_DIR   = REPO / 'test' / 'exports-notebook'",
        "CATALOG   = 'rqm'",
        "",
        "print(f'repo:      {REPO}')",
        "print(f'parquet:   {PARQUET}  ({PARQUET.stat().st_size / 1024 / 1024:.1f} MB)')",
        "print(f'warehouse: {WAREHOUSE}')",
        "print(f'exports:   {OUT_DIR}')",
    ),

    md(
        "## 1. Boot Spark + Sedona + Iceberg",
        "",
        "`tools/ingest_nsi.py` already encapsulates the session configuration",
        "(Sedona + Iceberg packages, Kryo serializer, local Hadoop catalog). We",
        "reuse it here so the notebook and the CLI stay in sync.",
    ),

    code(
        "import shutil",
        "if WAREHOUSE.exists():",
        "    shutil.rmtree(WAREHOUSE)",
        "WAREHOUSE.mkdir(parents=True, exist_ok=True)",
        "",
        "from ingest_nsi import build_spark, apply_schema, load_buildings, load_geometries, NSI_SCALAR_FIELDS",
        "",
        "spark = build_spark(WAREHOUSE, catalog=CATALOG)",
        "print(f'Spark {spark.version}')",
    ),

    md(
        "## 2. Peek at the raw NSI parquet",
        "",
        "Before touching the schema, show what the source actually carries — field",
        "names, row count, a handful of sample rows.",
    ),

    code(
        "import pyarrow.parquet as pq",
        "pf = pq.ParquetFile(str(PARQUET))",
        "print(f'rows: {pf.metadata.num_rows:,}')",
        "print(f'cols: {len(pf.schema_arrow)}')",
        "print('columns:', sorted(f.name for f in pf.schema_arrow))",
    ),

    code(
        "# Preview a few rows (drop the WKB `shape` column for readability).",
        "raw = spark.read.parquet(str(PARQUET))",
        "raw.drop('shape').limit(5).toPandas()",
    ),

    md(
        "## 3. Create the Iceberg tables and ingest",
        "",
        "`apply_schema` runs `tools/schema.sql` (idempotent `CREATE TABLE IF NOT",
        "EXISTS`). `load_buildings` projects every NSI column into `rqm.buildings`",
        "with a surrogate `building_id`; `load_geometries` derives one computation",
        "point per structure (`role='point'`, `purpose='computation'`,",
        "`is_selected_for_export=TRUE`, geom as WKB).",
    ),

    code(
        "apply_schema(spark, CATALOG)",
        "n_b = load_buildings(spark, PARQUET, CATALOG)",
        "n_g = load_geometries(spark, PARQUET, CATALOG)",
        "print(f'loaded: {n_b:,} buildings, {n_g:,} geometries')",
    ),

    md(
        "## 4. Validate the ingest",
        "",
        "Assert the ADR-nsi-minimal invariants directly — the same checks",
        "`test/test_nsi_ingest.py` runs in CI, inline here so a stakeholder",
        "flipping through the notebook can see them pass.",
    ),

    code(
        "# Row-count parity",
        "assert spark.table(f'{CATALOG}.buildings').count() == pf.metadata.num_rows",
        "",
        "# Field-coverage / release-drift guard",
        "parquet_cols = {f.name for f in pf.schema_arrow}",
        "unmapped = parquet_cols - set(NSI_SCALAR_FIELDS) - {'shape'}",
        "assert not unmapped, f'unmapped NSI columns: {unmapped}'",
        "",
        "# Every building has its computation-selected point",
        "missing = spark.sql(f'''",
        "    SELECT COUNT(*) AS n",
        "    FROM {CATALOG}.buildings b",
        "    LEFT JOIN {CATALOG}.geometries g",
        "      ON g.building_id = b.building_id",
        "     AND g.role = 'point'",
        "     AND g.purpose = 'computation'",
        "     AND g.is_selected_for_export = TRUE",
        "    WHERE g.geometry_id IS NULL",
        "''').collect()[0]['n']",
        "assert missing == 0, f'{missing} buildings are missing a computation point'",
        "",
        "# Geometry validity + CRS",
        "invalid = spark.sql(f'''",
        "    SELECT COUNT(*) AS n",
        "    FROM {CATALOG}.geometries",
        "    WHERE NOT ST_IsValid(ST_GeomFromWKB(geom)) OR crs != 'EPSG:3857'",
        "''').collect()[0]['n']",
        "assert invalid == 0, f'{invalid} geometries failed validity / CRS'",
        "",
        "# Exactly one selected point per (building, purpose=computation)",
        "dupes = spark.sql(f'''",
        "    SELECT building_id, COUNT(*) AS n",
        "    FROM {CATALOG}.geometries",
        "    WHERE purpose = 'computation' AND is_selected_for_export = TRUE",
        "    GROUP BY building_id",
        "    HAVING COUNT(*) != 1",
        "''').count()",
        "assert dupes == 0, f'{dupes} buildings violate the selected-geometry invariant'",
        "",
        "print('All ADR-nsi-minimal validation rules passed.')",
    ),

    md(
        "## 5. Example analytical queries",
        "",
        "A small, representative sample of what the ingested schema answers",
        "directly. None of these should look surprising to anyone who has worked",
        "with NSI before — the point is that they are one SQL hop from the",
        "canonical table.",
    ),

    md("### 5.1 — Structures by occupancy type"),

    code(
        "spark.sql(f'''",
        "    SELECT occtype, COUNT(*) AS n_structures",
        "    FROM {CATALOG}.buildings",
        "    GROUP BY occtype",
        "    ORDER BY n_structures DESC",
        "    LIMIT 10",
        "''').toPandas()",
    ),

    md("### 5.2 — Total exposed value by census block (top 10)"),

    code(
        "spark.sql(f'''",
        "    SELECT cbfips,",
        "           COUNT(*)                     AS n_structures,",
        "           ROUND(SUM(val_struct), 0)    AS structure_value_usd,",
        "           ROUND(SUM(val_cont), 0)      AS contents_value_usd,",
        "           ROUND(SUM(val_struct + val_cont), 0) AS total_value_usd",
        "    FROM {CATALOG}.buildings",
        "    WHERE cbfips IS NOT NULL",
        "    GROUP BY cbfips",
        "    ORDER BY total_value_usd DESC",
        "    LIMIT 10",
        "''').toPandas()",
    ),

    md(
        "### 5.3 — Daytime vs nighttime population, SFHA vs non-SFHA",
        "",
        "Rough Special Flood Hazard Area classification by FIRM zone letter.",
    ),

    code(
        "spark.sql(f'''",
        "    SELECT",
        "        CASE WHEN firmzone IN ('A','AE','AO','AH','V','VE') THEN 'SFHA' ELSE 'Non-SFHA' END AS zone_class,",
        "        COUNT(*)                                                 AS structures,",
        "        ROUND(SUM(pop2amu65 + pop2amo65), 0)                     AS pop_2am,",
        "        ROUND(SUM(pop2pmu65 + pop2pmo65), 0)                     AS pop_2pm,",
        "        ROUND(SUM(u65disable + o65disable), 0)                   AS disabled_population",
        "    FROM {CATALOG}.buildings",
        "    GROUP BY zone_class",
        "    ORDER BY structures DESC",
        "''').toPandas()",
    ),

    md(
        "### 5.4 — Structures below the base flood elevation",
        "",
        "Comparing ground elevation to the static BFE gives a first-cut look at",
        "which structures sit below the regulatory flood surface. Depth here is",
        "`static_bfe - ground_elv` in feet; positive means the ground is below",
        "the BFE.",
    ),

    code(
        "spark.sql(f'''",
        "    SELECT",
        "        firmzone,",
        "        COUNT(*)                                                 AS n_structures,",
        "        SUM(CASE WHEN ground_elv < static_bfe THEN 1 ELSE 0 END) AS below_bfe,",
        "        ROUND(AVG(static_bfe - ground_elv), 2)                   AS avg_depth_below_bfe_ft",
        "    FROM {CATALOG}.buildings",
        "    WHERE static_bfe IS NOT NULL AND ground_elv IS NOT NULL",
        "    GROUP BY firmzone",
        "    ORDER BY below_bfe DESC NULLS LAST",
        "    LIMIT 10",
        "''').toPandas()",
    ),

    md(
        "## 6. Export an analysis-ready flat table",
        "",
        "Join `buildings` with its computation-selected geometry, reproject the",
        "point from EPSG:3857 to WGS84 (EPSG:4326), and select the fields a",
        "downstream flood-loss engine typically consumes: identity, occupancy /",
        "structural descriptors, value components, regulatory elevation, and",
        "day/night population exposure.",
        "",
        "Output format: single-part parquet (primary analysis format) and CSV",
        "(for inspection in spreadsheets). Spark writes each as a directory",
        "containing one `part-*.ext` file, because `.coalesce(1)` is in effect.",
    ),

    code(
        "flat = spark.sql(f'''",
        "    WITH pt AS (",
        "        SELECT building_id,",
        "               ST_Transform(ST_GeomFromWKB(geom), 'EPSG:3857', 'EPSG:4326') AS geom_wgs84",
        "        FROM {CATALOG}.geometries",
        "        WHERE role = 'point' AND purpose = 'computation' AND is_selected_for_export = TRUE",
        "    )",
        "    SELECT",
        "        b.building_id, b.fd_id, b.cbfips,",
        "        b.occtype, b.st_damcat, b.bldgtype,",
        "        b.num_story, b.sqft, b.bldheight,",
        "        b.found_type, b.found_ht,",
        "        b.val_struct, b.val_cont, b.val_vehic,",
        "        b.firmzone, b.static_bfe, b.ground_elv,",
        "        b.pop2amu65 + b.pop2amo65 AS pop_2am,",
        "        b.pop2pmu65 + b.pop2pmo65 AS pop_2pm,",
        "        b.u65disable + b.o65disable AS pop_disabled,",
        "        ST_X(pt.geom_wgs84) AS lon_wgs84,",
        "        ST_Y(pt.geom_wgs84) AS lat_wgs84",
        "    FROM {CATALOG}.buildings b",
        "    JOIN pt USING (building_id)",
        "''')",
        "flat.cache()",
        "print(f'flat table: {flat.count():,} rows x {len(flat.columns)} cols')",
        "flat.limit(5).toPandas()",
    ),

    code(
        "if OUT_DIR.exists():",
        "    shutil.rmtree(OUT_DIR)",
        "OUT_DIR.mkdir(parents=True, exist_ok=True)",
        "",
        "parquet_dir = OUT_DIR / 'nsi_analysis_ready.parquet'",
        "csv_dir     = OUT_DIR / 'nsi_analysis_ready.csv'",
        "",
        "flat.coalesce(1).write.mode('overwrite').parquet(str(parquet_dir))",
        "flat.coalesce(1).write.mode('overwrite').option('header', True).csv(str(csv_dir))",
        "",
        "for d in (parquet_dir, csv_dir):",
        "    for f in sorted(d.glob('part-*')):",
        "        print(f'wrote: {f}  ({f.stat().st_size / 1024:.1f} KB)')",
    ),

    md("### 6.1 — Read the exported parquet back as pandas"),

    code(
        "import pandas as pd",
        "part = sorted((OUT_DIR / 'nsi_analysis_ready.parquet').glob('part-*.parquet'))[0]",
        "flat_pd = pd.read_parquet(part)",
        "print(f'{len(flat_pd):,} rows, {len(flat_pd.columns)} cols')",
        "flat_pd.head()",
    ),

    md("## 7. Teardown"),

    code(
        "spark.stop()",
        "print('Spark session stopped.')",
    ),
]


notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3 (rqm-base)",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "pygments_lexer": "ipython3",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}


def main() -> None:
    OUT.write_text(json.dumps(notebook, indent=1) + "\n", encoding="utf-8")
    n_cells = len(notebook["cells"])
    n_code = sum(1 for c in notebook["cells"] if c["cell_type"] == "code")
    n_md = n_cells - n_code
    print(f"wrote {OUT}  ({n_cells} cells: {n_code} code, {n_md} markdown)")


if __name__ == "__main__":
    main()
