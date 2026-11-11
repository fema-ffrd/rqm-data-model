# Notebooks

## Run order and the shared warehouse

The three demos form a series and share one Iceberg warehouse, `test/warehouse-notebook/`, whose JDBC catalog is the SQLite file `test/warehouse-notebook/catalog.db`. Run them in order, one at a time, because SQLite allows only one writer at once:

1. `01_setup_demo.ipynb` deletes and recreates the warehouse, loads the NSI sample, and commits it as version `nsi-sample-v1`.
2. `02_versioning_demo.ipynb` starts from `nsi-sample-v1` and leaves `main` at `nsi-sample-v7`.
3. `03_multi_source_demo.ipynb` starts from `nsi-sample-v7` and adds the other sources, ending at `riverbend-merged`.

Each of 02 and 03 checks first that `main` is exactly at the version it expects, and stops with a message naming the notebook to run if not. To run the series again, start over from 01. Every notebook keeps its own inputs and exports in its own folder under `test/` (all gitignored).

## `01_setup_demo.ipynb`

End-to-end demo of the NSI-minimal workflow: boot Apache Sedona + Iceberg (Spark 4.1, Iceberg v3 tables with native geometry), ingest the NSI parquet into `rqm.inventory.buildings` and `rqm.inventory.geometries`, inspect the partition/sort layout, assert the ADR invariants, run a few analytical queries, and export a flat analysis-ready table (parquet + CSV, WGS84 lat/lon) for the structures inside a watershed polygon. The watershed is `test/data/watershed/trinity.geojson` (HUC4 1203, Trinity River, TX); the export keeps only NSI points strictly inside it via a Sedona broadcast spatial join (`ST_Contains`). Point `WATERSHED_PATH` / `WATERSHED_CRS` in the setup cell at another GeoJSON file to subset a different area. The load is committed as version `nsi-sample-v1`, which `02_versioning_demo.ipynb` builds on.

Run inside the devcontainer (`rqm-base` env). The input is `test/data/nsi.parquet`. The notebook resets the shared warehouse `test/warehouse-notebook/` and writes its exports to `test/exports-notebook/`. It runs in under a minute.

## `02_versioning_demo.ipynb`

Walkthrough of inventory versioning ([ADR-versioning](../docs/decisions/ADR-versioning.md)) on the 45,622-structure sample `test/data/nsi.parquet`. It covers:

- starting from `nsi-sample-v1`, the sample as committed by `01_setup_demo.ipynb`;
- generating a modified copy with `versioning.make_modified_nsi` (40 removed, 120 attribute corrections, 30 moved points, 15 new structures) and loading it as `nsi-sample-v2`;
- querying both versions with `VERSION AS OF`, diffing them (with a map of what changed), and tracing one building's history (who, when, why);
- a calibration branch merged by fast-forward, a buyout-scenario branch merged three-way alongside a revaluation on `main`, and a conflicting edit that is refused and then resolved;
- exports pinned to a version by a `_rqm_version.json` manifest, re-exported identically after the inventory has moved on;
- `check_consistency` over every version.

It runs in about two minutes and leaves `main` at `nsi-sample-v7`. The inventory lives in the shared warehouse; the notebook's own inputs and exports are recreated on each run under `test/versioning-notebook/` (`inputs/`, `exports/`). The committed copy was executed with `jupyter nbconvert --to notebook --execute --inplace notebooks/02_versioning_demo.ipynb`, right after 01.

## `03_multi_source_demo.ipynb`

Walkthrough of multi-source inventories ([ADR-multi-source](../docs/decisions/ADR-multi-source.md), RQM-04), using synthetic sources that `tools/synthetic_sources.py` derives from `test/data/nsi.parquet`. The sources are:

- a county assessor roll: GeoPackage parcel polygons in EPSG:2276 with land-use codes, first-floor elevation in inches, text foundation types, and `PARCEL_ID` ids;
- a field survey: CSV with lon / lat and a sparse mapping;
- a planned development: parquet with WKB points.

Their mapping specs are in [`test/data/sources/`](../test/data/sources/). The notebook covers:

- validating the specs, including a rejected one;
- loading the assessor and the survey as versions `assessor-r1` and `survey-2025`, beside the NSI inventory that 02 left at `nsi-sample-v7`, then inspecting the `sources` registry and the version lineage;
- querying one field schema across sources: occupancy mix, exposed value, footprints vs. points, maps, and why summing across sources double counts;
- a scoped reload of the assessor's second release (`assessor-r2`) whose diff touches only assessor rows;
- loading the project inventory on a `riverbend` branch (`riverbend-plan`) and fast-forward merging it (`riverbend-merged`);
- a pinned export filtered to NSI + assessor, with its manifest;
- `check_consistency` over every version.

It runs in about two minutes. The inventory lives in the shared warehouse; the notebook's own inputs and exports are recreated on each run under `test/multi-source-notebook/` (`inputs/`, `exports/`). The committed copy was executed with `jupyter nbconvert --to notebook --execute --inplace notebooks/03_multi_source_demo.ipynb`, right after 01 and 02.
