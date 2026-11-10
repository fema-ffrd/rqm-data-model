# Notebooks

## `nsi_demo.ipynb`

End-to-end demo of the NSI-minimal workflow: boot Apache Sedona + Iceberg (Spark 4.1, Iceberg v3 tables with native geometry), ingest the NSI parquet into `rqm.inventory.buildings` and `rqm.inventory.geometries`, inspect the partition/sort layout, assert the ADR invariants, run a few analytical queries, and export a flat analysis-ready table (parquet + CSV, WGS84 lat/lon) for the structures inside a watershed polygon. The watershed is `test/data/watershed/trinity.shp` (HUC4 1203, Trinity River, TX); the export keeps only NSI points strictly inside it via a Sedona broadcast spatial join (`ST_Contains`). Point `WATERSHED_SHP` / `WATERSHED_CRS` in the setup cell at another shapefile to subset a different area.

Run inside the devcontainer (`rqm-base` env). The input is `$RQM_DATA_DIR/data/nsi2026_public_wkb.parquet`, and outputs land under `$RQM_DATA_DIR/warehouse-notebook/` and `$RQM_DATA_DIR/exports-notebook/`, on the devcontainer's Linux data volume (see [`.devcontainer/README.md`](../.devcontainer/README.md#large-data-rqm_data_dir)). Without `RQM_DATA_DIR`, the notebook falls back to `test/data/`, `test/warehouse-notebook/`, and `test/exports-notebook/` (the latter two are gitignored).

Warehouses written before the move to the JDBC catalog and the `inventory` namespace are not visible to the notebook. Delete `warehouse-notebook/` to reload, or register the old tables (see the main README, "Configuring the Spark session").

## `nsi_versioning_demo.ipynb`

Walkthrough of inventory versioning ([ADR-versioning](../docs/decisions/ADR-versioning.md)) on the 45,622-structure sample `test/data/nsi.parquet`. It covers:

- loading the sample as version `nsi-sample-v1`;
- generating a modified copy with `versioning.make_modified_nsi` (40 removed, 120 attribute corrections, 30 moved points, 15 new structures) and loading it as `nsi-sample-v2`;
- querying both versions with `VERSION AS OF`, diffing them (with a map of what changed), and tracing one building's history (who, when, why);
- a calibration branch merged by fast-forward, a buyout-scenario branch merged three-way alongside a revaluation on `main`, and a conflicting edit that is refused and then resolved;
- exports pinned to a version by a `_rqm_version.json` manifest, re-exported identically after the inventory has moved on;
- `check_consistency` over every version.

It runs in about a minute and a half. Everything is recreated on each run under `$RQM_DATA_DIR/versioning-notebook/` (`warehouse/`, `inputs/`, `exports/`), or under `test/versioning-notebook/` (gitignored) without `RQM_DATA_DIR`. The committed copy was executed with `jupyter nbconvert --to notebook --execute --inplace notebooks/nsi_versioning_demo.ipynb`.

## Regenerating the notebook

The committed `.ipynb` is generated from `_build_demo_notebook.py` so cell edits stay diff-friendly. After editing the generator:

```bash
python notebooks/_build_demo_notebook.py
```
