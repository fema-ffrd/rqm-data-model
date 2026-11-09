# Notebooks

## `nsi_demo.ipynb`

End-to-end demo of the NSI-minimal workflow: boot Apache Sedona + Iceberg, ingest `test/data/nsi.parquet` into `rqm.buildings` and `rqm.geometries`, assert the ADR invariants, run a few analytical queries, and export a flat analysis-ready table (parquet + CSV, WGS84 lat/lon).

Run inside the devcontainer (`rqm-base` env). Outputs land under `test/warehouse-notebook/` and `test/exports-notebook/` — both gitignored.

## Regenerating the notebook

The committed `.ipynb` is generated from `_build_demo_notebook.py` so cell edits stay diff-friendly. After editing the generator:

```bash
python notebooks/_build_demo_notebook.py
```
