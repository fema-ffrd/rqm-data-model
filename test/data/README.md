# Test data

## `nsi.parquet`

A single-region extract of the FEMA National Structure Inventory (NSI), used
as the fixture for `tools/ingest_nsi.py` and `test/test_nsi_ingest.py`.

- **Rows:** 45,622 structures
- **Columns:** 46 (all NSI v2 fields plus the WKB geometry `shape`)
- **CRS:** EPSG:3857 (Web Mercator)
- **Size:** ~5.1 MB

Every column in this file is mapped by the NSI-minimal schema. The field
coverage test (`test_field_coverage`) fails if the parquet grows a new column
the schema does not map — that is the schema's drift signal. GeoParquet 1.1
covering columns (e.g. the `shape_bbox` struct GDAL writes into the nationwide
release) are derived from `shape`, declared in the file's `geo` metadata, and
ignored by `diagnostic_coverage_check`.

Source: NSI public distribution (https://www.hec.usace.army.mil/confluence/nsi).

## `watershed/trinity.shp`

USGS WBD HUC4 1203 (Trinity River, TX) watershed boundary: one polygon,
NAD83 geographic (EPSG:4269). `notebooks/01_setup_demo.ipynb` uses it to spatially
subset the analysis-ready export to the NSI points inside the watershed.

## `sources/`

YAML source mapping specs (ADR-multi-source) for the synthetic inventories that
`tools/synthetic_sources.py` derives from `nsi.parquet`:

- `county-assessor.yaml`: `COUNTY-ASSESSOR`, GeoPackage parcel polygons in EPSG:2276;
- `field-survey-2025.yaml`: `FIELD-SURVEY-2025`, CSV with lon / lat;
- `project-riverbend.yaml`: `PROJECT-RIVERBEND`, parquet with WKB points in EPSG:3857.

They are used by `test/test_sources.py`, `test/test_multi_source.py`, and
`notebooks/03_multi_source_demo.ipynb`. The built-in NSI spec lives with the
loader, in `tools/sources/nsi.yaml`.
