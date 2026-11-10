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
NAD83 geographic (EPSG:4269). `notebooks/nsi_demo.ipynb` uses it to spatially
subset the analysis-ready export to the NSI points inside the watershed.
