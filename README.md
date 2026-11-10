# Risk Quantification Methodology (RQM) Data Model

> **NSI-Minimal Edition.** A focused, loadable schema for the National Structure Inventory, with first-class multi-geometry attach preserved for future building-footprint sources.

**Version:** 0.2.0 &nbsp;|&nbsp; **License:** [MIT](LICENSE) &nbsp;|&nbsp; **Companion:** `fema-ffrd/inland-consequences`

## Overview

This edition narrows the model to a tractable surface: a flat `buildings` table that maps every National Structure Inventory (NSI) parquet field one-to-one, and a `geometries` table that keeps geometry as its own class so a point from NSI and an alternate polygon footprint from Microsoft Building Footprints (MSBF) or FEMA USA Structures (USAS) can coexist without a schema break.

A prior exploration on `feature/hybrid_data_model` built the full RQM wish list — 35 tables across seven domains covering attribute uncertainty, components, hazard links, depth-damage functions, loss realizations, provenance, bitemporal versioning, and export contracts. That exploration is preserved for reference at [data-dictionary/data-dictionary-full.yaml](data-dictionary/data-dictionary-full.yaml) and [diagrams/rqm-erd-full.mmd](diagrams/rqm-erd-full.mmd). The decisions behind the narrowing — and a concrete re-entry point for each deferred concept — are documented in [docs/decisions/ADR-nsi-minimal.md](docs/decisions/ADR-nsi-minimal.md).

### Design goals (v0.1)

1. **Full NSI field coverage.** Every column in the NSI parquet lands in a table column with the NSI name preserved. Nothing is silently dropped.
2. **Loadable and queryable.** The schema runs under Apache Sedona on PySpark against Apache Iceberg storage. An ingest script and a pytest smoke-test suite exercise the schema end-to-end against a real sample.
3. **One architectural idea worth keeping.** `geometries` stays a first-class table so alternate footprint sources can attach without a migration.

## Storage + engine

| Tier | Technology | Role |
|---|---|---|
| **Lakehouse** | Apache Iceberg (format-version 3) | Versioned storage for `buildings` and `geometries`; native `GEOMETRY` columns |
| **Query engine** | Apache Sedona 1.9 on PySpark 4.1 | SQL, `ST_*` geometry functions, batch ingest |

Primary and foreign keys are logical (Iceberg does not enforce them). The ingest script and the smoke tests enforce the invariants listed in the ADR.

## Data model

Two tables. One FK edge. See [data-dictionary/data-dictionary.md](data-dictionary/data-dictionary.md) for the generated field reference and [diagrams/rqm-erd.mmd](diagrams/rqm-erd.mmd) for the ERD.

| Table | Purpose | Rows per NSI load |
|---|---|---|
| `buildings` | One row per NSI structure; all 46 NSI parquet columns mapped 1:1 | one per structure |
| `geometries` | First-class geometry registry (point + optional footprints + alternates) | one `role='point'` row per structure at load; additional rows added later for footprint sources |

### How the pieces fit together

- **`buildings`** is the flat inventory projection. NSI field names (`occtype`, `num_story`, `val_struct`, `pop2amu65`, `firmzone`, `cbfips`, …) are preserved verbatim so queries line up with the raw parquet. `building_id` is the primary key and equals the NSI `fd_id`, so ids are stable across re-ingests. The table is partitioned by state (`truncate(2, cbfips)`) and sorted by `cbfips`.
- **`geometries`** carries one row per `(building, representation, alternate)` tuple. The NSI load creates exactly one row per structure with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`, `crs='EPSG:3857'`, and `geom` holding the NSI `shape` point as a native Iceberg v3 `GEOMETRY(3857)`. `geometry_id` is the xxhash64 of the row's natural key `(source_ref, building_id, role, purpose, alt_group_key)`, so it is stable across re-ingests. Sedona queries read it via `ST_GeomFromWKB(spark_st_asbinary(geom))` (see `sedona_geom_sql()` in `tools/ingest_nsi.py`). Later, a footprint load (MSBF or USAS) writes additional rows with `role='footprint'`; a selection migration flips `is_selected_for_export` as needed.
- **Enums** for `occtype`, `st_damcat`, `found_type`, `geometry_role`, and `geometry_purpose` are inlined in the YAML rather than normalized into lookup tables. NSI already publishes stable short codes; a lookup table adds nothing until per-code metadata (RSMeans rates, fragility curves) is actually needed, and adding one later is additive.

### Deferred concepts (re-entry map)

Each deferred concept has a concrete re-entry point on this schema so reintroducing it is additive, not a rewrite. See [ADR-nsi-minimal §Deferrals](docs/decisions/ADR-nsi-minimal.md#deferrals-and-re-entry-map) for the full table; the short form:

- **Attribute uncertainty** → `uncertainty_ref` with `consumer_type='buildings'`.
- **Components + connections** → FK on `buildings.building_id`.
- **Hazard links + events** → FK on `buildings.building_id`; events standalone.
- **DDF library + DDF uncertainty** → keyed on `occtype` + `found_type` enums.
- **Loss realizations** → Iceberg table keyed on `building_id` + `event_id` + `realization_id`.
- **Provenance registry (PROV-O)** → soft refs on `buildings.source` and `geometries.source_ref` seed it.
- **Bitemporal identity + branching** → add `asset_id` to `buildings`; bundle five supporting tables.
- **Export contracts** → separate `exports` domain.
- **3D geometry roles + component-level geometry** → extend `geometries.role` enum; add optional `component_id` FK.

## Repository structure

```
rqm-data-model/
├── data-dictionary/
│   ├── data-dictionary.yaml       # single source of truth (NSI-minimal)
│   ├── data-dictionary.md         # generated — do not edit by hand
│   ├── data-dictionary-full.yaml  # archived hybrid exploration (reference only)
│   ├── data-dictionary-full.md    # archived generated output
│   ├── preview_dict.py            # validates + renders the Markdown
│   └── requirements.txt           # generator deps (PyYAML)
├── diagrams/
│   ├── rqm-erd.mmd                # Mermaid ERD (NSI-minimal)
│   ├── rqm-erd-full.mmd           # archived hybrid ERD (reference only)
│   └── pfrl-erd.png               # pre-rename PNG snapshot
├── docs/
│   ├── decisions/
│   │   └── ADR-nsi-minimal.md     # the decision behind this edition
│   ├── user-stories/
│   ├── building-standards-options/
│   └── Risk_Assessment_Maturity_Roadmap - DRAFT.pdf
├── tools/
│   ├── ingest_nsi.py              # CLI: NSI parquet → Iceberg via Sedona
│   ├── schema.sql                 # DDL for rqm.buildings + rqm.geometries
│   └── requirements.txt           # PySpark, Sedona, Iceberg, pyarrow, pytest
├── test/
│   ├── conftest.py                # session-scoped Spark + ingest fixture
│   ├── test_nsi_ingest.py         # smoke tests (ADR validation rules)
│   ├── test_spark_settings.py     # unit tests for session/catalog/layout config
│   └── data/
│       ├── nsi.parquet            # sample NSI extract (46 cols, ~45k rows)
│       └── README.md              # provenance of the sample
└── LICENSE
```

## Regenerating the data dictionary

The YAML is authoritative; the Markdown is generated.

```powershell
pip install -r data-dictionary/requirements.txt
python data-dictionary/preview_dict.py
```

The generator validates that every table has a primary key and that every FK resolves. Expected output:

```
OK: validated 2 tables, 57 columns. Wrote data-dictionary.md.
```

## Running the ingest and smoke tests

Requires Java 17+ on PATH for PySpark 4.1.

```powershell
pip install -r tools/requirements.txt
python tools/ingest_nsi.py --parquet test/data/nsi.parquet --warehouse test/warehouse
pytest test/test_nsi_ingest.py -v
```

The ingest emits:

```
OK: ingested 45622 buildings, 45622 geometries from 45622 parquet rows
into Iceberg warehouse at test/warehouse.
```

The test suite asserts the seven validation rules from the ADR: field coverage, row-count parity, non-null `occtype`, geometry validity + `EPSG:3857`, selected-geometry uniqueness, every-building-has-a-point, and stable unique `building_id` (= `fd_id`). It also checks the tables are Iceberg v3 with a native geometry column and the default partition/sort layout. A non-asserting diagnostic prints NSI release completeness for `found_type`, `val_struct`, and `bldheight`.

### Configuring the Spark session

`tools/ingest_nsi.py` builds its session from `SparkSettings`; defaults run Spark locally (`local[*]`) against a Hadoop (file-system) Iceberg catalog in `--warehouse`. The same knobs are exposed on the CLI so moving to a cluster or a managed catalog is configuration, not code:

```bash
# Local tuning
python tools/ingest_nsi.py --parquet test/data/nsi.parquet --warehouse test/warehouse \
    --master "local[4]" --conf spark.driver.memory=8g

# Table layout overrides (applied when a table is created)
python tools/ingest_nsi.py --parquet test/data/nsi.parquet --warehouse test/warehouse \
    --partition "geometries=bucket(16, building_id)" --write-order "geometries=building_id"
```

For cluster runs, `--master ""` defers to `spark-submit` / the cluster manager, `--catalog-type` + `--catalog-prop KEY=VALUE` select another Iceberg catalog (e.g. `rest` with `uri=...`, or `glue`), `--warehouse` accepts an object-store URI, and `--conf` passes any other Spark setting. The `--parquet` source is still read from the local file system.

## License

Released under the [MIT License](LICENSE).
