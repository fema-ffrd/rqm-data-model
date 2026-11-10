# Risk Quantification Methodology (RQM) Data Model

> **NSI-Minimal Edition.** A focused, loadable schema for the National Structure Inventory, with first-class multi-geometry attach preserved for future building-footprint sources, and versioned inventories: named versions, diffs, project branches, and pinned exports.

**Version:** 0.3.0 &nbsp;|&nbsp; **License:** [MIT](LICENSE) &nbsp;|&nbsp; **Companion:** `fema-ffrd/inland-consequences`

## Overview

This edition narrows the model to a tractable surface: a flat `buildings` table that maps every National Structure Inventory (NSI) parquet field one-to-one, and a `geometries` table that keeps geometry as its own class so a point from NSI and an alternate polygon footprint from Microsoft Building Footprints (MSBF) or FEMA USA Structures (USAS) can coexist without a schema break.

A prior exploration on `feature/hybrid_data_model` built the full RQM wish list — 35 tables across seven domains covering attribute uncertainty, components, hazard links, depth-damage functions, loss realizations, provenance, bitemporal versioning, and export contracts. That exploration is preserved for reference at [data-dictionary/data-dictionary-full.yaml](data-dictionary/data-dictionary-full.yaml) and [diagrams/rqm-erd-full.mmd](diagrams/rqm-erd-full.mmd). The decisions behind the narrowing — and a concrete re-entry point for each deferred concept — are documented in [docs/decisions/ADR-nsi-minimal.md](docs/decisions/ADR-nsi-minimal.md).

### Design goals (v0.1)

1. **Full NSI field coverage.** Every column in the NSI parquet lands in a table column with the NSI name preserved. Nothing is silently dropped.
2. **Loadable and queryable.** The schema runs under Apache Sedona on PySpark against Apache Iceberg storage. An ingest script and a pytest smoke-test suite exercise the schema end-to-end against a real sample.
3. **One architectural idea worth keeping.** `geometries` stays a first-class table so alternate footprint sources can attach without a migration.
4. **Versioned inventories** ([ADR-versioning](docs/decisions/ADR-versioning.md)). Loading a modified dataset creates a new named version instead of overwriting the old one. Any version can be queried, diffed, branched for a project, merged back, and pinned in an export.

## Storage + engine

| Tier | Technology | Role |
|---|---|---|
| **Lakehouse** | Apache Iceberg (format-version 3) | Versioned storage (snapshots, tags, branches) for the `inventory` namespace; native `GEOMETRY` columns |
| **Catalog** | Iceberg JDBC catalog, SQLite file `<warehouse>/catalog.db` by default | Atomic commits without a server; point `uri` at Postgres for a shared multi-user catalog; Hadoop, REST, and Glue also work |
| **Query engine** | Apache Sedona 1.9 on PySpark 4.1 | SQL, `ST_*` geometry functions, batch ingest |

Primary and foreign keys are logical (Iceberg does not enforce them). The ingest script and the smoke tests enforce the invariants listed in the ADR.

## Data model

Two inventory tables plus two versioning tables, all in the Iceberg namespace `inventory` (`rqm.inventory.<table>`). See [data-dictionary/data-dictionary.md](data-dictionary/data-dictionary.md) for the generated field reference and [diagrams/rqm-erd.mmd](diagrams/rqm-erd.mmd) for the ERD.

| Table | Purpose | Rows per NSI load |
|---|---|---|
| `buildings` | One row per NSI structure; all 46 NSI parquet columns mapped 1:1 | one per structure |
| `geometries` | First-class geometry registry (point + optional footprints + alternates) | one `role='point'` row per structure at load; additional rows added later for footprint sources |
| `inventory_versions` | Version registry: name (= Iceberg tag), parent, branch, editor, reason, source hash, snapshot ids, change counts | one per committed version |
| `inventory_changes` | Per-building change log of each version relative to its parent | one per building added / removed / modified |

### How the pieces fit together

- **`buildings`** is the flat inventory projection. NSI field names (`occtype`, `num_story`, `val_struct`, `pop2amu65`, `firmzone`, `cbfips`, …) are preserved verbatim so queries line up with the raw parquet. `building_id` is the primary key and equals the NSI `fd_id`, so ids are stable across re-ingests. The table is partitioned by state (`truncate(2, cbfips)`) and sorted by `cbfips`.
- **`geometries`** carries one row per `(building, representation, alternate)` tuple. The NSI load creates exactly one row per structure with `role='point'`, `purpose='computation'`, `is_selected_for_export=true`, `crs='EPSG:3857'`, and `geom` holding the NSI `shape` point as a native Iceberg v3 `GEOMETRY(3857)`. `geometry_id` is the xxhash64 of the row's natural key `(source_ref, building_id, role, purpose, alt_group_key)`, so it is stable across re-ingests. Sedona queries read it via `ST_GeomFromWKB(spark_st_asbinary(geom))` (see `sedona_geom_sql()` in `tools/ingest_nsi.py`). Later, a footprint load (MSBF or USAS) writes additional rows with `role='footprint'`; a selection migration flips `is_selected_for_export` as needed.
- **Versions** ([ADR-versioning](docs/decisions/ADR-versioning.md)). Loads are full-replacement `MERGE` upserts, so a modified dataset updates only the rows that changed, and merge-on-read writes keep the storage cost to those rows. Committing a load or an edit creates a named **version**: an Iceberg tag of that name on `buildings` and `geometries`, a row in `inventory_versions` (who, when, why, source file), and per-building rows in `inventory_changes`. Every version stays queryable with `VERSION AS OF '<version>'`. Project inventories are Iceberg **branches**. They merge back by fast-forward, or by a building-level three-way merge that refuses conflicting edits. `export_version` writes a version plus a `_rqm_version.json` manifest that pins its snapshots.
- **Enums** for `occtype`, `st_damcat`, `found_type`, `geometry_role`, and `geometry_purpose` are inlined in the YAML rather than normalized into lookup tables. NSI already publishes stable short codes; a lookup table adds nothing until per-code metadata (RSMeans rates, fragility curves) is actually needed, and adding one later is additive.

### Deferred concepts (re-entry map)

Each deferred concept has a concrete re-entry point on this schema so reintroducing it is additive, not a rewrite. See [ADR-nsi-minimal §Deferrals](docs/decisions/ADR-nsi-minimal.md#deferrals-and-re-entry-map) for the full table; the short form:

- **Attribute uncertainty** → `uncertainty_ref` with `consumer_type='buildings'`.
- **Components + connections** → FK on `buildings.building_id`.
- **Hazard links + events** → FK on `buildings.building_id`; events standalone.
- **DDF library + DDF uncertainty** → keyed on `occtype` + `found_type` enums.
- **Loss realizations** → Iceberg table keyed on `building_id` + `event_id` + `realization_id`.
- **Provenance registry (PROV-O)** → soft refs on `buildings.source` and `geometries.source_ref` seed it.
- **Bitemporal identity + branching** → resolved by [ADR-versioning](docs/decisions/ADR-versioning.md) (Iceberg tags and branches plus a version registry); valid time is still deferred.
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
│   │   ├── ADR-nsi-minimal.md     # the decision behind this edition
│   │   └── ADR-versioning.md      # versions, branches, merges, pinned exports
│   ├── user-stories/
│   ├── building-standards-options/
│   └── Risk_Assessment_Maturity_Roadmap - DRAFT.pdf
├── notebooks/
│   ├── nsi_demo.ipynb             # nationwide ingest + watershed export demo
│   └── nsi_versioning_demo.ipynb  # versioning walkthrough on the NSI sample
├── tools/
│   ├── ingest_nsi.py              # CLI: NSI parquet → Iceberg via Sedona (MERGE + --version)
│   ├── versioning.py              # versions, diffs, history, branches, merges, exports
│   ├── rqm_version.py             # CLI: list / diff / history / branch / export / check
│   ├── schema.sql                 # DDL for the rqm.inventory namespace (4 tables)
│   └── requirements.txt           # PySpark, Sedona, Iceberg, pyarrow, shapely, pytest
├── test/
│   ├── conftest.py                # session-scoped Spark + ingest fixture
│   ├── test_nsi_ingest.py         # smoke tests (ADR validation rules)
│   ├── test_versioning.py         # versions, diffs, branches, merges, exports
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
OK: validated 4 tables, 77 columns. Wrote data-dictionary.md.
```

## Running the ingest and smoke tests

Requires Java 17+ on PATH for PySpark 4.1.

```powershell
pip install -r tools/requirements.txt
python tools/ingest_nsi.py --parquet test/data/nsi.parquet --warehouse test/warehouse \
    --version nsi-sample-v1 --editor "$USER" --reason "initial load"
pytest test -v
```

The ingest emits:

```
OK: loaded 45622 buildings, 45622 geometries from 45622 parquet rows into branch 'main' of the Iceberg warehouse at test/warehouse.
OK: committed version 'nsi-sample-v1' on 'main' (parent None): +45622 added, -0 removed, ~0 changed.
```

The test suite asserts the seven validation rules from the ADR: field coverage, row-count parity, non-null `occtype`, geometry validity + `EPSG:3857`, selected-geometry uniqueness, every-building-has-a-point, and stable unique `building_id` (= `fd_id`). It also checks the tables are Iceberg v3 with a native geometry column and the default partition/sort layout. A non-asserting diagnostic prints NSI release completeness for `found_type`, `val_struct`, and `bldheight`.

`test/test_versioning.py` loads a 5,000-structure slice as `v1` and a modified copy as `v2` (`versioning.make_modified_nsi`: removed, edited, moved, and added structures). It then asserts:

- time travel, diffs, the change log, and building history;
- that a reload of identical data rewrites nothing;
- fast-forward and three-way merges, refused conflicts, and identical edits on both sides merging cleanly;
- that a branch reload removes footprints of deleted buildings;
- pinned exports, and detection of incomplete commits.

### Versioning a modified dataset

Load each dataset with a version name. Loading into an existing inventory is a full-replacement `MERGE`: new structures are added, changed ones updated, and missing ones removed from the head. Earlier versions keep everything.

```bash
python tools/ingest_nsi.py --parquet nsi_2025.parquet --warehouse test/warehouse \
    --version nsi-2025 --editor jdoe --reason "2025 NSI release"

python tools/rqm_version.py --warehouse test/warehouse list
python tools/rqm_version.py --warehouse test/warehouse diff nsi-sample-v1 nsi-2025
python tools/rqm_version.py --warehouse test/warehouse history 120262564

# Project branch: create, load or edit, commit, merge back
python tools/rqm_version.py --warehouse test/warehouse branch create calibration --from nsi-2025
python tools/ingest_nsi.py --parquet calibrated.parquet --warehouse test/warehouse \
    --branch calibration --version calibration-1 --kind edit --editor jdoe --reason "FFE survey"
python tools/rqm_version.py --warehouse test/warehouse branch merge calibration \
    --version nsi-2025.1 --editor lead --reason "accept calibration"

# Reproducible export pinned to a version, and an integrity check
python tools/rqm_version.py --warehouse test/warehouse export nsi-2025 out/nsi-2025 --where "firmzone IN ('A','AE')"
python tools/rqm_version.py --warehouse test/warehouse check
```

In SQL, any version is `SELECT … FROM rqm.inventory.buildings VERSION AS OF 'nsi-2025'`. Branch edits go through `rqm.inventory.buildings.branch_<name>`, and `versioning.commit_version(..., branch=<name>)` commits them. [notebooks/nsi_versioning_demo.ipynb](notebooks/nsi_versioning_demo.ipynb) walks through the whole workflow on the NSI sample.

### Configuring the Spark session

`tools/ingest_nsi.py` builds its session from `SparkSettings`; defaults run Spark locally against an Iceberg JDBC catalog whose SQLite database is `<warehouse>/catalog.db`. The same knobs are exposed on the CLI so moving to a cluster or a managed catalog is configuration, not code:

```bash
# Local tuning
python tools/ingest_nsi.py --parquet test/data/nsi.parquet --warehouse test/warehouse \
    --master "local[4]" --conf spark.driver.memory=8g

# Table layout overrides (applied when a table is created)
python tools/ingest_nsi.py --parquet test/data/nsi.parquet --warehouse test/warehouse \
    --partition "geometries=bucket(16, building_id)" --write-order "geometries=building_id"
```

For cluster runs, `--master ""` defers to `spark-submit` / the cluster manager. `--catalog-type` + `--catalog-prop KEY=VALUE` select another Iceberg catalog: a shared Postgres JDBC catalog (`--catalog-prop uri=jdbc:postgresql://host/db`, with the Postgres driver added to the Spark packages), `rest` with `uri=...`, `glue`, or the single-writer `hadoop` catalog. `--warehouse` accepts an object-store URI, `--namespace` changes the table namespace, and `--conf` passes any other Spark setting. The `--parquet` source is still read from the local file system.

Warehouses written before 0.3.0 used the Hadoop catalog with tables at the catalog root. Reload them, or register their tables in the new catalog with `CALL rqm.system.register_table('rqm.inventory.buildings', '<path>/buildings/metadata/vN.metadata.json')`.

## License

Released under the [MIT License](LICENSE).
