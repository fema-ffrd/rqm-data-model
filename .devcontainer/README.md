# Devcontainer Template
Devcontainers allow interactive development inside of a docker container using VSCode. 


This devcontainer creates a reproducible environment for python projects using micromamba    
environments (faster/more robust version of conda). To add this devcontainer template to your project, copy this .devcontainer folder  
into the parent directory of your repository, and copy this [.gitattributes file](https://github.com/Michael-Baker-International-Lakewood/mbi_templates/blob/main/.gitattributes) into the same parent directory.

When opening this repository in VSCode, you may be prompted to re-open the project in devcontainer.  
Alternatively, you may access this option through the  
View menu -> Command Palette -> DevContainers: Reopen in Container.

Other requirements:
1. An environment file (env.yaml) is required placed in the root folder of the  
project for a reproducible python environment to be succussfully built.
2. docker installed on the local machine (linux)

## Large data (`RQM_DATA_DIR`)

When the repository is cloned on a Windows drive, the workspace is bind-mounted
over 9p, which reads at roughly 70 MB/s. The devcontainer therefore mounts a
Docker named volume, `rqm-data`, at `/data` and sets `RQM_DATA_DIR=/data/rqm`.
The volume lives on the Linux side and persists across container rebuilds.
Keep large NSI inputs, Iceberg warehouses, and exports there:

```
/data/rqm/
├── data/nsi2026_public_wkb.parquet   # nationwide NSI input
├── warehouse-notebook/               # Iceberg warehouse + catalog.db shared by the three demos (reset by 01)
├── exports-notebook/                 # analysis-ready exports (01_setup_demo.ipynb)
├── versioning-notebook/              # 02_versioning_demo.ipynb inputs/exports (recreated each run)
└── multi-source-notebook/            # 03_multi_source_demo.ipynb inputs/exports (recreated each run)
```

After the first rebuild with the volume, copy the nationwide file in once
(the source can then be removed from the Windows drive):

```bash
cp test/data/nsi2026_public_wkb.parquet "$RQM_DATA_DIR/data/"
```

`notebooks/01_setup_demo.ipynb` reads from `RQM_DATA_DIR` automatically. For the CLI,
pass volume paths explicitly:

```bash
python tools/ingest_nsi.py \
    --parquet "$RQM_DATA_DIR/data/nsi2026_public_wkb.parquet" \
    --warehouse "$RQM_DATA_DIR/warehouse" \
    --conf spark.driver.memory=8g
```
