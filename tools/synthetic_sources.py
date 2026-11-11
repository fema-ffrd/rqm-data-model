"""Deterministic synthetic building inventories for the multi-source demo and
tests (ADR-multi-source).

Every source is derived from the NSI sample (`test/data/nsi.parquet`) at run
time, so no third-party inventory is committed. Each one looks like a real
non-NSI inventory: its own column names, codes, units, CRS, file format, and
native ids.

| source_ref         | format                       | what it exercises                       |
|--------------------|------------------------------|-----------------------------------------|
| COUNTY-ASSESSOR    | GeoPackage polygons, EPSG:2276 | land-use crosswalk, FFE in inches,    |
|                    | (TX North Central, US ft)    | text foundation types, PARCEL_ID ids,   |
|                    |                              | footprints + point-on-surface; release 2 |
| FIELD-SURVEY-2025  | CSV lon / lat (EPSG:4326)    | sparse mapping (most fields NULL)       |
| PROJECT-RIVERBEND  | parquet, WKB points (EPSG:3857) | planned structures for a project branch |

The matching specs live in `notebooks/sources/*.yaml`.

Usage
-----
    python tools/synthetic_sources.py --nsi test/data/nsi.parquet --out /data/rqm/synthetic
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ASSESSOR_CRS = "EPSG:2276"  # NAD83 / Texas North Central (ftUS)
ASSESSOR_LAYER = "parcels"
DEFAULT_COUNTY = "48113"  # Dallas County: the sample's second county
SURVEY_COUNTY = "48397"  # Rockwall County: the bulk of the sample
FT_PER_M = 1 / 0.3048006096  # US survey foot

# NSI occupancy (prefix) -> assessor land-use code. The assessor spec's
# crosswalk maps these back (lossily) to RQM occupancy codes.
_LANDUSE_BY_OCCTYPE: tuple[tuple[str, str], ...] = (
    ("RES1", "A1"),  # single-family
    ("RES2", "A2"),  # mobile home
    ("RES3A", "B2"),  # duplex
    ("RES3B", "B2"),
    ("RES3", "B1"),  # multi-family
    ("RES", "F1"),  # hotels, dorms, nursing homes: commercial roll
    ("COM", "F1"),  # commercial
    ("IND", "F2"),  # industrial
    ("AGR", "D2"),  # farm improvements
    ("GOV", "XG"),  # exempt: government
    ("REL", "XC"),  # exempt: church
    ("EDU", "XS"),  # exempt: school
)
# Land-use codes the assessor uses but the demo crosswalk leaves out, so
# the load reports them and applies the default.
UNMAPPED_LANDUSE = "Q9"
_ASSESSOR_FOUNDATION = {"S": "SLAB", "C": "CRAWL", "B": "BASEMENT", "P": "PIER"}
_SURVEY_FOUNDATION = {
    "S": "Slab on grade",
    "C": "Crawlspace",
    "B": "Basement",
    "P": "Pier/post",
}
_SURVEY_USE = {"RES": "Residential", "COM": "Commercial", "IND": "Industrial"}


@dataclass(frozen=True)
class AssessorTruth:
    """What the assessor releases contain, keyed by PARCEL_ID."""

    release1: str
    release2: str
    nsi_fd_id: dict[str, int]  # parcel -> the NSI structure it was derived from
    removed: tuple[str, ...]
    edited: tuple[str, ...]  # IMPR_VAL reassessed and FFE_IN resurveyed
    added: tuple[str, ...]
    unmapped_landuse: tuple[str, ...]  # parcels coded UNMAPPED_LANDUSE


@dataclass(frozen=True)
class SyntheticSources:
    """Paths of the generated inputs plus ground truth for checks."""

    out_dir: str
    assessor: AssessorTruth
    survey: str
    survey_fd_id: dict[str, int]  # SURVEY_ID -> surveyed NSI structure
    riverbend: str
    riverbend_lots: tuple[str, ...]
    seed: int
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


def _occupancy_base(occtype: str | None) -> str:
    return (occtype or "").split("-")[0]


def landuse_code(occtype: str | None) -> str:
    """Assessor land-use code for an NSI occupancy."""
    base = _occupancy_base(occtype)
    for prefix, code in _LANDUSE_BY_OCCTYPE:
        if base.startswith(prefix):
            return code
    return UNMAPPED_LANDUSE


def web_mercator_to_assessor_crs(
    x: np.ndarray, y: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """EPSG:3857 -> EPSG:2276 with the NAD83 = WGS84 identity datum step.

    `Transformer.from_crs("EPSG:3857", "EPSG:2276")` lets PROJ pick the
    NAD83 <-> WGS84 operation: the identity step offline, but a NOAA grid
    shift (~0.4 m here) when PROJ_NETWORK is on or grids are installed.
    Sedona (GeoTools) always uses the identity step, so pinning it keeps
    the synthetic data identical on every machine and consistent with the
    loader's ST_Transform.
    """
    import pyproj

    to_lonlat = pyproj.Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
    # NAD83 geographic (EPSG:4269) -> its own projection: no datum step.
    project = pyproj.Transformer.from_crs("EPSG:4269", ASSESSOR_CRS, always_xy=True)
    return project.transform(*to_lonlat.transform(x, y))


def _nsi_rows(nsi: pa.Table, county: str) -> pa.Table:
    """NSI rows of one county with a point."""
    in_county = pc.starts_with(pc.fill_null(nsi["cbfips"], ""), county)
    return nsi.filter(pc.and_(in_county, pc.is_valid(nsi["shape"])))


def _points_3857(table: pa.Table) -> np.ndarray:
    import shapely

    points = shapely.from_wkb(table["shape"].to_pylist())
    return shapely.get_coordinates(points)  # (n, 2), drops Z


def _col(table: pa.Table, name: str) -> list:
    return table[name].to_pylist()


def _footprints(
    centers_2276: np.ndarray, area_sqft: np.ndarray, rng: np.random.Generator
) -> list:
    """Rotated square footprints (EPSG:2276 feet) centered on each point."""
    import shapely
    from shapely import affinity

    side = np.sqrt(np.clip(area_sqft, 400.0, 40_000.0))
    angles = rng.uniform(0.0, 30.0, size=len(side))
    polygons = []
    for (x, y), s, angle in zip(centers_2276, side, angles):
        square = shapely.box(x - s / 2, y - s / 2, x + s / 2, y + s / 2)
        polygons.append(affinity.rotate(square, angle, origin=(x, y)))
    return polygons


def _write_gpkg(path: Path, columns: dict, geometries: list) -> None:
    import geopandas as gpd

    path.unlink(missing_ok=True)
    gdf = gpd.GeoDataFrame(columns, geometry=geometries, crs=ASSESSOR_CRS)
    gdf = gdf.rename_geometry("geom")
    gdf.to_file(path, layer=ASSESSOR_LAYER, driver="GPKG")


def make_county_assessor(
    nsi_path: str | Path,
    out_dir: str | Path,
    *,
    county: str = DEFAULT_COUNTY,
    max_parcels: int | None = None,
    n_remove: int = 12,
    n_edit: int = 25,
    n_add: int = 8,
    n_unmapped: int = 3,
    seed: int = 11,
) -> AssessorTruth:
    """Write two releases of a county assessor roll as GeoPackages.

    Release 1 has one parcel per NSI structure in `county` (optionally the
    first `max_parcels` by fd_id), with a rotated square footprint around
    the NSI point in EPSG:2276, its own PARCEL_ID, land-use codes, IMPR_VAL,
    first-floor elevation above grade in inches (FFE_IN), and text
    foundation types. `n_unmapped` parcels carry a land-use code the demo
    crosswalk leaves out. Release 2 removes `n_remove` parcels, reassesses
    `n_edit` (IMPR_VAL +8 %, FFE_IN +6 in), and adds `n_add` new ones.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    rows = _nsi_rows(pq.read_table(str(nsi_path)), county)
    rows = rows.take(pc.sort_indices(rows["fd_id"]))
    if max_parcels is not None:
        rows = rows.slice(0, max_parcels)
    n = rows.num_rows
    if n < n_remove + n_edit + n_add + n_unmapped:
        raise ValueError(f"county {county} has only {n} NSI structures")

    xy = _points_3857(rows)
    cx, cy = web_mercator_to_assessor_crs(xy[:, 0], xy[:, 1])
    centers = np.column_stack([cx, cy])
    sqft = np.array([s or 1600.0 for s in _col(rows, "sqft")], dtype=float)
    stories = np.array([max(1, s or 1) for s in _col(rows, "num_story")], dtype=float)
    footprints = _footprints(centers, sqft / stories, rng)

    parcel_numbers = rng.permutation(n) + 100_000
    parcel_ids = [f"R{p:07d}" for p in parcel_numbers]
    landuse = [landuse_code(o) for o in _col(rows, "occtype")]
    unmapped = sorted(rng.choice(n, size=n_unmapped, replace=False).tolist())
    for i in unmapped:
        landuse[i] = UNMAPPED_LANDUSE
    found_ht = _col(rows, "found_ht")
    release1 = {
        "PARCEL_ID": parcel_ids,
        "LANDUSE": landuse,
        "IMPR_VAL": [
            None if v is None else int(round(v)) for v in _col(rows, "val_struct")
        ],
        "LAND_VAL": [int(v) for v in rng.integers(20_000, 250_000, size=n)],
        "FFE_IN": [None if h is None else int(round(h * 12)) for h in found_ht],
        "FOUND": [_ASSESSOR_FOUNDATION.get(f) for f in _col(rows, "found_type")],
        "STORIES": _col(rows, "num_story"),
        "YR_BUILT": _col(rows, "med_yr_blt"),
        "LIVING_SQFT": _col(rows, "sqft"),
        "BLOCK_GEOID": _col(rows, "cbfips"),
    }
    path1 = out / "county-assessor-r1.gpkg"
    _write_gpkg(path1, release1, footprints)

    # Release 2: removals, reassessments, and new parcels.
    order = rng.permutation([i for i in range(n) if i not in set(unmapped)])
    removed_idx = np.sort(order[:n_remove])
    edited_idx = np.sort(order[n_remove : n_remove + n_edit])
    release2 = {k: list(v) for k, v in release1.items()}
    for i in edited_idx:
        if release2["IMPR_VAL"][i] is not None:
            release2["IMPR_VAL"][i] = int(round(release2["IMPR_VAL"][i] * 1.08))
        ffe = release2["FFE_IN"][i]
        release2["FFE_IN"][i] = 6 if ffe is None else ffe + 6
    geoms2 = list(footprints)
    source_idx = rng.choice(n, size=n_add, replace=False)
    added_ids = [f"R{900_000 + k:07d}" for k in range(n_add)]
    offset_ft = 150.0
    for k, i in enumerate(source_idx):
        for key in release2:
            release2[key].append(release1[key][i])
        release2["PARCEL_ID"][-1] = added_ids[k]
        release2["LANDUSE"][-1] = "A1"
        geoms2.append(
            _footprints(
                centers[[i]] + offset_ft, np.array([sqft[i] / stories[i]]), rng
            )[0]
        )
    keep = [i for i in range(len(geoms2)) if i not in set(removed_idx)]
    release2 = {k: [v[i] for i in keep] for k, v in release2.items()}
    path2 = out / "county-assessor-r2.gpkg"
    _write_gpkg(path2, release2, [geoms2[i] for i in keep])

    fd_ids = _col(rows, "fd_id")
    return AssessorTruth(
        release1=str(path1),
        release2=str(path2),
        nsi_fd_id={parcel_ids[i]: int(fd_ids[i]) for i in range(n)},
        removed=tuple(parcel_ids[i] for i in removed_idx),
        edited=tuple(parcel_ids[i] for i in edited_idx),
        added=tuple(added_ids),
        unmapped_landuse=tuple(parcel_ids[i] for i in unmapped),
    )


def make_field_survey(
    nsi_path: str | Path,
    out_csv: str | Path,
    *,
    county: str = SURVEY_COUNTY,
    n: int = 300,
    jitter_m: float = 3.0,
    seed: int = 23,
) -> dict[str, int]:
    """Write a field survey of `n` structures as CSV (lon / lat, EPSG:4326).

    Each record observes one NSI structure with a GPS fix within `jitter_m`
    meters, a measured first-floor height (feet, to 0.1 ft), a text
    foundation type, stories, an observed use, and the census block. Returns
    SURVEY_ID -> surveyed NSI fd_id.
    """
    import csv

    rng = np.random.default_rng(seed)
    nsi = pq.read_table(str(nsi_path))
    rows = _nsi_rows(nsi, county)
    rows = rows.filter(pc.is_valid(rows["found_ht"]))
    rows = rows.take(np.sort(rng.choice(rows.num_rows, size=n, replace=False)))
    lon = np.array(_col(rows, "x")) + rng.normal(0, jitter_m / 2, n) / 94_000.0
    lat = np.array(_col(rows, "y")) + rng.normal(0, jitter_m / 2, n) / 111_000.0
    ffh = np.round(np.array(_col(rows, "found_ht")) + rng.normal(0, 0.3, n), 1)
    survey_ids = [f"FS25-{i + 1:04d}" for i in range(n)]
    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "SURVEY_ID",
                "LON",
                "LAT",
                "FFH_FT",
                "FOUNDATION",
                "STORIES",
                "USE_OBSERVED",
                "BLOCK_GEOID",
                "SURVEY_DATE",
            ]
        )
        for i in range(n):
            writer.writerow(
                [
                    survey_ids[i],
                    f"{lon[i]:.7f}",
                    f"{lat[i]:.7f}",
                    f"{max(ffh[i], 0.0):.1f}",
                    _SURVEY_FOUNDATION.get(rows["found_type"][i].as_py(), ""),
                    int(rows["num_story"][i].as_py() or 0) or "",
                    _SURVEY_USE.get(rows["st_damcat"][i].as_py(), "Other"),
                    rows["cbfips"][i].as_py() or "",
                    f"2025-{6 + i % 4:02d}-{1 + i % 28:02d}",
                ]
            )
    return {survey_ids[i]: int(rows["fd_id"][i].as_py()) for i in range(n)}


def make_project_riverbend(
    nsi_path: str | Path,
    out_parquet: str | Path,
    *,
    county: str = SURVEY_COUNTY,
    rows_x: int = 6,
    rows_y: int = 6,
    spacing_m: float = 40.0,
    seed: int = 31,
) -> tuple[str, ...]:
    """Write a planned development as parquet with WKB points (EPSG:3857).

    A `rows_x` x `rows_y` grid of new lots placed just east of the county's
    NSI extent: single-family homes, a row of townhomes, and two retail
    pads, with planned values, design finished-floor heights, and the census
    block of the nearest NSI structure. Returns the LOT_NO values.
    """
    import shapely

    rng = np.random.default_rng(seed)
    rows = _nsi_rows(pq.read_table(str(nsi_path)), county)
    xy = _points_3857(rows)
    anchor = np.array([xy[:, 0].max() + 500.0, np.median(xy[:, 1])])
    nearest = int(np.argmin(np.hypot(*(xy - anchor).T)))
    block = rows["cbfips"][nearest].as_py()

    n = rows_x * rows_y
    gx, gy = np.meshgrid(np.arange(rows_x), np.arange(rows_y))
    coords = anchor + np.column_stack([gx.ravel(), gy.ravel()]) * spacing_m
    use = ["SFR"] * n
    for i in range(rows_x):  # last row: townhomes
        use[n - rows_x + i] = "TH"
    use[0] = use[1] = "RETAIL"
    value = {"SFR": 385_000, "TH": 290_000, "RETAIL": 1_250_000}
    lots = tuple(f"RB-{i + 1:03d}" for i in range(n))
    table = pa.table(
        {
            "LOT_NO": list(lots),
            "USE": use,
            "PLANNED_VALUE": [float(value[u] * rng.uniform(0.9, 1.1)) for u in use],
            "FIN_FLOOR_FT": [2.0 if u != "RETAIL" else 1.0 for u in use],
            "STORIES": [1 if u == "RETAIL" else 2 for u in use],
            "FOUND": ["S"] * n,
            "BLOCK_GEOID": [block] * n,
            "geometry": shapely.to_wkb(shapely.points(coords)).tolist(),
        }
    )
    out = Path(out_parquet)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out)
    return lots


def generate_all(
    nsi_path: str | Path,
    out_dir: str | Path,
    *,
    seed: int = 11,
    max_parcels: int | None = None,
    n_survey: int = 300,
) -> SyntheticSources:
    """Generate every demo source into `out_dir` (deterministic per seed)."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    assessor = make_county_assessor(nsi_path, out, max_parcels=max_parcels, seed=seed)
    survey_path = out / "field-survey-2025.csv"
    survey = make_field_survey(nsi_path, survey_path, n=n_survey, seed=seed + 12)
    riverbend_path = out / "project-riverbend.parquet"
    lots = make_project_riverbend(nsi_path, riverbend_path, seed=seed + 20)
    sources = SyntheticSources(
        out_dir=str(out),
        assessor=assessor,
        survey=str(survey_path),
        survey_fd_id=survey,
        riverbend=str(riverbend_path),
        riverbend_lots=lots,
        seed=seed,
    )
    (out / "truth.json").write_text(json.dumps(sources.to_dict(), indent=2) + "\n")
    return sources


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--nsi", type=Path, required=True, help="NSI sample parquet.")
    ap.add_argument("--out", type=Path, required=True, help="Output directory.")
    ap.add_argument("--seed", type=int, default=11)
    args = ap.parse_args(argv)
    sources = generate_all(args.nsi, args.out, seed=args.seed)
    print(
        f"OK: wrote {Path(sources.assessor.release1).name}, "
        f"{Path(sources.assessor.release2).name}, {Path(sources.survey).name}, "
        f"{Path(sources.riverbend).name} and truth.json to {args.out}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
