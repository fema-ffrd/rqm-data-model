"""Source spec parsing and validation (ADR-multi-source); no Spark needed."""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

TEST_DIR = Path(__file__).resolve().parent
REPO = TEST_DIR.parent
DEMO_SPECS = sorted((TEST_DIR / "data" / "sources").glob("*.yaml"))

from sources import (  # type: ignore[import-not-found]
    ENUM_FIELDS,
    MAPPABLE_FIELDS,
    SOURCE_KINDS,
    SpecError,
    load_spec,
    nsi_spec,
    parse_spec,
    valid_code,
)

VALID = {
    "source_ref": "COUNTY-ASSESSOR",
    "source_name": "Example County Assessor Parcels",
    "source_kind": "local_inventory",
    "input": {
        "format": "gpkg",
        "layer": "parcels",
        "crs": "EPSG:2276",
        "geometry": {"kind": "polygon", "column": "geom"},
    },
    "id": {"column": "PARCEL_ID"},
    "fields": {
        "occtype": {"from": "LANDUSE", "map": {"A1": "RES1"}, "default": "RES1"},
        "val_struct": {"expr": "IMPR_VAL * 1.0"},
        "found_ht": {"expr": "FFE_IN / 12.0"},
        "found_type": {"from": "FOUND", "map": {"SLAB": "S", "CRAWL": "C"}},
        "num_story": "STORIES",
    },
}


def spec_with(**changes) -> dict:
    """VALID with dotted-path changes; a value of None deletes the key."""
    data = copy.deepcopy(VALID)
    for path, value in changes.items():
        *parents, leaf = path.split("__")
        node = data
        for key in parents:
            node = node[key]
        if value is None:
            node.pop(leaf, None)
        else:
            node[leaf] = value
    return data


def problems(data) -> list[str]:
    with pytest.raises(SpecError) as err:
        parse_spec(data)
    return err.value.problems


def test_valid_spec_parses() -> None:
    spec = parse_spec(VALID)
    assert spec.source_ref == "COUNTY-ASSESSOR"
    assert spec.id.scheme == "hash"
    assert spec.input.geometry.kind == "polygon"
    rules = spec.rules
    assert rules["num_story"].source == "STORIES"  # string shorthand = from
    assert rules["occtype"].mapping == {"A1": "RES1"}
    assert rules["val_struct"].expr == "IMPR_VAL * 1.0"
    assert spec.input_columns == {
        "PARCEL_ID", "geom", "LANDUSE", "FOUND", "STORIES"
    }  # fmt: skip


def test_builtin_nsi_spec_is_the_identity_mapping() -> None:
    from ingest_nsi import (  # type: ignore[import-not-found]
        BUILDINGS_CAST,
        NSI_SCALAR_FIELDS,
        NSI_SOURCE_REF,
    )

    spec = nsi_spec()
    assert spec.builtin and spec.is_nsi and spec.source_ref == NSI_SOURCE_REF
    assert (spec.id.column, spec.id.scheme) == ("fd_id", "native")
    assert [r.target for r in spec.fields] == NSI_SCALAR_FIELDS
    assert all(r.source == r.target for r in spec.fields)
    assert spec.input.geometry.subcode == "source"
    assert spec.sha256 and len(spec.sha256) == 64
    # sources.py mirrors the mappable columns without importing PySpark.
    assert dict(MAPPABLE_FIELDS) == BUILDINGS_CAST


@pytest.mark.parametrize("path", DEMO_SPECS, ids=lambda p: p.stem)
def test_demo_specs_are_valid(path: Path) -> None:
    spec = load_spec(path)
    assert spec.path == path and not spec.builtin


def test_enums_match_the_data_dictionary() -> None:
    dictionary = yaml.safe_load(
        (REPO / "data-dictionary" / "data-dictionary.yaml").read_text()
    )
    enums = dictionary["enums"]
    for name, codes in ENUM_FIELDS.items():
        assert list(codes) == enums[name], name
    assert list(SOURCE_KINDS) == enums["source_kind"]


def test_invalid_crosswalk_code_is_rejected() -> None:
    bad = spec_with(fields__occtype={"from": "LANDUSE", "map": {"A1": "RES9"}})
    [problem] = problems(bad)
    assert "fields.occtype" in problem and "'RES9'" in problem


def test_nsi_qualified_occupancy_codes_are_valid() -> None:
    assert valid_code("occtype", "RES1-1SNB")
    assert valid_code("occtype", "COM1")
    assert not valid_code("occtype", "RES9-1SNB")
    assert not valid_code("found_type", "S-1")


def test_every_problem_is_reported_at_once() -> None:
    bad = spec_with(
        source_ref="county assessor",
        source_kind="county",
        input__crs="2276",
        input__geometry={"kind": "line", "column": "geom"},
        fields__found_type={"from": "FOUND", "map": {"SLAB": "SLAB"}},
        fields__building_id="PARCEL_ID",
        fields__val_struct={"from": "IMPR_VAL", "expr": "IMPR_VAL * 1.0"},
    )
    found = "\n".join(problems(bad))
    for expected in (
        "source_ref:",
        "source_kind:",
        "input.crs:",
        "input.geometry.kind:",
        "fields.found_type: map value 'SLAB'",
        "fields.building_id: not a mappable",
        "fields.val_struct: use exactly one of",
    ):
        assert expected in found


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"fields__occtype": None}, "fields.occtype: required"),
        ({"source_ref": "NSI"}, "reserved for the built-in NSI spec"),
        ({"source_ref": "NSI-SURVEY"}, "reserved for the built-in NSI spec"),
        ({"id": {"column": "PARCEL_ID", "scheme": "native"}}, "only the built-in NSI"),
        ({"id": {"scheme": "hash"}}, "id: required"),
        ({"input__layer": None}, "input.layer: required for gpkg"),
        ({"input__format": "shp"}, "input.format:"),
        (
            {"input__geometry": {"kind": "polygon", "lon": "X", "lat": "Y"}},
            "polygons need a geometry 'column'",
        ),
        ({"fields__x": "LON"}, "map both x and y"),
        (
            {"fields__num_story": {"from": "STORIES", "default": "two"}},
            "must be a number",
        ),
        ({"fields__occtype": {"map": {"A1": "RES1"}}}, "'map' needs 'from'"),
        ({"fields__val_struct": {"expr": "  "}}, "'expr' must be a non-empty"),
        ({"surprise": 1}, "unknown key 'surprise'"),
        ({"fields__num_story": {"form": "STORIES"}}, "unknown key 'form'"),
    ],
)
def test_spec_validation_rules(changes: dict, expected: str) -> None:
    assert any(expected in p for p in problems(spec_with(**changes))), problems(
        spec_with(**changes)
    )


def test_default_only_rule_is_a_constant() -> None:
    spec = parse_spec(spec_with(fields__occtype={"default": "RES1"}))
    rule = spec.rules["occtype"]
    assert (rule.source, rule.expr, rule.default) == (None, None, "RES1")


def test_csv_lonlat_points_and_numeric_map_keys() -> None:
    spec = parse_spec(
        spec_with(
            input={
                "format": "csv",
                "crs": "EPSG:4326",
                "geometry": {"kind": "point", "lon": "LON", "lat": "LAT"},
            },
            fields__occtype={"from": "USE", "map": {1: "RES1", 2: "COM1"}},
        )
    )
    assert spec.rules["occtype"].mapping == {"1": "RES1", "2": "COM1"}
    assert {"LON", "LAT", "USE"} <= spec.input_columns


def test_load_spec_reports_yaml_errors(tmp_path: Path) -> None:
    path = tmp_path / "broken.yaml"
    path.write_text("source_ref: [unclosed\n")
    with pytest.raises(SpecError, match="not valid YAML"):
        load_spec(path)


@pytest.mark.parametrize("network", [False, True])
def test_synthetic_assessor_transform_ignores_proj_network(network: bool) -> None:
    """The synthetic assessor footprints use the NAD83 = WGS84 identity
    datum step, like Sedona's ST_Transform, whatever PROJ grids or network
    access are available. With PROJ's own choice, PROJ_NETWORK=ON shifts
    them ~0.45 m and the loaded points no longer land on the NSI points.
    """
    np = pytest.importorskip("numpy")
    pyproj = pytest.importorskip("pyproj")
    from synthetic_sources import (  # type: ignore[import-not-found]
        web_mercator_to_assessor_crs,
    )

    before = pyproj.network.is_network_enabled()
    pyproj.network.set_network_enabled(network)
    try:
        x, y = web_mercator_to_assessor_crs(
            np.array([-10736764.887]), np.array([3882038.146])
        )
    finally:
        pyproj.network.set_network_enabled(before)
    assert (x[0], y[0]) == (
        pytest.approx(2597629.2605, abs=1e-3),
        pytest.approx(7016504.8979, abs=1e-3),
    )
