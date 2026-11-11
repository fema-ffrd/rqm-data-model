"""Source mapping specs: how a building inventory maps onto `buildings`
(ADR-multi-source).

Every inventory (NSI, a county assessor roll, a field survey, a project
inventory, ...) is loaded side by side into `buildings`, scoped by its
`source_ref`, using the NSI field set as the shared column schema. One
declarative YAML spec per source says how to read its file and how its
columns, codes, units, CRS, and ids map onto that schema:

    source_ref: COUNTY-ASSESSOR          # dataset lineage, stable across releases
    source_name: Example County Assessor Parcels
    source_kind: local_inventory         # enum source_kind
    description: ...
    input:
      format: gpkg                       # parquet | geoparquet | csv | gpkg
      layer: parcels                     # gpkg only
      crs: EPSG:2276
      geometry: {kind: polygon, column: geom}    # or {kind: point, lon: LON, lat: LAT}
    id: {column: PARCEL_ID}
    fields:                              # target buildings column -> rule
      occtype:    {from: LANDUSE, map: {A1: RES1, F1: COM1}, default: RES1}
      val_struct: {expr: "IMPR_VAL * 1.0"}   # Spark SQL expression
      found_ht:   {expr: "FFE_IN / 12.0"}    # unit conversion
      num_story:  STORIES                    # shorthand for {from: STORIES}

A field rule is exactly one of `from` (rename), `expr` (Spark SQL
expression over the input columns), or `from` + `map` (code crosswalk),
optionally with a `default` that replaces NULL (a rule with only a
`default` is a constant). Values are cast to the `buildings` column type;
unmapped fields are NULL. `x` / `y` (WGS84 lon / lat), when not mapped, are
derived from the computation point. `expr` is executed by Spark SQL, so a
spec is trusted configuration, like code.

`parse_spec` / `load_spec` validate a spec in pure Python, before Spark
starts; `ingest_source` checks the input columns and ids at load time.
`tools/sources/nsi.yaml` is the built-in NSI spec (identity mapping, native
`fd_id` ids).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent
SPEC_DIR = HERE / "sources"
NSI_SPEC_PATH = SPEC_DIR / "nsi.yaml"

# Mirrors ingest_nsi.NSI_SOURCE_REF (kept import-free so specs validate
# without importing PySpark).
NSI_SOURCE_REF = "NSI"

SOURCE_REF_RE = re.compile(r"^[A-Z0-9][A-Z0-9_.-]{0,63}$")
EPSG_RE = re.compile(r"^EPSG:[1-9][0-9]{3,5}$")

# Enum source_kind (data-dictionary.yaml).
SOURCE_KINDS: tuple[str, ...] = (
    "national_inventory",
    "local_inventory",
    "commercial_inventory",
    "project_inventory",
    "field_survey",
    "remote_sensing",
    "other",
)
INPUT_FORMATS: tuple[str, ...] = ("parquet", "geoparquet", "csv", "gpkg")
GEOMETRY_KINDS: tuple[str, ...] = ("point", "polygon")
# `hash`: building_id = xxhash64(source_ref, id) with the sign bit set.
# `native`: building_id = the integer id itself (built-in NSI spec only).
ID_SCHEMES: tuple[str, ...] = ("hash", "native")

# Enums of the coded buildings columns (data-dictionary.yaml `enums:`).
OCCTYPE_CODES: tuple[str, ...] = (
    "RES1", "RES2", "RES3A", "RES3B", "RES3C", "RES3D", "RES3E", "RES3F",
    "RES4", "RES5", "RES6",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "COM10",
    "IND1", "IND2", "IND3", "IND4", "IND5", "IND6",
    "AGR1", "GOV1", "GOV2", "EDU1", "EDU2", "REL1",
)  # fmt: skip
ST_DAMCAT_CODES: tuple[str, ...] = ("RES", "COM", "IND", "PUB", "AGR")
FOUND_TYPE_CODES: tuple[str, ...] = ("S", "C", "B", "P", "F")
ENUM_FIELDS: Mapping[str, tuple[str, ...]] = {
    "occtype": OCCTYPE_CODES,
    "st_damcat": ST_DAMCAT_CODES,
    "found_type": FOUND_TYPE_CODES,
}
# NSI qualifies RES1 with stories and basement, e.g. RES1-1SNB, RES1-2SWB.
_OCCTYPE_QUALIFIED_RE = re.compile(r"^([A-Z0-9]+)-[A-Z0-9]+$")

# Target type of every mappable buildings column (ingest_nsi.BUILDINGS_CAST);
# the computed columns building_id, source_ref, source_record_id, and
# ingested_at cannot be mapped.
MAPPABLE_FIELDS: Mapping[str, str] = {
    "fd_id": "bigint", "bid": "string", "usastrucid": "string",
    "ftprntid": "string", "ftprntsrc": "string", "source": "string",
    "occtype": "string", "st_damcat": "string", "bldgtype": "string",
    "num_story": "smallint", "sqft": "double", "ftprntsqft": "double",
    "bldheight": "float", "med_yr_blt": "smallint", "found_type": "string",
    "found_ht": "float", "val_struct": "double", "val_cont": "double",
    "val_vehic": "double", "ornl_low": "double", "ornl_med": "double",
    "ornl_hgh": "double", "resunits": "float", "novehprob": "float",
    "vehperunit": "float", "pop2amu65": "float", "pop2amo65": "float",
    "pop2pmu65": "float", "pop2pmo65": "float", "u65disable": "float",
    "o65disable": "float", "students": "float", "firmzone": "string",
    "zone_sub": "string", "static_bfe": "float", "ground_elv": "float",
    "grnd_elv_m": "float", "pctlowclr": "float", "fullrep": "float",
    "depindex": "float", "creprcnt": "float", "crerank": "float",
    "cbfips": "string", "x": "double", "y": "double",
}  # fmt: skip


class SpecError(ValueError):
    """A source spec is invalid; `problems` lists every issue found."""

    def __init__(self, problems: list[str], where: str = "source spec") -> None:
        self.problems = list(problems)
        super().__init__(
            f"{where} is invalid:\n" + "\n".join(f"  - {p}" for p in self.problems)
        )


@dataclass(frozen=True)
class FieldRule:
    """How one `buildings` column is computed from the input row."""

    target: str
    source: str | None = None  # YAML `from`
    expr: str | None = None
    mapping: Mapping[str, Any] | None = None  # YAML `map`; keys are strings
    default: Any = None

    @property
    def input_columns(self) -> tuple[str, ...]:
        """Input columns the rule reads by name (not those inside `expr`)."""
        return (self.source,) if self.source else ()


@dataclass(frozen=True)
class GeometrySpec:
    """Where the structure geometry comes from."""

    kind: str  # point | polygon
    column: str | None = None
    lon: str | None = None
    lat: str | None = None
    # Input column whose value suffixes geometries.source_ref
    # (`<source_ref>-<value>`, e.g. NSI-P); None means plain `<source_ref>`.
    subcode: str | None = None

    @property
    def input_columns(self) -> tuple[str, ...]:
        cols = (self.column,) if self.column else (self.lon, self.lat)
        return tuple(c for c in (*cols, self.subcode) if c)


@dataclass(frozen=True)
class InputSpec:
    format: str
    crs: str
    geometry: GeometrySpec
    layer: str | None = None
    options: Mapping[str, str] = field(default_factory=dict)  # Spark reader options


@dataclass(frozen=True)
class IdSpec:
    column: str
    scheme: str = "hash"


@dataclass(frozen=True)
class SourceSpec:
    """A validated source mapping spec (see the module docstring)."""

    source_ref: str
    source_name: str
    source_kind: str
    description: str | None
    input: InputSpec
    id: IdSpec
    fields: tuple[FieldRule, ...]
    sha256: str | None = None
    path: Path | None = None
    builtin: bool = False

    @property
    def is_nsi(self) -> bool:
        return self.source_ref == NSI_SOURCE_REF

    @property
    def rules(self) -> dict[str, FieldRule]:
        return {rule.target: rule for rule in self.fields}

    @property
    def input_columns(self) -> set[str]:
        """Input columns the spec reads by name (checked at load time)."""
        cols = {self.id.column, *self.input.geometry.input_columns}
        for rule in self.fields:
            cols.update(rule.input_columns)
        return cols

    @property
    def geometry_source_refs(self) -> str:
        """Human-readable pattern of the geometries.source_ref values it writes."""
        if self.input.geometry.subcode:
            return f"{self.source_ref}-<{self.input.geometry.subcode}>"
        return self.source_ref


# ---------------------------------------------------------------------------
# Parsing and validation
# ---------------------------------------------------------------------------

_TOP_KEYS = {
    "source_ref", "source_name", "source_kind", "description", "input", "id", "fields",
}  # fmt: skip
_INPUT_KEYS = {"format", "layer", "crs", "geometry", "options"}
_GEOMETRY_KEYS = {"kind", "column", "lon", "lat", "subcode"}
_ID_KEYS = {"column", "scheme"}
_RULE_KEYS = {"from", "expr", "map", "default"}


def valid_code(target: str, code: Any) -> bool:
    """True if `code` is a valid value of the enum column `target`."""
    codes = ENUM_FIELDS[target]
    if not isinstance(code, str):
        return False
    if code in codes:
        return True
    qualified = _OCCTYPE_QUALIFIED_RE.match(code)
    return target == "occtype" and bool(qualified) and qualified.group(1) in codes


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _check_value(target: str, value: Any, what: str, problems: list[str]) -> None:
    """Check a literal (map value or default) against the target column."""
    if value is None:
        return
    if target in ENUM_FIELDS:
        if not valid_code(target, value):
            problems.append(
                f"fields.{target}: {what} {value!r} is not a valid {target} code "
                f"(enum {target}: {', '.join(ENUM_FIELDS[target])})"
            )
    elif MAPPABLE_FIELDS[target] == "string":
        if not isinstance(value, str | int):
            problems.append(f"fields.{target}: {what} {value!r} must be a string")
    elif not _is_number(value):
        problems.append(f"fields.{target}: {what} {value!r} must be a number")


def _str(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _unknown_keys(
    data: Mapping, allowed: set[str], where: str, problems: list[str]
) -> None:
    for key in sorted(set(map(str, data)) - allowed):
        problems.append(
            f"{where}: unknown key {key!r} (allowed: {', '.join(sorted(allowed))})"
        )


def _parse_rule(target: str, raw: Any, problems: list[str]) -> FieldRule | None:
    where = f"fields.{target}"
    if isinstance(raw, str):
        raw = {"from": raw}
    if not isinstance(raw, Mapping):
        problems.append(f"{where}: expected a column name or a mapping, got {raw!r}")
        return None
    _unknown_keys(raw, _RULE_KEYS, where, problems)
    source, expr, mapping = raw.get("from"), raw.get("expr"), raw.get("map")
    default = raw.get("default")
    if source is not None and _str(source) is None:
        problems.append(f"{where}: 'from' must be a column name")
    if expr is not None and _str(expr) is None:
        problems.append(f"{where}: 'expr' must be a non-empty Spark SQL expression")
    if source is not None and expr is not None:
        problems.append(f"{where}: use exactly one of 'from' or 'expr'")
    if mapping is not None:
        if source is None:
            problems.append(f"{where}: 'map' needs 'from' (the input code column)")
        if not isinstance(mapping, Mapping) or not mapping:
            problems.append(f"{where}: 'map' must be a non-empty mapping of codes")
            mapping = None
    if source is None and expr is None and default is None:
        problems.append(f"{where}: needs 'from', 'expr', or 'default'")
    if mapping is not None:
        mapping = {str(k): v for k, v in mapping.items()}
        for value in mapping.values():
            _check_value(target, value, "map value", problems)
    _check_value(target, default, "default", problems)
    return FieldRule(target, source, expr, mapping, default)


def _parse_geometry(raw: Any, fmt: Any, problems: list[str]) -> GeometrySpec | None:
    if not isinstance(raw, Mapping):
        problems.append(
            "input.geometry: required mapping {kind, column} or {kind, lon, lat}"
        )
        return None
    _unknown_keys(raw, _GEOMETRY_KEYS, "input.geometry", problems)
    kind = raw.get("kind")
    column, lon, lat = raw.get("column"), raw.get("lon"), raw.get("lat")
    subcode = raw.get("subcode")
    if kind not in GEOMETRY_KINDS:
        problems.append(
            f"input.geometry.kind: {kind!r} is not one of {', '.join(GEOMETRY_KINDS)}"
        )
    has_lonlat = lon is not None or lat is not None
    if column is not None and has_lonlat:
        problems.append(
            "input.geometry: give either 'column' or 'lon' + 'lat', not both"
        )
    elif column is None and not has_lonlat:
        problems.append(
            "input.geometry: 'column' (or 'lon' + 'lat' for points) is required"
        )
    elif has_lonlat:
        if _str(lon) is None or _str(lat) is None:
            problems.append("input.geometry: 'lon' and 'lat' must both be column names")
        if kind == "polygon":
            problems.append(
                "input.geometry: polygons need a geometry 'column', not lon/lat"
            )
        if fmt in ("gpkg", "geoparquet"):
            problems.append(f"input.geometry: {fmt} inputs need a geometry 'column'")
    elif _str(column) is None:
        problems.append("input.geometry.column must be a column name")
    if subcode is not None and _str(subcode) is None:
        problems.append("input.geometry.subcode must be a column name")
    return GeometrySpec(kind, column, lon, lat, subcode)


def parse_spec(
    data: Any,
    *,
    sha256: str | None = None,
    path: Path | None = None,
    builtin: bool = False,
) -> SourceSpec:
    """Validate a spec given as parsed YAML; raise `SpecError` listing every problem."""
    problems: list[str] = []
    where = f"source spec {path}" if path else "source spec"
    if not isinstance(data, Mapping):
        raise SpecError(["the spec must be a YAML mapping"], where)
    _unknown_keys(data, _TOP_KEYS, "spec", problems)

    source_ref = data.get("source_ref")
    if not isinstance(source_ref, str) or not SOURCE_REF_RE.match(source_ref):
        problems.append(
            f"source_ref: {source_ref!r} must match {SOURCE_REF_RE.pattern} "
            "(upper-case letters, digits, '.', '_', '-')"
        )
    elif not builtin and (
        source_ref == NSI_SOURCE_REF or source_ref.startswith(f"{NSI_SOURCE_REF}-")
    ):
        # NSI geometries are tagged NSI-<subcode>, so the prefix is NSI's too.
        problems.append(
            f"source_ref: {NSI_SOURCE_REF!r} and {NSI_SOURCE_REF + '-*'!r} are "
            "reserved for the built-in NSI spec"
        )
    source_name = _str(data.get("source_name"))
    if source_name is None:
        problems.append("source_name: required")
    source_kind = data.get("source_kind")
    if source_kind not in SOURCE_KINDS:
        problems.append(
            f"source_kind: {source_kind!r} is not one of {', '.join(SOURCE_KINDS)}"
        )
    description = data.get("description")
    if description is not None and not isinstance(description, str):
        problems.append("description: must be text")

    raw_input = data.get("input")
    input_spec = None
    if not isinstance(raw_input, Mapping):
        problems.append("input: required mapping {format, crs, geometry}")
    else:
        _unknown_keys(raw_input, _INPUT_KEYS, "input", problems)
        fmt, crs = raw_input.get("format"), raw_input.get("crs")
        layer, options = raw_input.get("layer"), raw_input.get("options") or {}
        if fmt not in INPUT_FORMATS:
            problems.append(
                f"input.format: {fmt!r} is not one of {', '.join(INPUT_FORMATS)}"
            )
        if not isinstance(crs, str) or not EPSG_RE.match(crs):
            problems.append(
                f"input.crs: {crs!r} is not an EPSG string such as EPSG:4326"
            )
        if fmt == "gpkg" and _str(layer) is None:
            problems.append("input.layer: required for gpkg inputs")
        if layer is not None and fmt != "gpkg":
            problems.append("input.layer: only applies to gpkg inputs")
        if not isinstance(options, Mapping):
            problems.append("input.options: must be a mapping of Spark reader options")
            options = {}
        geometry = _parse_geometry(raw_input.get("geometry"), fmt, problems)
        if geometry is not None:
            input_spec = InputSpec(
                fmt, crs, geometry, layer, {str(k): str(v) for k, v in options.items()}
            )

    raw_id = data.get("id")
    id_spec = None
    if not isinstance(raw_id, Mapping) or _str(raw_id.get("column")) is None:
        problems.append("id: required mapping {column: <native id column>}")
    else:
        _unknown_keys(raw_id, _ID_KEYS, "id", problems)
        scheme = raw_id.get("scheme", "hash")
        if scheme not in ID_SCHEMES:
            problems.append(
                f"id.scheme: {scheme!r} is not one of {', '.join(ID_SCHEMES)}"
            )
        elif scheme == "native" and not builtin:
            problems.append(
                "id.scheme: only the built-in NSI spec may use 'native' ids"
            )
        id_spec = IdSpec(raw_id["column"], scheme)

    raw_fields = data.get("fields")
    rules: list[FieldRule] = []
    if not isinstance(raw_fields, Mapping) or not raw_fields:
        problems.append("fields: required mapping of buildings column -> rule")
    else:
        for target, raw in raw_fields.items():
            if target not in MAPPABLE_FIELDS:
                problems.append(
                    f"fields.{target}: not a mappable buildings column "
                    "(building_id, source_ref, source_record_id, and ingested_at "
                    "are computed)"
                )
                continue
            rule = _parse_rule(target, raw, problems)
            if rule is not None:
                rules.append(rule)
        targets = {r.target for r in rules}
        if "occtype" not in raw_fields:
            problems.append(
                "fields.occtype: required (map it or give a default; "
                "ADR-nsi-minimal rule 5)"
            )
        if ("x" in targets) != ("y" in targets):
            problems.append("fields: map both x and y (WGS84 lon / lat) or neither")

    if problems:
        raise SpecError(problems, where)
    return SourceSpec(
        source_ref=source_ref,
        source_name=source_name,
        source_kind=source_kind,
        description=description,
        input=input_spec,
        id=id_spec,
        fields=tuple(rules),
        sha256=sha256,
        path=path,
        builtin=builtin,
    )


def load_spec(path: str | Path, *, builtin: bool = False) -> SourceSpec:
    """Read and validate a YAML spec file; `sha256` is the file's hash."""
    path = Path(path)
    raw = path.read_bytes()
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as err:
        raise SpecError([f"not valid YAML: {err}"], f"source spec {path}") from err
    return parse_spec(
        data, sha256=hashlib.sha256(raw).hexdigest(), path=path, builtin=builtin
    )


@cache
def nsi_spec() -> SourceSpec:
    """The built-in NSI spec (`tools/sources/nsi.yaml`)."""
    return load_spec(NSI_SPEC_PATH, builtin=True)
