"""Read helpers for source sidecar attributes and canonical-field lineage
(ADR-source-sidecar-attributes).

One function per question:

- `buildings_with_sidecar`: buildings with their source-native extra fields
  (`source_attributes.attributes`, NULL for buildings without any);
- `lineage_for`: which load and which exact spec produced a building, with
  the spec document attached;
- `source_of`: which source column (or expression, crosswalk, default)
  produced `buildings.<field>` for a building.

`version=None` reads the head of `branch` (default main); a version name
reads the tables exactly as recorded for that version. `source_specs` is
read at the head of main (it is append-only and never branched).
"""

from __future__ import annotations

import json
from typing import Any

from ingest_nsi import NAMESPACE, conform_buildings, table_name
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sources import COMPUTED_FIELDS, MAPPABLE_FIELDS

# Parsed spec documents by spec_sha256. A hash names one immutable document,
# so the cache never goes stale.
_SPEC_CACHE: dict[str, dict] = {}


def _table(
    spark: SparkSession,
    catalog: str,
    table: str,
    version: str | None,
    branch: str | None,
    namespace: str,
) -> DataFrame:
    if version is not None:
        from versioning import read_version

        return read_version(spark, catalog, version, table, namespace)
    df = spark.table(table_name(catalog, table, namespace, branch))
    return conform_buildings(df) if table == "buildings" else df


def buildings_with_sidecar(
    spark: SparkSession,
    catalog: str,
    version: str | None = None,
    source_ref: str | None = None,
    *,
    branch: str | None = None,
    namespace: str = NAMESPACE,
) -> DataFrame:
    """`buildings` LEFT JOIN `source_attributes`: every buildings column plus
    `attributes` (MAP<STRING, STRING>, NULL when the building has no sidecar
    values). `source_ref` keeps one source. Cast values with the declared
    types in `sources.sidecar_schema`, e.g.
    `CAST(attributes['premium_usd'] AS DOUBLE)`.
    """
    buildings = _table(spark, catalog, "buildings", version, branch, namespace)
    attributes = _table(
        spark, catalog, "source_attributes", version, branch, namespace
    ).select("building_id", "attributes")
    if source_ref is not None:
        buildings = buildings.where(F.col("source_ref") == source_ref)
    return buildings.join(attributes, "building_id", "left")


def lineage_for(
    spark: SparkSession,
    catalog: str,
    building_id: int,
    version: str | None = None,
    *,
    branch: str | None = None,
    namespace: str = NAMESPACE,
) -> dict | None:
    """The `building_lineage` row of one building joined to its spec:
    building_id, source_ref, source_record_id, source_version_name,
    spec_sha256, ingested_at, spec_uri, spec_document, and `spec` (the parsed
    document). None if the building has no lineage row.
    """
    lineage = _table(spark, catalog, "building_lineage", version, branch, namespace)
    specs = spark.table(table_name(catalog, "source_specs", namespace)).select(
        "spec_sha256", "spec_uri", "spec_document"
    )
    rows = (
        lineage.where(F.col("building_id") == int(building_id))
        .join(specs, "spec_sha256", "left")
        .collect()
    )
    if not rows:
        return None
    row = rows[0].asDict()
    row["spec"] = (
        _parsed(row["spec_sha256"], row["spec_document"])
        if row["spec_document"] is not None
        else None
    )
    return row


def _parsed(sha256: str, document: str) -> dict:
    if sha256 not in _SPEC_CACHE:
        _SPEC_CACHE[sha256] = json.loads(document)
    return _SPEC_CACHE[sha256]


def field_rule(spec: dict, target_field: str) -> dict[str, Any] | None:
    """The normalized `fields:` rule of `target_field` in a parsed spec
    document (string shorthand -> `{"from": ...}`), or None if unmapped."""
    rule = (spec.get("fields") or {}).get(target_field)
    if rule is None:
        return None
    return {"from": rule} if isinstance(rule, str) else dict(rule)


def source_of(
    spark: SparkSession,
    catalog: str,
    building_id: int,
    target_field: str,
    version: str | None = None,
    *,
    branch: str | None = None,
    namespace: str = NAMESPACE,
) -> dict | None:
    """Which source column populated `buildings.<target_field>` of a building?

    Returns building_id, source_ref, source_record_id, source_version_name,
    spec_sha256, spec_uri, target_field, `rule` (the spec's field rule:
    `from` / `expr` / `map` / `default`, or None when the spec does not map
    the field), `source_columns` (the input columns the rule reads by name),
    and `note`. None if the building has no lineage row.
    """
    if target_field not in MAPPABLE_FIELDS and target_field not in COMPUTED_FIELDS:
        raise ValueError(f"{target_field!r} is not a buildings column")
    lineage = lineage_for(
        spark, catalog, building_id, version, branch=branch, namespace=namespace
    )
    if lineage is None:
        return None
    spec = lineage["spec"] or {}
    rule = field_rule(spec, target_field)
    note = None
    if target_field in COMPUTED_FIELDS:
        id_column = (spec.get("id") or {}).get("column")
        rule = None
        note = {
            "building_id": "computed from the source id (id.column "
            f"{id_column!r}; ADR-multi-source)",
            "source_ref": "the spec's source_ref",
            "source_record_id": f"the source id column {id_column!r}",
            "ingested_at": "load time",
        }[target_field]
    elif rule is None and target_field in ("x", "y"):
        note = "unmapped: derived from the computation point (WGS84 lon / lat)"
    elif rule is None:
        note = "unmapped: NULL for this source"
    elif "expr" in rule:
        note = "Spark SQL expression over the input columns"
    elif "map" in rule:
        note = "code crosswalk of the input column"
    elif "from" not in rule:
        note = "constant default"
    return {
        "building_id": lineage["building_id"],
        "source_ref": lineage["source_ref"],
        "source_record_id": lineage["source_record_id"],
        "source_version_name": lineage["source_version_name"],
        "spec_sha256": lineage["spec_sha256"],
        "spec_uri": lineage["spec_uri"],
        "target_field": target_field,
        "rule": rule,
        "source_columns": [rule["from"]] if rule and "from" in rule else [],
        "note": note,
    }
