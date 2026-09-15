#!/usr/bin/env python
"""Render data-dictionary.md from data-dictionary.yaml and validate integrity.

The YAML is the single source of truth; the Markdown is generated. Also validates that every
foreign key points to an existing table.column.
"""
import sys
import yaml
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "data-dictionary.yaml"
OUT = HERE / "data-dictionary.md"

DOMAIN_TITLES = {
    "inventory": "A — Inventory (coverage of building inventory / components)",
    "attribute_uncertainty": "B — Attribute Uncertainty (uncertainty quantification)",
    "hazard_linkage": "C — Hazard Linkage (probabilistic flood depth)",
    "ddf": "D — Depth-Damage Functions (probabilistic DDFs)",
    "realization_results": "E — Realization & Loss Results (ensemble scale)",
    "provenance": "F — Provenance & Versioning",
    "exports": "G — Exports & Contracts",
}

TIER_BADGE = {"postgres": "PostgreSQL", "iceberg": "Iceberg", "icechunk": "Icechunk"}


def validate(model):
    """Static schema-level checks. Runtime data-content checks live in the ADRs
    (numbered rules per ADR) and are enforced at ingest/compile time — this
    validator only sees the YAML.
    """
    tables = model["tables"]
    declared_domains = set(model["metadata"].get("domains", []))
    colindex = {t: {c["name"] for c in tables[t]["columns"]} for t in tables}
    errors = []
    for tname, t in tables.items():
        # Table-level checks
        if not (t.get("description") or "").strip():
            errors.append(f"[{tname}] table is missing a description")
        if not (t.get("grain") or "").strip():
            errors.append(f"[{tname}] table is missing a grain")
        if declared_domains and t.get("domain") not in declared_domains:
            errors.append(
                f"[{tname}] domain '{t.get('domain')}' not declared in metadata.domains"
            )
        if t.get("domain") not in DOMAIN_TITLES:
            errors.append(
                f"[{tname}] domain '{t.get('domain')}' has no DOMAIN_TITLES entry — "
                "table would not render in the generated Markdown"
            )
        pks = [c["name"] for c in t["columns"] if c.get("pk")]
        if t["storage"] == "postgres" and not pks:
            errors.append(f"[{tname}] postgres table has no primary key")
        # Column-level checks
        for c in t["columns"]:
            if not c.get("name"):
                errors.append(f"[{tname}] column missing 'name'")
                continue
            if not (c.get("type") or "").strip():
                errors.append(f"[{tname}.{c['name']}] missing type")
            if not (c.get("description") or "").strip():
                errors.append(f"[{tname}.{c['name']}] missing description")
            fk = c.get("fk")
            if fk:
                if "." not in fk:
                    errors.append(f"[{tname}.{c['name']}] malformed fk '{fk}'")
                    continue
                ft, fc = fk.split(".", 1)
                if ft not in tables:
                    errors.append(f"[{tname}.{c['name']}] fk -> unknown table '{ft}'")
                elif fc not in colindex[ft]:
                    errors.append(
                        f"[{tname}.{c['name']}] fk -> unknown column '{ft}.{fc}'"
                    )
        # SQL supertype/subtable integrity: any column that is BOTH pk and fk
        # must FK a superclass whose own PK column has the same name (identity
        # inheritance). This is the pattern shared by uncertainty_spec_* and
        # generic_building_component (see ADR-uncertainty, ADR-generics).
        for c in t["columns"]:
            if c.get("pk") and c.get("fk"):
                fk = c["fk"]
                if "." in fk:
                    ft, fc = fk.split(".", 1)
                    if ft in tables and fc != c["name"]:
                        errors.append(
                            f"[{tname}.{c['name']}] supertype/subtable pattern "
                            f"expects the FK column to share its parent's PK name "
                            f"('{fc}' != '{c['name']}')"
                        )
    return errors


def render(model):
    m = model["metadata"]
    lines = []
    lines.append(f"# {m['model_name']} — Data Dictionary\n")
    lines.append(
        f"**Version:** {m['version']} &nbsp;|&nbsp; "
        f"**Companion:** `{m['companion_repo']}`\n"
    )
    lines.append(
        "Deterministic building-loss inputs (flood depth, foundation type, "
        "first-floor height, depth-damage function) are re-architected as "
        "first-class, versioned **distribution** objects for probabilistic risk "
        "assessment. Storage tiers are inherited from the FFRD data model: "
        "**PostgreSQL** (relational integrity), **Iceberg** (ensemble-scale "
        "results), **Icechunk/Zarr** (gridded hazard fields).\n"
    )

    # Table of contents
    lines.append("## Contents\n")
    for dom, title in DOMAIN_TITLES.items():
        lines.append(f"- **Domain {title}**")
        for tname, t in model["tables"].items():
            if t["domain"] == dom:
                lines.append(f"  - [`{tname}`](#{tname})")
    lines.append("")

    # Enums
    lines.append("## Enumerations\n")
    for ename, vals in model["enums"].items():
        lines.append(f"- **`{ename}`**: {', '.join(f'`{v}`' for v in vals)}")
    lines.append("")

    # Tables by domain
    for dom, title in DOMAIN_TITLES.items():
        lines.append(f"## Domain {title}\n")
        for tname, t in model["tables"].items():
            if t["domain"] != dom:
                continue
            lines.append(f"### `{tname}`")
            lines.append(
                f"**Storage:** {TIER_BADGE[t['storage']]} &nbsp;|&nbsp; "
                f"**Grain:** {t['grain']}\n"
            )
            lines.append(t["description"].strip() + "\n")
            if t.get("partitioned_by"):
                lines.append(
                    f"*Partitioned by:* {', '.join(f'`{p}`' for p in t['partitioned_by'])}\n"
                )
            lines.append("| Column | Type | Key | Null | Description |")
            lines.append("|---|---|---|---|---|")
            for c in t["columns"]:
                key = ""
                if c.get("pk"):
                    key = "PK"
                elif c.get("fk"):
                    key = f"FK → `{c['fk']}`"
                null = "" if c.get("nullable", True) else "NOT NULL"
                lines.append(
                    f"| `{c['name']}` | {c['type']} | {key} | {null} | {c['description']} |"
                )
            lines.append("")
    return "\n".join(lines)


def main():
    model = yaml.safe_load(SRC.read_text(encoding="utf-8"))
    errors = validate(model)
    if errors:
        print("VALIDATION FAILED:", file=sys.stderr)
        for e in errors:
            print("  -", e, file=sys.stderr)
        sys.exit(1)
    OUT.write_text(render(model), encoding="utf-8")
    ntables = len(model["tables"])
    ncols = sum(len(t["columns"]) for t in model["tables"].values())
    print(f"OK: validated {ntables} tables, {ncols} columns. Wrote {OUT.name}.")


if __name__ == "__main__":
    main()
