#!/usr/bin/env python
"""Render data-dictionary.md from data-dictionary.yaml and validate integrity.

The YAML is the single source of truth; the Markdown is generated. Also validates that every
foreign key points to an existing table.column and that every table of a tier that requires
a primary key has one.
"""

import sys
from pathlib import Path

import yaml

HERE = Path(__file__).parent
SRC = HERE / "data-dictionary.yaml"
OUT = HERE / "data-dictionary.md"

DOMAIN_TITLES = {
    "inventory": "A — Inventory (NSI structures + first-class geometry)",
}

TIER_BADGE = {
    "postgres": "PostgreSQL",
    "iceberg": "Iceberg (via Apache Sedona)",
    "icechunk": "Icechunk",
}

PK_REQUIRED_TIERS = {"postgres", "iceberg"}


def validate(model):
    tables = model["tables"]
    colindex = {t: {c["name"] for c in tables[t]["columns"]} for t in tables}
    errors = []
    for tname, t in tables.items():
        if t["domain"] not in DOMAIN_TITLES:
            errors.append(
                f"[{tname}] domain '{t['domain']}' has no DOMAIN_TITLES entry — "
                "table would not render in the generated Markdown"
            )
        pks = [c["name"] for c in t["columns"] if c.get("pk")]
        if t["storage"] in PK_REQUIRED_TIERS and not pks:
            errors.append(f"[{tname}] {t['storage']} table has no primary key")
        for c in t["columns"]:
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
    return errors


def render(model):
    m = model["metadata"]
    lines = []
    lines.append(f"# {m['model_name']} — Data Dictionary\n")
    lines.append(
        f"**Version:** {m['version']} &nbsp;|&nbsp; "
        f"**Companion:** `{m['companion_repo']}` &nbsp;|&nbsp; "
        f"**Query engine:** {m.get('query_engine', '')}\n"
    )
    lines.append(
        "NSI-minimal edition: a focused, loadable schema for a single National "
        "Structure Inventory parquet ingest, with first-class multiple-geometry "
        "support (ADR-geometry) preserved so additional footprint sources can be "
        "attached beside each NSI point without a schema break. Component "
        "decomposition, attribute uncertainty, hazard links, DDFs, loss "
        "realizations, export contracts, provenance registry, and valid-time "
        "(bitemporal) history are deferred; re-entry points are documented in "
        "ADR-nsi-minimal. Inventory versions, branches, and the per-building "
        "change log follow ADR-versioning. The previous full-coverage "
        "dictionary is preserved at `data-dictionary-full.yaml`.\n"
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
