"""Inspect and manage RQM inventory versions (ADR-versioning).

Usage
-----
    python tools/rqm_version.py --warehouse WH list
    python tools/rqm_version.py --warehouse WH diff nsi-2024 nsi-2025 [--show 20] \
        [--include-sidecar] [--include-lineage]
    python tools/rqm_version.py --warehouse WH history 120262564
    python tools/rqm_version.py --warehouse WH branch list
    python tools/rqm_version.py --warehouse WH branch create calibration --from nsi-2025
    python tools/rqm_version.py --warehouse WH branch merge calibration \\
        --version nsi-2025-cal --editor jdoe --reason "found_ht survey"
    python tools/rqm_version.py --warehouse WH branch drop calibration
    python tools/rqm_version.py --warehouse WH export nsi-2025 out/ [--where SQL] \
        [--source NSI --source COUNTY-ASSESSOR] [--no-include-sidecar] \
        [--no-include-lineage]
    python tools/rqm_version.py --warehouse WH sources list
    python tools/rqm_version.py --warehouse WH sources show COUNTY-ASSESSOR
    python tools/rqm_version.py --warehouse WH specs list [--source REF]
    python tools/rqm_version.py --warehouse WH specs show <spec_sha256 or prefix>
    python tools/rqm_version.py --warehouse WH check

Versions are created by loading data: `ingest_nsi.py --version NAME` or
`ingest_source.py --spec SPEC --version NAME` (onto main or `--branch`).
Edits made directly on a branch with Spark SQL are committed with
`versioning.commit_version`. Session and catalog options are the same as for
`ingest_nsi.py` (`--catalog`, `--namespace`, `--catalog-type`,
`--catalog-prop`, `--master`, `--conf`). Every command first applies the
idempotent schema migration (`ingest_nsi.apply_schema`).
"""

from __future__ import annotations

import argparse
import json
import sys

from ingest_nsi import (
    add_session_arguments,
    apply_schema,
    build_spark,
    default_editor,
    settings_from_args,
)


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    add_session_arguments(ap)
    sub = ap.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="List registered versions.")

    diff = sub.add_parser("diff", help="Building-level diff between two versions.")
    diff.add_argument("old")
    diff.add_argument("new")
    diff.add_argument(
        "--show", type=int, default=20, help="Changed buildings to print (default 20)."
    )
    diff.add_argument(
        "--include-sidecar",
        action="store_true",
        help="Also diff source_attributes (changed attribute keys per building).",
    )
    diff.add_argument(
        "--include-lineage",
        action="store_true",
        help="Also diff building_lineage (spec / version / record id changes).",
    )

    history = sub.add_parser("history", help="Versions that changed one building.")
    history.add_argument("building_id", type=int)

    branch = sub.add_parser("branch", help="Project branches.").add_subparsers(
        dest="branch_command", required=True
    )
    branch.add_parser("list", help="List branches and their head versions.")
    create = branch.add_parser("create", help="Create a branch from a version.")
    create.add_argument("name")
    create.add_argument("--from", dest="from_version", required=True)
    merge = branch.add_parser("merge", help="Merge a branch and commit a version.")
    merge.add_argument("name")
    merge.add_argument("--version", required=True, help="Name of the merge version.")
    merge.add_argument("--into", default="main", help="Target branch (default: main).")
    merge.add_argument("--editor", default=None, help="Default: $USER.")
    merge.add_argument("--reason", default=None)
    drop = branch.add_parser("drop", help="Drop a branch (its versions stay).")
    drop.add_argument("name")

    export = sub.add_parser(
        "export", help="Export a version to parquet with a _rqm_version.json manifest."
    )
    export.add_argument("version")
    export.add_argument("out_dir")
    export.add_argument(
        "--where", default=None, help="SQL filter, e.g. \"occtype = 'RES1'\"."
    )
    export.add_argument(
        "--source",
        dest="sources",
        action="append",
        default=None,
        metavar="REF",
        help="Export only this source's buildings; repeatable (default: all sources).",
    )
    export.add_argument(
        "--include-sidecar",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Write _source_attributes.parquet (default: when an exported source "
        "declares a sidecar).",
    )
    export.add_argument(
        "--include-lineage",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write _building_lineage.parquet and _source_specs.json (default: yes).",
    )

    sources = sub.add_parser(
        "sources", help="Registered building sources."
    ).add_subparsers(dest="sources_command", required=True)
    sources.add_parser("list", help="List sources with their building count on main.")
    show = sources.add_parser("show", help="One source: registry row and history.")
    show.add_argument("source_ref")

    specs = sub.add_parser(
        "specs", help="Registered mapping specs (source_specs)."
    ).add_subparsers(dest="specs_command", required=True)
    specs_list = specs.add_parser("list", help="List registered specs.")
    specs_list.add_argument("--source", default=None, metavar="REF")
    spec_show = specs.add_parser("show", help="Print one registered spec.")
    spec_show.add_argument("sha", help="spec_sha256 or a unique prefix of it.")

    sub.add_parser("check", help="Check registry, tags, change log, and invariants.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    import versioning as v

    spark = build_spark(settings_from_args(args))
    cat, ns = args.catalog, args.namespace
    try:
        apply_schema(spark, cat, namespace=ns)
        if args.command == "list":
            v.list_versions(spark, cat, ns).drop(
                "source_uri", *[f"{t}_snapshot_id" for t in v.VERSIONED_TABLES]
            ).show(100, truncate=40)
        elif args.command == "diff":
            diff = v.diff_versions(spark, cat, args.old, args.new, ns).cache()
            print(json.dumps(v.diff_summary(diff), indent=2))
            diff.orderBy("change_kind", "building_id").show(args.show, truncate=False)
            for table, wanted in (
                ("source_attributes", args.include_sidecar),
                ("building_lineage", args.include_lineage),
            ):
                if not wanted:
                    continue
                changes = v.diff_sidecar_versions(
                    spark, cat, args.old, args.new, table, ns
                ).cache()
                print(f"{table}:")
                print(json.dumps(v.diff_summary(changes), indent=2))
                changes.orderBy("change_kind", "building_id").show(
                    args.show, truncate=False
                )
        elif args.command == "history":
            v.building_history(spark, cat, args.building_id, ns).show(100, truncate=40)
        elif args.command == "branch":
            if args.branch_command == "list":
                v.list_branches(spark, cat, ns).show(100, truncate=False)
            elif args.branch_command == "create":
                v.create_branch(spark, cat, args.name, args.from_version, ns)
                print(f"OK: branch {args.name!r} created at {args.from_version!r}.")
            elif args.branch_command == "merge":
                try:
                    info = v.merge_branch(
                        spark,
                        cat,
                        args.name,
                        version=args.version,
                        editor=args.editor or default_editor(),
                        reason=args.reason,
                        into=args.into,
                        namespace=ns,
                    )
                except v.MergeConflictError as err:
                    print(f"error: {err}", file=sys.stderr)
                    for row in err.conflicts:
                        print(f"  {row.asDict()}", file=sys.stderr)
                    return 1
                print(
                    f"OK: {info['mode']} merge of {args.name!r} into {args.into!r} "
                    f"committed as {info['version_name']!r}: +{info['n_added']} "
                    f"-{info['n_removed']} ~{info['n_changed']}."
                )
            elif args.branch_command == "drop":
                v.drop_branch(spark, cat, args.name, ns)
                print(f"OK: branch {args.name!r} dropped.")
        elif args.command == "export":
            manifest = v.export_version(
                spark,
                cat,
                args.version,
                args.out_dir,
                where=args.where,
                sources=args.sources,
                include_sidecar=args.include_sidecar,
                include_lineage=args.include_lineage,
                namespace=ns,
            )
            print(json.dumps(manifest, indent=2))
        elif args.command == "sources":
            return _sources_command(spark, args)
        elif args.command == "specs":
            return _specs_command(spark, args)
        elif args.command == "check":
            problems = v.check_consistency(spark, cat, ns)
            for problem in problems:
                print(f"problem: {problem}")
            if problems:
                return 1
            print("OK: versions, tags, change log, and invariants are consistent.")
    finally:
        spark.stop()
    return 0


def _sources_command(spark, args: argparse.Namespace) -> int:
    import ingest_source as src
    import versioning as v
    from ingest_nsi import table_name
    from pyspark.sql import functions as F

    cat, ns = args.catalog, args.namespace
    if args.sources_command == "list":
        on_main = (
            v.conform_buildings(spark.table(table_name(cat, "buildings", ns)))
            .groupBy("source_ref")
            .agg(F.count("*").alias("n_buildings_main"))
        )
        src.list_sources(spark, cat, ns).join(on_main, "source_ref", "left").orderBy(
            "source_ref"
        ).show(100, truncate=40)
        return 0
    try:
        row = src.get_source(spark, cat, args.source_ref, ns)
    except KeyError as err:
        print(f"error: {err.args[0]}", file=sys.stderr)
        return 1
    print(json.dumps(row.asDict(), indent=2, default=str))
    print("\nSpecs registered for this source (rqm_version.py specs show <sha>):")
    spark.table(table_name(cat, "source_specs", ns)).where(
        F.col("source_ref") == args.source_ref
    ).select("spec_sha256", "spec_uri", "registered_at").orderBy("registered_at").show(
        100, truncate=False
    )
    print("Versions that loaded this source:")
    v.list_versions(spark, cat, ns).where(
        F.col("source_ref") == args.source_ref
    ).select(
        "version_name", "branch", "kind", "editor", "reason", "source_uri", "created_at"
    ).show(100, truncate=40)
    print("Buildings of this source per version:")
    v.source_counts(spark, cat, ns).where(F.col("source_ref") == args.source_ref).drop(
        "source_ref"
    ).show(100, truncate=False)
    return 0


def _specs_command(spark, args: argparse.Namespace) -> int:
    from ingest_nsi import table_name
    from pyspark.sql import functions as F

    specs = spark.table(table_name(args.catalog, "source_specs", args.namespace))
    if args.specs_command == "list":
        if args.source:
            specs = specs.where(F.col("source_ref") == args.source)
        specs.select("spec_sha256", "source_ref", "spec_uri", "registered_at").orderBy(
            "source_ref", "registered_at"
        ).show(100, truncate=False)
        return 0
    rows = specs.where(F.col("spec_sha256").startswith(args.sha)).collect()
    if len(rows) != 1:
        found = "no" if not rows else f"{len(rows)}"
        print(f"error: {found} specs match {args.sha!r}", file=sys.stderr)
        return 1
    row = rows[0]
    print(
        f"spec_sha256: {row['spec_sha256']}\nsource_ref: {row['source_ref']}\n"
        f"spec_uri: {row['spec_uri']}\nregistered_at: {row['registered_at']}\n"
    )
    print(json.dumps(json.loads(row["spec_document"]), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
