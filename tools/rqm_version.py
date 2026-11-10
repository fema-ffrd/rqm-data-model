"""Inspect and manage RQM inventory versions (ADR-versioning).

Usage
-----
    python tools/rqm_version.py --warehouse WH list
    python tools/rqm_version.py --warehouse WH diff nsi-2024 nsi-2025 [--show 20]
    python tools/rqm_version.py --warehouse WH history 120262564
    python tools/rqm_version.py --warehouse WH branch list
    python tools/rqm_version.py --warehouse WH branch create calibration --from nsi-2025
    python tools/rqm_version.py --warehouse WH branch merge calibration \\
        --version nsi-2025-cal --editor jdoe --reason "found_ht survey"
    python tools/rqm_version.py --warehouse WH branch drop calibration
    python tools/rqm_version.py --warehouse WH export nsi-2025 out/ [--where SQL]
    python tools/rqm_version.py --warehouse WH check

Versions are created by loading data: `ingest_nsi.py --version NAME` (onto
main or `--branch`). Edits made directly on a branch with Spark SQL are
committed with `versioning.commit_version`. Session and catalog options are
the same as for `ingest_nsi.py` (`--catalog`, `--namespace`,
`--catalog-type`, `--catalog-prop`, `--master`, `--conf`).
"""

from __future__ import annotations

import argparse
import json
import sys

from ingest_nsi import (
    add_session_arguments,
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

    sub.add_parser("check", help="Check registry, tags, change log, and invariants.")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    import versioning as v

    spark = build_spark(settings_from_args(args))
    cat, ns = args.catalog, args.namespace
    try:
        if args.command == "list":
            v.list_versions(spark, cat, ns).drop(
                "source_uri", "buildings_snapshot_id", "geometries_snapshot_id"
            ).show(100, truncate=40)
        elif args.command == "diff":
            diff = v.diff_versions(spark, cat, args.old, args.new, ns).cache()
            print(json.dumps(v.diff_summary(diff), indent=2))
            diff.orderBy("change_kind", "building_id").show(args.show, truncate=False)
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
                spark, cat, args.version, args.out_dir, where=args.where, namespace=ns
            )
            print(json.dumps(manifest, indent=2))
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


if __name__ == "__main__":
    sys.exit(main())
