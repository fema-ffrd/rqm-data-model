"""Run RQM benchmark queries Q1..Q8 (scaffold).

Runs the eight canonical queries from ADR-performance §Workload catalog
against a Postgres + Iceberg backend populated by generate_synthetic.py.
Records p50 / p95 per query via pg_stat_statements and writes JSON to
tools/bench/results/perf-benchmark.<scale>.json.

The eight queries (each parameterized against the loaded scale tier):

    Q1  current-head attributes for one building
    Q2  alternate geometries for one asset (all roles)
    Q3  uncertainty_spec resolution for (building, attribute) inc. subclass
    Q4  bulk deterministic export compile
    Q5  all revisions for one asset with editor + reason
    Q6  mv_loss_summary aggregate for one community x AEP (Iceberg)
    Q7  full lineage: observation -> activity -> adopted_value_link
    Q8  bitemporal point-in-time snapshot of the inventory at T

Usage (once implemented):
    python run_queries.py --scale small --iterations 100

Deterministic per seed. See ADR-performance for thresholds and rationale.
"""

from __future__ import annotations

import argparse
import sys


SCALES = ("small", "medium", "large")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scale", choices=SCALES, required=True)
    p.add_argument("--iterations", type=int, default=100)
    p.add_argument("--pg-dsn", type=str, default="postgresql://localhost/rqm_bench")
    p.add_argument("--iceberg-catalog", type=str, default="rqm_bench")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    print(
        f"[scaffold] would run Q1..Q8 x {args.iterations} iterations "
        f"at scale={args.scale} against {args.pg_dsn}",
        file=sys.stderr,
    )
    raise NotImplementedError(
        "run_queries.py is a scaffold. "
        "Implementation lands in a follow-up task per ADR-performance §Deferrals. "
        "Do not populate docs/review/perf-benchmark.md without a real run."
    )


if __name__ == "__main__":
    sys.exit(main())
