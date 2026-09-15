"""Measure storage footprint: S_flat, S_canonical, S_history (scaffold).

Computes the three storage figures defined in ADR-performance §Storage-
footprint comparison and writes them to tools/bench/results/.

    S_flat       plain flat baseline (one row/building, no history)
    S_canonical  RQM Postgres tables (Domain A/B/C/D/F) + Iceberg (E)
    S_history    delta versus a canonical-without-revisions control

Reports:
    S_canonical / S_flat        overhead ratio (target: <= 3x at Medium/Large)
    S_history / S_canonical     revision cost (target: <= 25%)

Usage (once implemented):
    python measure_storage.py --scale small

Requires the same populated backend as run_queries.py.
"""

from __future__ import annotations

import argparse
import sys


SCALES = ("small", "medium", "large")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scale", choices=SCALES, required=True)
    p.add_argument("--pg-dsn", type=str, default="postgresql://localhost/rqm_bench")
    p.add_argument("--iceberg-catalog", type=str, default="rqm_bench")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    print(
        f"[scaffold] would compute S_flat / S_canonical / S_history "
        f"at scale={args.scale}",
        file=sys.stderr,
    )
    raise NotImplementedError(
        "measure_storage.py is a scaffold. "
        "Implementation lands in a follow-up task per ADR-performance §Deferrals."
    )


if __name__ == "__main__":
    sys.exit(main())
