"""Synthetic RQM inventory generator (scaffold).

Emits a synthetic Postgres INSERT script + Iceberg parquet files sized to
one of the ADR-performance scale tiers. NO PRODUCTION DATA. Distributions
are documented per-table below and are frozen for the ADR-performance
benchmark; changing them requires a new ADR version.

Scale tiers (from ADR-performance):
    small   : 100_000 buildings
    medium  : 1_000_000 buildings
    large   : 10_000_000 buildings

Per building (all tiers):
    ~20 base attributes on buildings
    ~5 uncertainty_ref rows into shared uncertainty_spec pool
    1.6 geometries avg (1 point + 0.6 footprint)
    4 building_components rows
    1.1 asset_revision rows avg
    8 hazard_links rows (one per AEP)
    ~500 mv_loss_summary rows (per community aggregation)
    8 * 1_000 = 8_000 loss_realizations rows per building (Iceberg)

Uncertainty-spec pool size: 200 shared specs (class-level / regional)
plus 1 building-specific spec per building for FFH. This is the sharing
model that makes the S_canonical / S_flat ratio target achievable.

Usage (once implemented):
    python generate_synthetic.py --scale small --seed 42 --out ./out/

The generator is deterministic in the given seed. Two runs at the same
scale + seed produce byte-identical output.
"""

from __future__ import annotations

import argparse
import sys


SCALE_TIERS = {
    "small": 100_000,
    "medium": 1_000_000,
    "large": 10_000_000,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scale", choices=list(SCALE_TIERS), required=True)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", type=str, default="./out/")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    n_buildings = SCALE_TIERS[args.scale]
    print(
        f"[scaffold] would generate {n_buildings:,} buildings "
        f"(scale={args.scale}, seed={args.seed}) into {args.out}",
        file=sys.stderr,
    )
    raise NotImplementedError(
        "generate_synthetic.py is a scaffold. "
        "Implementation lands in a follow-up task per ADR-performance §Deferrals. "
        "Do not report benchmark numbers without a real generator run."
    )


if __name__ == "__main__":
    sys.exit(main())
