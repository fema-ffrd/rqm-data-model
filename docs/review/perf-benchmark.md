# RQM Performance Benchmark — Results Template

**Status:** template. Every "measured" column is intentionally blank. Do not populate without a real run — Prompt 11 rule 1 rejects fabricated numbers.

Thresholds are copied verbatim from [ADR-performance](../decisions/ADR-performance.md); update this doc if the ADR ratifies new thresholds.

## Environment

| Field | Value |
|---|---|
| Postgres version | *(fill from run)* |
| Iceberg backend | *(S3 / MinIO / local)* |
| Hardware profile | *(vCPU / RAM / disk)* |
| Generator seed | *(from `generate_synthetic.py --seed`)* |
| Scale tier | *(small / medium / large)* |
| Benchmark run date | *(YYYY-MM-DD)* |
| JSON artifact | `tools/bench/results/perf-benchmark.<scale>.json` |

## Query latency — p95 (ms)

| ID | Query | Storage tier | Threshold (small) | Measured (small) | Threshold (medium) | Measured (medium) | Threshold (large) | Measured (large) |
|---|---|---|---|---|---|---|---|---|
| Q1 | Single-building attribs | Postgres | 50 |  | 100 |  | 250 |  |
| Q2 | Alternate geometries | Postgres | 50 |  | 100 |  | 250 |  |
| Q3 | Uncertainty spec resolution | Postgres | 50 |  | 100 |  | 250 |  |
| Q4a | Full deterministic export | Postgres → obj store | 60_000 |  | 900_000 |  | 7_200_000 |  |
| Q4b | Full pre-sampled export (1k × 8) | Postgres → Iceberg | 300_000 |  | 1_800_000 |  | 21_600_000 |  |
| Q5 | Revision history for one asset | Postgres | 100 |  | 200 |  | 500 |  |
| Q6 | mv_loss_summary community × AEP | Iceberg | 10_000 |  | 30_000 |  | 90_000 |  |
| Q7 | Full adopted-value lineage | Postgres | 200 |  | 500 |  | 1_000 |  |
| Q8 | Bitemporal snapshot at T (bulk) | Postgres | 30_000 |  | 300_000 |  | 1_800_000 |  |

## Storage footprint

| Metric | Threshold (small) | Measured (small) | Threshold (medium) | Measured (medium) | Threshold (large) | Measured (large) |
|---|---|---|---|---|---|---|
| S_flat (GB) | — |  | — |  | — |  |
| S_canonical (GB) | — |  | — |  | — |  |
| S_history (GB) | — |  | — |  | — |  |
| S_canonical / S_flat | ≤ 4× |  | ≤ 3× |  | ≤ 3× |  |
| S_history / S_canonical | ≤ 25% |  | ≤ 25% |  | ≤ 25% |  |

## Pass / fail

Fill after populating measured columns. Threshold-vs-measured is the gate; every row is Pass / Fail / N/A.

## Threshold changes

Record any change to a threshold here, with team confirmation. Historical thresholds remain in the git log; do not overwrite the ADR silently.

| Date | Metric | Old threshold | New threshold | Confirmed by | Rationale |
|---|---|---|---|---|---|
