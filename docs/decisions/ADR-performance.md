# ADR — Performance & Scale Benchmarking

**ID:** ADR-performance
**Status:** Proposed (thresholds pending team confirmation)
**Date:** 2026-09-08
**Extends:** ADR-versioning, ADR-uncertainty, ADR-geometry, ADR-exports
**Resolves:** ISSUE-Q08
**Serves user stories:** RQM-03, RQM-06, RQM-09, RQM-11
**Locks:** thresholds are proposed pending team confirmation; measured numbers are not published until a benchmark run produces them; the schema does not carry synthetic or fabricated performance data.

## Context

The canonical model is richer than the flat file every current consequence engine consumes. Reviewers asked (ISSUE-Q08): at representative inventory scale, is the design's export latency, query latency, and storage growth acceptable? Answering that needs three artifacts that don't exist yet:

1. **A stated representative scale** — otherwise "acceptable" is undefined.
2. **A benchmark harness that runs on synthetic data** — no production data can be required.
3. **Pass/fail thresholds agreed *before* measuring** — otherwise the numbers we get become the definition of "acceptable" by default.

The ADR fixes all three. It does *not* run the benchmark, and it does not fabricate numbers.

## Decision

Adopt the scale, workload, and thresholds below as v0.1 targets. Scaffold a benchmark harness under `tools/bench/` that a follow-up task can execute; publish results to `docs/review/perf-benchmark.md` (template checked in with empty measured fields). Every threshold is marked *proposed — pending team confirmation* and will move to *ratified* after a measured baseline is agreed.

### Representative scale (per scenario tier)

| Tier | Buildings | Attribs/bldg | Uncertainty refs/bldg | Geometries/bldg | Components/bldg | Revisions/asset | AEPs/run | Realizations/run |
|---|---|---|---|---|---|---|---|---|
| **Small** (validation basin — e.g., Allegheny pilot) | 100,000 | ~20 | ~5 | 1.6 | 4 | 1.1 | 8 | 1,000 |
| **Medium** (state-scale study) | 1,000,000 | ~20 | ~5 | 1.6 | 4 | 1.1 | 8 | 1,000 |
| **Large** (regional / multi-state) | 10,000,000 | ~20 | ~5 | 1.6 | 4 | 1.1 | 8 | 1,000 |

**Assumptions behind the numbers** (all *project-requires*, not measured):

- NSI structure count nationwide ~124M; per-basin FFRD studies fall in the 100K–2M range → Small and Medium are the primary design targets; Large is a stretch target for later multi-state work.
- "Attribs/bldg ~20" is the `buildings` column count today plus baseline component metadata.
- "Uncertainty refs/bldg ~5" reflects FFH + foundation class + depth-in-structure + DDF spread + one component uncertainty — the shape ADR-uncertainty was built around.
- "Geometries/bldg 1.6" = 1 point (required by ADR-geometry rule 5) + 0.6 average footprint (present for ~60% of RES1 in NSI + Microsoft Buildings joined).
- "Revisions/asset 1.1" is post-first-calibration; v0.1 pre-calibration is 1.0.
- Loss-realization row count at Medium: `1M × 8 × 1000 = 8B rows` → Iceberg tier (partition-pruned by `event_id, aep`).

### Workload catalog

Eight canonical queries. Each is measured p50 / p95 latency at each scale tier.

| ID | Query | User story | Storage tier hit |
|---|---|---|---|
| Q1 | Current-head attributes for a single building | RQM-02 | Postgres |
| Q2 | Enumerate alternate geometries for one asset (all roles) | RQM-01 | Postgres |
| Q3 | Resolve the `uncertainty_spec` bound to `(building, attribute)` including subclass row | RQM-15, RQM-17 | Postgres |
| Q4 | Bulk deterministic export — compile one full inventory to flat | RQM-06 | Postgres → object store |
| Q5 | All revisions for an asset with editor + reason (revision_log) | RQM-11, RQM-16 | Postgres |
| Q6 | `mv_loss_summary` for one community × AEP | RQM-09 | Iceberg |
| Q7 | Full lineage for an adopted value (observation → activity → link) | RQM-07 | Postgres |
| Q8 | Bitemporal point-in-time query: inventory state at record_time = T | RQM-03 | Postgres |

### Storage-footprint comparison

Measure three things at each scale tier:

- `S_flat` — a plain flat baseline (one row/building, no history, no provenance): CSV / Parquet of the current NSI-style shape.
- `S_canonical` — the RQM Postgres tables in Domain A/B/C/D/F (attribute uncertainty, DDFs, versioning, provenance) plus Iceberg tables in E.
- `S_history` — the delta between `S_canonical` and a `S_canonical`-without-revisions synthetic control (isolates the versioning + revision cost).

Report as ratios: `S_canonical / S_flat` and `S_history / S_canonical`.

### Proposed thresholds *(pending team confirmation)*

| Metric | Small (100K) | Medium (1M) | Large (10M) |
|---|---|---|---|
| Q1 — single-building attrib fetch, p95 | ≤ 50 ms | ≤ 100 ms | ≤ 250 ms |
| Q2 — alternate geometries for asset, p95 | ≤ 50 ms | ≤ 100 ms | ≤ 250 ms |
| Q3 — uncertainty spec resolution, p95 | ≤ 50 ms | ≤ 100 ms | ≤ 250 ms |
| Q4 — full deterministic export | ≤ 60 s | ≤ 15 min | ≤ 2 hr |
| Q4 — full pre-sampled export (1k draws × 8 AEPs) | ≤ 5 min | ≤ 30 min | ≤ 6 hr |
| Q5 — asset revision history, p95 | ≤ 100 ms | ≤ 200 ms | ≤ 500 ms |
| Q6 — community × AEP loss aggregate, p95 (Iceberg, partitioned) | ≤ 10 s | ≤ 30 s | ≤ 90 s |
| Q7 — full adopted-value lineage, p95 | ≤ 200 ms | ≤ 500 ms | ≤ 1 s |
| Q8 — bitemporal snapshot at T, p95 (bulk) | ≤ 30 s | ≤ 5 min | ≤ 30 min |
| `S_canonical / S_flat` | ≤ 4× | ≤ 3× | ≤ 3× |
| `S_history / S_canonical` (revision cost) | ≤ 25% | ≤ 25% | ≤ 25% |

Rationale for the storage target: normalization + shared `uncertainty_spec` rows (one spec bound by many `uncertainty_ref` rows) offsets a lot of the canonical richness. The 3× target at Medium/Large depends on that sharing paying off; small-tier's 4× reflects less amortization at low counts. The revision-cost cap (25%) is deliberately tight — bitemporal columns should not double storage; if they do, the schema is over-eager.

### Hardware baseline for measurement (proposed)

Not part of the schema, but the ADR pins the assumption so numbers are comparable:

- Postgres 16, single node, 8 vCPU / 32 GiB RAM / gp3 SSD (10 KIOPS baseline).
- Iceberg tables on S3 (or local MinIO for CI), read through Trino / DuckDB.
- No manual index tuning; standard PK / FK indexes + `hazard_links(building_id, event_id, aep)` composite index only.

Larger hardware improves numbers linearly for point queries and roughly by `sqrt(N)` for scan-bound workloads — thresholds above assume this baseline.

### What the benchmark harness does

Under `tools/bench/` (scaffolded in this ADR — see §Files below):

- `generate_synthetic.py` — deterministic synthetic inventory generator, parameterized by `--scale small|medium|large` and `--seed <int>`. Emits Postgres INSERTs / Iceberg parquet for every Domain A/B/C/D table plus a proportional volume for E (realizations). No real NSI data used; distributions are stated in the script header.
- `run_queries.py` — runs Q1..Q8 with `pg_stat_statements` timing, N iterations per query, computes p50/p95, writes JSON output.
- `measure_storage.py` — computes `S_flat`, `S_canonical`, `S_history`; writes JSON.
- `results/perf-benchmark.md` (template) — assembled from JSON outputs; measured fields empty until a run populates them.

Files to be added by this ADR (scaffolding only; no fabricated numbers):

- `tools/bench/README.md`
- `tools/bench/generate_synthetic.py` (stub with docstring + parameter parser; body raises NotImplementedError with an inline TODO)
- `tools/bench/run_queries.py` (same shape)
- `tools/bench/measure_storage.py` (same shape)
- `docs/review/perf-benchmark.md` — the results template with empty measured columns.

Every executable file is honest about what it does not yet do. **No fabricated measurements land in the repo.**

### Reporting

`docs/review/perf-benchmark.md` renders three sections:

1. **Environment** — hardware, Postgres version, Iceberg backend, generator seed, scale tier.
2. **Query latency** — table of Q1..Q8 with p50/p95 measured vs threshold, pass/fail per row.
3. **Storage footprint** — `S_flat`, `S_canonical`, `S_history`, ratios, pass/fail.

Threshold-vs-measured is the pass/fail gate; the template has the threshold column pre-populated and the measured column blank.

## Validation rules (Prompt 11 will enforce in `preview_dict.py`)

Prompt 7 does not add schema. The one validation-adjacent check Prompt 11 will run:

1. `docs/review/perf-benchmark.md`, when present, must have every "measured" cell either blank or backed by a JSON artifact under `tools/bench/results/`. Fabricated numbers are rejected at CI time.

## Worked examples

### Example 1 — A team runs the benchmark and lands under-threshold at Medium

```
$ python tools/bench/generate_synthetic.py --scale medium --seed 42
$ python tools/bench/run_queries.py
$ python tools/bench/measure_storage.py
$ cat tools/bench/results/perf-benchmark.json  # measured p95 Q1 = 68 ms, threshold 100 ms → pass
```

Q1..Q8 all pass, `S_canonical / S_flat = 2.4×` (under the 3× cap), `S_history / S_canonical = 18%` (under the 25% cap). The team pastes the JSON into `docs/review/perf-benchmark.md`; Prompt 11 rule 1 accepts because every "measured" cell is JSON-backed.

### Example 2 — A team lands over-threshold on Q4

Q4 (full deterministic export at Medium) measures 22 minutes vs the 15-minute threshold. Not a failure of the schema per se — three response options:

- Optimize the compiler (better parallelism, streaming writer).
- Adjust the threshold with team confirmation (documented under `docs/review/perf-benchmark.md#threshold-changes`).
- Push the workload to a Small-tier benchmark and accept the Medium/Large threshold as pending.

Not an option: silently backfill a passing number. Prompt 11 rule 1 blocks that.

### Example 3 — Storage exceeds 3× at Large

`S_canonical / S_flat = 4.6×` at Large. The `S_history / S_canonical` breakdown says 62% of the overhead is `versioning + asset_revision + revision_log`. This is a schema-level signal: revision rows may be too eager (recording metadata churn as revisions, not just value changes). The fix goes to ADR-versioning + Prompt 11 (revision-emission policy), not to Prompt 7.

## Claim discipline

- Every threshold in this ADR is *project-requires* + *proposed*, not *implementation-demonstrates*. No number here is measured.
- The scale tiers are *project-requires* — grounded in NSI structure counts and FFRD basin study sizes. Adjustable with team confirmation.
- The hardware baseline is a *specification-allows* — measurements on other hardware are valid; they just need to state the delta.
- Storage ratios depend on `uncertainty_spec` sharing paying off. If it does not, the 3× target is *research-recommends* (normalization + dedup typically achieves 2–4× versus flat), and Medium/Large targets will move accordingly.
- Scaffolding is *specification-allows*: files exist and parse; bodies raise `NotImplementedError` where a measurement would happen. Nothing here claims to have run.

## Alternatives considered

- **Skip the ADR; wait until a benchmark run exists.** Rejected — thresholds set *after* measurement become the definition of "acceptable." ISSUE-Q08 asked for the reverse: agree on the target, then measure.
- **Pull real NSI + real Allegheny pilot data as the benchmark corpus.** Rejected — reproducibility outside FFRD environments requires synthetic data; the harness must run for anyone.
- **Publish placeholder measured numbers now, mark them "provisional".** Rejected — placeholder numbers migrate into decision papers within one cycle; the ADR + Prompt 11 rule 1 makes this a CI-blocked path.
- **Threshold per query only; ignore storage.** Rejected — the whole point of ADR-versioning is bitemporal history; storage cost is the tax that history charges and needs its own gate.
- **One threshold across all scales.** Rejected — Q4 at Medium and Q4 at Large are not the same workload; per-tier thresholds make regressions visible.
- **Colocate the harness with the schema tooling in `data-dictionary/`.** Rejected — different lifecycle, different dependencies (Postgres/Iceberg client vs YAML validator). `tools/bench/` keeps the concerns separate.

## Impact on other ADRs

- **ADR-versioning:** the `S_history / S_canonical` ratio is a check on revision-emission policy. If revisions balloon storage, revise Prompt 3's revision-vs-metadata distinction.
- **ADR-uncertainty:** the `S_canonical / S_flat` ratio pressures `uncertainty_spec` sharing (one spec ↔ many `uncertainty_ref`). If sharing does not materialize, target moves to 4×.
- **ADR-geometry:** Q2 (alternate geometries) is a workload gate; if it fails, the alt-representations story needs an index (composite on `(asset_id, role)`).
- **ADR-exports:** Q4 measures both `deterministic` and `pre_sampled_realizations` compile paths independently; contract-level regressions will surface here.
- **ADR-distribution-registry:** parameter-contract validation cost lands inside Q4 (compile time); if it dominates, contract validation may need caching.
- **ADR-ddf-uncertainty:** rendered-cache staleness checks (Prompt 11 rule 3 there) add compile-time cost; measured under Q4.
- **ADR-restricted-sources:** access-classification join hits Q4 for every restricted-derived field; if the join dominates, `source_observation.source_ref` gains a covering index. Data-access cost, not schema shape.

## Deferrals

- **Actual benchmark run.** Follow-up task; results go to `docs/review/perf-benchmark.md` with JSON artifacts under `tools/bench/results/`.
- **Threshold ratification.** Requires team confirmation; today they are proposed.
- **Alternative hardware profiles** (multi-node Postgres, spot-instance Iceberg).
- **Streaming / delta export benchmarks.** ADR-exports deferred these; Prompt 7 does not benchmark what does not yet exist.
- **Read-replica / caching topologies.**
- **Load-generation tooling** (concurrent query mix). Today the harness measures single-query latency; concurrency-under-load is a v0.2 concern.
- **CI-integrated benchmark gating.** Prompt 11 rule 1 only rejects fabricated numbers; automatic regression gating on measured runs is deferred.
- **Component-level 3D geometry cost.** No populated data yet (ADR-geometry deferral); no meaningful benchmark until then.
