# Benchmark Results

`run_queries.py` and `measure_storage.py` write JSON artifacts here:

- `perf-benchmark.<scale>.json` — combined output consumed by [docs/review/perf-benchmark.md](../../../docs/review/perf-benchmark.md).

**Locked rule (ADR-performance):** every "measured" cell in `docs/review/perf-benchmark.md` must correspond to a value in one of these JSON files. Prompt 11 rule 1 rejects fabricated numbers at CI time.

Files here are gitignored (except this README). Copy or link them into the review doc when publishing measured results.
