# RQM Benchmark Harness

Scaffolding for the performance benchmark described in [ADR-performance](../../docs/decisions/ADR-performance.md).

**Status: scaffold only.** Every measurement path raises `NotImplementedError`. Do not report numbers from this directory unless they come from a real run that populates `results/*.json`.

## Layout

```
tools/bench/
├── README.md              — this file
├── generate_synthetic.py  — synthetic inventory generator (scale small|medium|large)
├── run_queries.py         — Q1..Q8 latency measurement
├── measure_storage.py     — S_flat / S_canonical / S_history footprint measurement
├── requirements.txt       — Python dependencies
└── results/               — generated JSON output (gitignored except this README)
    └── README.md          — result-file contract
```

## Usage (once implemented)

```
pip install -r tools/bench/requirements.txt
python tools/bench/generate_synthetic.py --scale small --seed 42
python tools/bench/run_queries.py --scale small --iterations 100
python tools/bench/measure_storage.py --scale small
```

Results land in `tools/bench/results/perf-benchmark.<scale>.json`. Copy the relevant fields into `docs/review/perf-benchmark.md`.

## Ground rules (locked)

- No production data. The generator emits synthetic buildings sized to the scale tier; distributions are documented in the generator's docstring.
- No fabricated numbers. `docs/review/perf-benchmark.md` measured cells must be JSON-backed by files under `results/` (Prompt 11 rule 1).
- Threshold ratification requires team confirmation. Today's thresholds are *proposed*.
