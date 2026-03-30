# db_migrations

This folder is a standalone benchmark handoff package that you can zip and send.
It is intentionally focused on replication only: what the benchmark is, what you need, and exactly how to run it.

All commands below assume you are already inside this `db_migrations` folder.
If you just unzipped it from a parent directory, run `cd db_migrations` first.

## Benchmark scope

The benchmark compares two PostgreSQL schema-migration strategies under concurrent load:

- `baseline_a`: monolithic one-shot migration
- `baseline_b`: staged Expand-Migrate-Cutover-Contract migration

The benchmark runs against a real PostgreSQL Docker container and executes `pgbench` inside that container.

## Prerequisites

- Linux or macOS
- Docker with the `docker compose` plugin
- Python 3.11 or newer

You do not need a host-side `pgbench` install because the workload runs inside the PostgreSQL container.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

## Fast smoke run

Use this when you want to confirm the benchmark pipeline works end to end with a very small run:

```bash
PGMIGBENCH_WARMUP_WINDOW_S=1 \
PGMIGBENCH_BASELINE_WINDOW_S=1 \
PGMIGBENCH_MIGRATION_BUFFER_S=1 \
PGMIGBENCH_CUTOVER_WINDOW_S=1 \
PGMIGBENCH_POST_WINDOW_S=1 \
python3 bench.py run \
  --suite-size 1 \
  --seed 2026 \
  --repetitions 1 \
  --out artifacts_smoke \
  --pg-version 16.3 \
  --shutdown
```

## Main benchmark run

Paper-style full run:

```bash
python3 bench.py run \
  --suite-size 100 \
  --seed 2026 \
  --repetitions 5 \
  --out artifacts \
  --pg-version 16.3
```

Balanced per-family sample:

```bash
python3 bench.py run \
  --sample-per-family 20 \
  --seed 2026 \
  --repetitions 5 \
  --out artifacts \
  --pg-version 16.3
```

Resume an interrupted run:

```bash
python3 bench.py run \
  --suite-size 100 \
  --seed 2026 \
  --repetitions 5 \
  --out artifacts \
  --pg-version 16.3 \
  --resume
```

Stop Docker after a run finishes:

```bash
python3 bench.py run \
  --suite-size 100 \
  --seed 2026 \
  --repetitions 5 \
  --out artifacts \
  --pg-version 16.3 \
  --shutdown
```

## Generate the report file

```bash
python3 bench.py report \
  --input artifacts/summary.json \
  --out results.tex
```

This creates LaTeX macros from the benchmark summary.

## Key outputs

- `artifacts/benchmark_results.csv`: one row per scenario/strategy/repetition
- `artifacts/summary.json`: aggregate benchmark summary
- `artifacts/evidence/<run_id>/<scenario_id>/<strategy>/rep_01/...`: per-run evidence, logs, SQL, timing, and environment metadata
- `artifacts/systemic_failure.json`: early-stop diagnostic when a repeated systemic failure is detected
- `results.tex`: LaTeX macro output for tables/figures

## Important environment variables

- `PGMIGBENCH_DB_PORT`: host port for PostgreSQL, default `55432`
- `PGMIGBENCH_WARMUP_WINDOW_S`
- `PGMIGBENCH_BASELINE_WINDOW_S`
- `PGMIGBENCH_MIGRATION_BUFFER_S`
- `PGMIGBENCH_CUTOVER_WINDOW_S`
- `PGMIGBENCH_POST_WINDOW_S`
- `PGMIGBENCH_REPETITIONS`
- `PGMIGBENCH_TELEMETRY_POLL_S`
- `PGMIGBENCH_LOCK_TIMEOUT_MS`
- `PGMIGBENCH_STATEMENT_TIMEOUT_S`

## Minimal verification

CLI help:

```bash
python3 bench.py --help
python3 bench.py run --help
python3 bench.py report --help
```

Test suite:

```bash
pytest -q
```

Docker-backed smoke test:

```bash
PGMIGBENCH_RUN_INTEGRATION=1 pytest tests/test_integration_smoke.py -q
```

## Cleanup

If you want to stop the benchmark container and remove its volume:

```bash
docker compose -f docker/docker-compose.yml down -v
```

## More in this folder

- `COMMANDS.md`: copy-paste command reference
- `FILES.md`: benchmark-relevant file map for the repo
