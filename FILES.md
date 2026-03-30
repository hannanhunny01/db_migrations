# Benchmark File Map

This is the small set of files and directories that matter for benchmark replication.

## Entry points

- `bench.py`: lightweight wrapper around the CLI
- `pgmigbench/__main__.py`: module entry point for `python3 -m pgmigbench`
- `pgmigbench/cli.py`: defines `run` and `report`

## Runtime and configuration

- `pgmigbench/runner.py`: orchestrates benchmark execution, evidence capture, resume handling, and summary generation
- `pgmigbench/config.py`: environment-variable settings such as time windows, lock timeout, and PostgreSQL port
- `pgmigbench/docker.py`: Docker compose start/stop and container command helpers

## Scenario generation

- `pgmigbench/scenarios/generator.py`: deterministic benchmark suite generation from seed
- `pgmigbench/scenarios/families.py`: scenario family definitions
- `pgmigbench/scenarios/base.py`: scenario model

## Migration strategies

- `pgmigbench/strategies/baseline_a.py`: monolithic migration plan generation
- `pgmigbench/strategies/baseline_b.py`: staged migration plan generation
- `pgmigbench/strategies/alembic_exec.py`: execution path for the monolithic Alembic-style flow

## Workload

- `pgmigbench/workload/pgbench.py`: `pgbench` orchestration and logging
- `pgmigbench/workload/scripts/*.sql`: old/new workload SQL templates used during benchmark phases

## Reporting

- `pgmigbench/report/aggregate.py`: CSV aggregation and summary writing
- `pgmigbench/report/stats.py`: statistics helpers
- `pgmigbench/report/latex.py`: `summary.json` to `results.tex` conversion

## Container setup

- `docker/docker-compose.yml`: PostgreSQL service used for the benchmark
- `docker/postgres.conf`: optional postgres tuning reference

## Dependency and package metadata

- `pyproject.toml`: package metadata and Python dependencies
- `uv.lock`: locked dependency snapshot for `uv` users

## Tests useful for replication confidence

- `tests/test_integration_smoke.py`: end-to-end smoke run
- `tests/test_determinism.py`: deterministic suite generation checks
- `tests/test_report_latex.py`: report generation checks
- `tests/test_plans.py`: migration plan structure checks

## Main artifact paths produced by a run

- `artifacts/benchmark_results.csv`
- `artifacts/summary.json`
- `artifacts/evidence/...`
- `artifacts/systemic_failure.json`
- `results.tex`
