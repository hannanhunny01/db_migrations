# Benchmark Commands

All commands below should be run from inside the `db_migrations` folder.

## 1. Create the environment

```bash
cd db_migrations
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

## 2. Check the CLI

```bash
python3 bench.py --help
python3 bench.py run --help
python3 bench.py report --help
```

## 3. Fast smoke run

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

## 4. Full benchmark run

```bash
python3 bench.py run \
  --suite-size 100 \
  --seed 2026 \
  --repetitions 5 \
  --out artifacts \
  --pg-version 16.3
```

## 5. Balanced run by family

```bash
python3 bench.py run \
  --sample-per-family 20 \
  --seed 2026 \
  --repetitions 5 \
  --out artifacts \
  --pg-version 16.3
```

## 6. Resume an interrupted run

```bash
python3 bench.py run \
  --suite-size 100 \
  --seed 2026 \
  --repetitions 5 \
  --out artifacts \
  --pg-version 16.3 \
  --resume
```

## 7. Generate the LaTeX report

```bash
python3 bench.py report \
  --input artifacts/summary.json \
  --out results.tex
```

## 8. Run tests

```bash
pytest -q
```

## 9. Run the Docker-backed smoke test

```bash
PGMIGBENCH_RUN_INTEGRATION=1 pytest tests/test_integration_smoke.py -q
```

## 10. Stop and remove the benchmark database volume

```bash
docker compose -f docker/docker-compose.yml down -v
```
