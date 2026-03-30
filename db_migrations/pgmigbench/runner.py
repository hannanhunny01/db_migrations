from __future__ import annotations

import csv
import hashlib
import json
import re
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

from pgmigbench.config import Settings
from pgmigbench.docker import compose_down, compose_up, wait_until_ready
from pgmigbench.report.aggregate import aggregate_rows, write_summary
from pgmigbench.scenarios import Scenario, generate_suite
from pgmigbench.scenarios.families import ADD_FK, ADD_NOT_NULL, DROP_LEGACY, HOT_RENAME, TYPE_NARROW
from pgmigbench.strategies import MigrationPlan, build_baseline_a_plan, build_baseline_b_plan
from pgmigbench.strategies.alembic_exec import execute_alembic_monolith
from pgmigbench.telemetry import TelemetrySampler, latency_delta_pct
from pgmigbench.workload.pgbench import (
    PgbenchResult,
    finish_pgbench,
    render_script,
    run_pgbench,
    start_looping_pgbench,
    start_pgbench,
    stop_looping_pgbench,
)


CSV_COLUMNS = [
    "run_id",
    "repeat_index",
    "scenario_id",
    "family",
    "strategy",
    "variant",
    "rows",
    "workload_level",
    "success",
    "lock_violations",
    "unsafe_destructive_actions",
    "postcheck_failures",
    "compat_breaks",
    "peak_blocked_backends",
    "peak_lock_wait_ms",
    "baseline_latency_ms",
    "mig_latency_ms",
    "latency_delta_pct",
    "duration_s",
    "pg_version",
    "seed",
    "started_at_utc",
    "error",
]

_COMPAT_PATTERN = re.compile(
    r"does not exist|invalid input syntax|cannot cast|violates .* constraint|undefined column",
    re.IGNORECASE,
)
_LOCK_FAIL_PATTERN = re.compile(r"lock timeout|statement timeout|deadlock", re.IGNORECASE)


@dataclass(frozen=True)
class RunnerResult:
    csv_path: Path
    summary_path: Path
    run_id: str


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _current_git_commit(project_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(project_root),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        return (result.stdout or "").strip() or "unknown"
    return "unknown"


def _actual_server_version(conn: psycopg.Connection[Any]) -> str:
    with conn.cursor() as cur:
        cur.execute("SHOW server_version")
        row = cur.fetchone()
    return str(row[0]) if row and row[0] else "unknown"


def _load_template(name: str) -> str:
    from importlib import resources

    return (
        resources.files("pgmigbench.workload.scripts")
        .joinpath(name)
        .read_text(encoding="utf-8")
    )


def _family_scripts(family: str) -> tuple[str, str]:
    mapping = {
        HOT_RENAME: ("rename_old.sql", "rename_new.sql"),
        ADD_NOT_NULL: ("addnn_old.sql", "addnn_new.sql"),
        TYPE_NARROW: ("typenarrow_old.sql", "typenarrow_new.sql"),
        DROP_LEGACY: ("drop_old.sql", "drop_new.sql"),
        ADD_FK: ("fk_old.sql", "fk_new.sql"),
    }
    if family not in mapping:
        raise ValueError(f"unsupported family: {family}")
    old_name, new_name = mapping[family]
    return (_load_template(old_name), _load_template(new_name))


def _pgbench_seed(base_seed: int, scenario_id: str, strategy: str, tag: str) -> int:
    token = f"{base_seed}:{scenario_id}:{strategy}:{tag}"
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    seed = int.from_bytes(digest[:4], "big") % 2147483647
    return seed if seed > 0 else 1


def _init_db(conn: psycopg.Connection[Any]) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")


def _reset_schema(conn: psycopg.Connection[Any], scenario: Scenario) -> None:
    parent_rows = scenario.parent_rows()
    with conn.cursor() as cur:
        # Baseline A uses Alembic; clear previous revision state per scenario.
        cur.execute("DROP TABLE IF EXISTS public.alembic_version")
        cur.execute("DROP SCHEMA IF EXISTS bench CASCADE")
        cur.execute("CREATE SCHEMA bench")

        cur.execute(
            """
CREATE TABLE bench.parent (
  id bigserial PRIMARY KEY,
  payload text
)
""".strip()
        )
        cur.execute(
            """
CREATE TABLE bench.child (
  id bigserial PRIMARY KEY,
  parent_id bigint,
  hot_col bigint,
  legacy_col bigint,
  payload text
)
""".strip()
        )

        has_fk_violations = bool(scenario.params.get("has_violations", False))
        overflow_variant = bool(scenario.params.get("overflow_variant", False))

        cur.execute(
            "INSERT INTO bench.parent (id, payload) "
            "SELECT g, md5(g::text) FROM generate_series(1, %s) AS g",
            (parent_rows,),
        )
        cur.execute("SELECT setval('bench.parent_id_seq', %s, true)", (parent_rows,))

        cur.execute(
            """
INSERT INTO bench.child(id, parent_id, hot_col, legacy_col, payload)
SELECT
  g,
  (
    CASE
      WHEN %s AND g %% 20 = 0 THEN %s + (g %% 1000)
      ELSE ((g - 1) %% %s) + 1
    END
  )::bigint,
  (g %% 100000)::bigint,
  (
    CASE
      WHEN %s AND g %% 25 = 0 THEN 3000000000::bigint + g
      ELSE (g %% 100000)::bigint
    END
  )::bigint,
  md5(g::text)
FROM generate_series(1, %s) AS g
""".strip(),
            (has_fk_violations, parent_rows, parent_rows, overflow_variant, scenario.rows),
        )
        cur.execute("SELECT setval('bench.child_id_seq', %s, true)", (scenario.rows,))

        if scenario.family == HOT_RENAME and bool(scenario.params.get("index_present", False)):
            cur.execute("CREATE INDEX idx_child_hot_col ON bench.child(hot_col)")

        if scenario.family == ADD_FK and bool(scenario.params.get("index_present", False)):
            cur.execute("CREATE INDEX idx_child_parent_id ON bench.child(parent_id)")

        if scenario.family == DROP_LEGACY and bool(scenario.params.get("replacement_prewarm", False)):
            cur.execute("ALTER TABLE bench.child ADD COLUMN legacy_col_new bigint")
            cur.execute("UPDATE bench.child SET legacy_col_new = legacy_col")

        cur.execute("ANALYZE bench.parent")
        cur.execute("ANALYZE bench.child")


def _capture_sentinel(conn: psycopg.Connection[Any], scenario: Scenario) -> dict[str, int]:
    sentinel: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
SELECT
  count(*)::bigint,
  COALESCE(sum(hot_col), 0)::bigint,
  COALESCE(sum(legacy_col), 0)::bigint,
  count(*) FILTER (WHERE legacy_col BETWEEN -2147483648 AND 2147483647)::bigint,
  COALESCE(sum(legacy_col) FILTER (WHERE legacy_col BETWEEN -2147483648 AND 2147483647), 0)::bigint
FROM bench.child
""".strip()
        )
        row = cur.fetchone()
        if row:
            sentinel["row_count"] = int(row[0])
            sentinel["sum_hot"] = int(row[1])
            sentinel["sum_legacy"] = int(row[2])
            sentinel["valid_legacy_count"] = int(row[3])
            sentinel["sum_valid_legacy"] = int(row[4])

        if scenario.family == ADD_FK:
            cur.execute(
                """
SELECT count(*)
FROM bench.child c
WHERE c.parent_id IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM bench.parent p WHERE p.id = c.parent_id)
""".strip()
            )
            fk_invalid = cur.fetchone()
            sentinel["invalid_fk_count"] = int(fk_invalid[0] if fk_invalid else 0)

    return sentinel


def _postcheck_failures(
    conn: psycopg.Connection[Any],
    scenario: Scenario,
    strategy: str,
    sentinel: dict[str, int],
    migration_ok: bool,
) -> int:
    if not migration_ok:
        return 0

    try:
        with conn.cursor() as cur:
            if scenario.family == HOT_RENAME:
                cur.execute("SELECT COALESCE(sum(hot_col_new), 0)::bigint FROM bench.child")
                value = cur.fetchone()
                return int(int(value[0] if value else 0) != sentinel.get("sum_hot", 0))

            if scenario.family == ADD_NOT_NULL:
                cur.execute(
                    "SELECT count(*) FROM bench.child WHERE customer_tier IS NULL"
                )
                nulls = cur.fetchone()
                return int(int(nulls[0] if nulls else 0) > 0)

            if scenario.family == TYPE_NARROW:
                cur.execute("SELECT count(*)::bigint FROM bench.child")
                row_count = cur.fetchone()
                if int(row_count[0] if row_count else 0) != sentinel.get("row_count", 0):
                    return 1

                cur.execute(
                    """
SELECT COALESCE(sum(legacy_col_int), 0)::bigint
FROM bench.child
WHERE legacy_col_int IS NOT NULL
""".strip()
                )
                sum_valid = cur.fetchone()
                return int(
                    int(sum_valid[0] if sum_valid else 0)
                    != sentinel.get("sum_valid_legacy", 0)
                )

            if scenario.family == DROP_LEGACY:
                cur.execute("SELECT COALESCE(sum(legacy_col_new), 0)::bigint FROM bench.child")
                sum_new = cur.fetchone()
                return int(int(sum_new[0] if sum_new else 0) != sentinel.get("sum_legacy", 0))

            if scenario.family == ADD_FK:
                cur.execute(
                    """
SELECT count(*)
FROM bench.child c
WHERE c.parent_id IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM bench.parent p WHERE p.id = c.parent_id)
""".strip()
                )
                invalid = cur.fetchone()
                return int(int(invalid[0] if invalid else 0) > 0)
    except Exception:
        return 1

    return 0


def _unsafe_destructive_action_count(
    *,
    scenario: Scenario,
    strategy: str,
    migration_ok: bool,
) -> int:
    if not migration_ok:
        return 0

    if strategy != "baseline_a":
        return 0

    if scenario.family in {DROP_LEGACY, TYPE_NARROW}:
        return 1

    return 0


def _compat_break_count(results: list[PgbenchResult]) -> int:
    breaks = 0
    for result in results:
        text = f"{result.stdout}\n{result.stderr}"
        if (not result.ok) or _COMPAT_PATTERN.search(text):
            breaks += 1
    return breaks


def _execute_step(
    conn: psycopg.Connection[Any],
    step_sql: str,
    *,
    sql_log: Path,
    step_name: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(step_sql)

    with sql_log.open("a", encoding="utf-8") as f:
        f.write(f"-- {step_name} @ {_now_utc()}\n{step_sql};\n\n")


def _execute_plan(
    conn: psycopg.Connection[Any],
    plan: MigrationPlan,
    *,
    settings: Settings,
    sql_log: Path,
    phases: tuple[str, ...],
) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SET lock_timeout = '{settings.lock_timeout_ms}ms'")
        cur.execute(f"SET statement_timeout = '{settings.statement_timeout_s}s'")

    for phase in phases:
        for step in plan.phases.get(phase, []):
            if step.batch_size:
                with conn.cursor() as cur:
                    cur.execute("SELECT COALESCE(max(id), 0) FROM bench.child")
                    max_id_row = cur.fetchone()
                    max_id = int(max_id_row[0] if max_id_row else 0)
                lo = 1
                while lo <= max_id:
                    hi = min(lo + step.batch_size - 1, max_id)
                    sql = step.sql.format(lo=lo, hi=hi)
                    _execute_step(conn, sql, sql_log=sql_log, step_name=f"{step.name}[{lo}-{hi}]")
                    if step.sleep_ms > 0:
                        time.sleep(step.sleep_ms / 1000.0)
                    lo = hi + 1
            else:
                _execute_step(conn, step.sql, sql_log=sql_log, step_name=step.name)


def _select_plan(scenario: Scenario, strategy: str) -> MigrationPlan:
    if strategy == "baseline_a":
        return build_baseline_a_plan(scenario)
    if strategy == "baseline_b":
        return build_baseline_b_plan(scenario)
    raise ValueError(f"unsupported strategy: {strategy}")


def _prepare_evidence_dir(
    out_dir: Path,
    run_id: str,
    scenario_id: str,
    strategy: str,
    repeat_index: int,
) -> Path:
    evidence_dir = out_dir / "evidence" / run_id / scenario_id / strategy / f"rep_{repeat_index:02d}"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    return evidence_dir


def _legacy_usage_tokens(family: str) -> list[str]:
    if family == HOT_RENAME:
        return ["hot_col"]
    if family == TYPE_NARROW:
        return ["legacy_col"]
    if family == DROP_LEGACY:
        return ["legacy_col"]
    return []


def _legacy_usage_calls(conn: psycopg.Connection[Any], tokens: list[str]) -> int:
    if not tokens:
        return 0

    regex_ors = " OR ".join([f"query ~* %s" for _ in tokens])
    sql = f"""
SELECT COALESCE(sum(calls), 0)
FROM pg_stat_statements
WHERE query ILIKE '%%bench.child%%'
  AND ({regex_ors})
""".strip()
    regex_patterns = [rf"\\m{token}\\M" for token in tokens]

    with conn.cursor() as cur:
        cur.execute(sql, regex_patterns)
        row = cur.fetchone()
    return int(row[0] if row else 0)


def _observe_migration_with_old_workload(
    *,
    settings: Settings,
    project_root: Path,
    evidence_dir: Path,
    old_script: str,
    workload_level: str,
    scenario_id: str,
    strategy: str,
    seed: int,
    migration_action,
) -> tuple[TelemetrySampler, PgbenchResult, bool, str]:
    sampler = TelemetrySampler(
        dsn=settings.dsn,
        poll_interval_s=settings.telemetry_poll_s,
        evidence_dir=evidence_dir,
    )
    sampler.start()

    handle = start_looping_pgbench(
        settings=settings,
        project_root=project_root,
        evidence_dir=evidence_dir,
        script_text=old_script,
        workload_level=workload_level,
        tag="migration_old",
        random_seed=_pgbench_seed(seed, scenario_id, strategy, "migration_old"),
    )

    migration_ok = True
    error_text = ""
    try:
        migration_action()
    except Exception as exc:  # noqa: BLE001
        migration_ok = False
        error_text = str(exc)
    finally:
        time.sleep(settings.migration_observation_buffer_s)
        mig_workload_result = stop_looping_pgbench(handle)
        sampler.stop()

    return sampler, mig_workload_result, migration_ok, error_text


def _run_single(
    *,
    conn: psycopg.Connection[Any],
    settings: Settings,
    project_root: Path,
    out_dir: Path,
    run_id: str,
    repeat_index: int,
    scenario: Scenario,
    strategy: str,
    seed: int,
    requested_pg_version: str,
    actual_pg_version: str,
) -> dict[str, Any]:
    started_at = _now_utc()

    _reset_schema(conn, scenario)
    sentinel = _capture_sentinel(conn, scenario)

    plan = _select_plan(scenario, strategy)

    evidence_dir = _prepare_evidence_dir(out_dir, run_id, scenario.id, strategy, repeat_index)
    (evidence_dir / "environment.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "repeat_index": repeat_index,
                "scenario_id": scenario.id,
                "strategy": strategy,
                "pg_version_requested": requested_pg_version,
                "pg_version_actual": actual_pg_version,
                "started_at_utc": started_at,
                "runner_git_commit": _current_git_commit(project_root),
                "generated_at_utc": _now_utc(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (evidence_dir / "scenario.json").write_text(
        json.dumps(
            {
                "scenario": {
                    "id": scenario.id,
                    "family": scenario.family,
                    "variant": scenario.variant,
                    "rows": scenario.rows,
                    "workload_level": scenario.workload_level,
                    "params": scenario.params,
                },
                "strategy": strategy,
                "plan": [
                    {
                        "name": s.name,
                        "phase": s.phase,
                        "sql": s.sql,
                        "batch_size": s.batch_size,
                        "sleep_ms": s.sleep_ms,
                    }
                    for s in plan.steps()
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    old_template, new_template = _family_scripts(scenario.family)
    old_script = render_script(old_template, max_id=scenario.rows)
    new_script = render_script(new_template, max_id=scenario.rows)

    warmup_workload = run_pgbench(
        settings=settings,
        project_root=project_root,
        evidence_dir=evidence_dir,
        script_text=old_script,
        duration_s=settings.warmup_window_s,
        workload_level=scenario.workload_level,
        tag="warmup_old",
        random_seed=_pgbench_seed(seed, scenario.id, strategy, "warmup_old"),
    )
    if not warmup_workload.ok:
        raise RuntimeError("warm-up workload failed before migration execution")

    baseline_workload = run_pgbench(
        settings=settings,
        project_root=project_root,
        evidence_dir=evidence_dir,
        script_text=old_script,
        duration_s=settings.baseline_window_s,
        workload_level=scenario.workload_level,
        tag="baseline_old",
        random_seed=_pgbench_seed(seed, scenario.id, strategy, "baseline_old"),
    )
    if not baseline_workload.ok:
        raise RuntimeError("baseline workload failed before migration execution")

    compat_results: list[PgbenchResult] = []
    post_window_done = False
    migration_ok = True
    error_text = ""
    mig_workload_result: PgbenchResult | None = None
    sampler: TelemetrySampler | None = None
    duration_s: float | None = None

    sql_log = evidence_dir / "migration_steps.sql"
    migration_t0 = time.monotonic()
    try:
        if strategy == "baseline_a":
            def _baseline_a_action() -> None:
                execute_alembic_monolith(
                    plan=plan,
                    dsn=settings.dsn,
                    evidence_dir=evidence_dir,
                    migration_sql_log=sql_log,
                )

            sampler, mig_workload_result, migration_ok, error_text = _observe_migration_with_old_workload(
                settings=settings,
                project_root=project_root,
                evidence_dir=evidence_dir,
                old_script=old_script,
                workload_level=scenario.workload_level,
                scenario_id=scenario.id,
                strategy=strategy,
                seed=seed,
                migration_action=_baseline_a_action,
            )
            compat_results.append(mig_workload_result)
            duration_s = time.monotonic() - migration_t0
            post_migration_result = run_pgbench(
                settings=settings,
                project_root=project_root,
                evidence_dir=evidence_dir,
                script_text=new_script,
                duration_s=settings.post_window_s,
                workload_level=scenario.workload_level,
                tag="post_migration_new",
                random_seed=_pgbench_seed(seed, scenario.id, strategy, "post_migration_new"),
            )
            compat_results.append(post_migration_result)
            post_window_done = True
        else:
            def _baseline_b_action() -> None:
                _execute_plan(
                    conn,
                    plan,
                    settings=settings,
                    sql_log=sql_log,
                    phases=("expand", "migrate"),
                )

            sampler, mig_workload_result, migration_ok, error_text = _observe_migration_with_old_workload(
                settings=settings,
                project_root=project_root,
                evidence_dir=evidence_dir,
                old_script=old_script,
                workload_level=scenario.workload_level,
                scenario_id=scenario.id,
                strategy=strategy,
                seed=seed,
                migration_action=_baseline_b_action,
            )
            compat_results.append(mig_workload_result)

            _execute_plan(
                conn,
                plan,
                settings=settings,
                sql_log=sql_log,
                phases=("cutover",),
            )

            cutover_old = start_pgbench(
                settings=settings,
                project_root=project_root,
                evidence_dir=evidence_dir,
                script_text=old_script,
                duration_s=settings.cutover_window_s,
                workload_level=scenario.workload_level,
                tag="cutover_old",
                random_seed=_pgbench_seed(seed, scenario.id, strategy, "cutover_old"),
            )
            cutover_new = start_pgbench(
                settings=settings,
                project_root=project_root,
                evidence_dir=evidence_dir,
                script_text=new_script,
                duration_s=settings.cutover_window_s,
                workload_level=scenario.workload_level,
                tag="cutover_new",
                random_seed=_pgbench_seed(seed, scenario.id, strategy, "cutover_new"),
            )
            compat_results.append(
                finish_pgbench(settings=settings, project_root=project_root, handle=cutover_old)
            )
            compat_results.append(
                finish_pgbench(settings=settings, project_root=project_root, handle=cutover_new)
            )

            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_stat_statements_reset()")
            except Exception:
                pass

            post_migration_result = run_pgbench(
                settings=settings,
                project_root=project_root,
                evidence_dir=evidence_dir,
                script_text=new_script,
                duration_s=settings.post_window_s,
                workload_level=scenario.workload_level,
                tag="post_migration_new",
                random_seed=_pgbench_seed(seed, scenario.id, strategy, "post_migration_new"),
            )
            compat_results.append(post_migration_result)
            post_window_done = True

            legacy_tokens = _legacy_usage_tokens(scenario.family)
            if legacy_tokens:
                calls = _legacy_usage_calls(conn, legacy_tokens)
                if calls > 0:
                    compat_results.append(
                        PgbenchResult(
                            ok=False,
                            latency_ms=None,
                            p95_latency_ms=None,
                            tps=None,
                            stdout="",
                            stderr=(
                                "legacy usage still observed during contract gate "
                                f"(tokens={legacy_tokens}, calls={calls})"
                            ),
                        )
                    )
                    raise RuntimeError(
                        "contract gate failed: legacy usage still observed "
                        f"(tokens={legacy_tokens}, calls={calls})"
                    )

            _execute_plan(
                conn,
                plan,
                settings=settings,
                sql_log=sql_log,
                phases=("contract",),
            )
            duration_s = time.monotonic() - migration_t0

    except Exception as exc:  # noqa: BLE001
        migration_ok = False
        if not error_text:
            error_text = str(exc)
        if duration_s is None:
            duration_s = time.monotonic() - migration_t0

    if not post_window_done:
        try:
            post_migration_result = run_pgbench(
                settings=settings,
                project_root=project_root,
                evidence_dir=evidence_dir,
                script_text=new_script,
                duration_s=settings.post_window_s,
                workload_level=scenario.workload_level,
                tag="post_migration_new",
                random_seed=_pgbench_seed(seed, scenario.id, strategy, "post_migration_new"),
            )
            compat_results.append(post_migration_result)
        except Exception as exc:  # noqa: BLE001
            if not error_text:
                error_text = f"post-migration workload failed: {exc}"
            migration_ok = False

    blocked_pressure = bool(
        sampler
        and (
            sampler.max_consecutive_blocked_over_threshold(settings.blocked_backend_threshold)
            >= settings.blocked_samples_threshold
        )
    )
    lock_violations = int(blocked_pressure or bool(_LOCK_FAIL_PATTERN.search(error_text)))

    compat_breaks = _compat_break_count(compat_results)
    unsafe_destructive_actions = _unsafe_destructive_action_count(
        scenario=scenario,
        strategy=strategy,
        migration_ok=migration_ok,
    )
    postcheck_failures = _postcheck_failures(
        conn,
        scenario,
        strategy,
        sentinel,
        migration_ok,
    )

    baseline_latency = baseline_workload.p95_latency_ms or baseline_workload.latency_ms
    mig_latency = (
        (mig_workload_result.p95_latency_ms or mig_workload_result.latency_ms)
        if mig_workload_result
        else None
    )
    delta = latency_delta_pct(baseline_latency, mig_latency)

    success = (
        migration_ok
        and lock_violations == 0
        and unsafe_destructive_actions == 0
        and postcheck_failures == 0
        and compat_breaks == 0
    )
    if duration_s is None:
        duration_s = time.monotonic() - migration_t0
    finished_at = _now_utc()

    (evidence_dir / "run_config.json").write_text(
        json.dumps(
            {
                "repeat_index": repeat_index,
                "warmup_window_s": settings.warmup_window_s,
                "baseline_window_s": settings.baseline_window_s,
                "migration_observation_buffer_s": settings.migration_observation_buffer_s,
                "cutover_window_s": settings.cutover_window_s,
                "post_window_s": settings.post_window_s,
                "lock_timeout_ms": settings.lock_timeout_ms,
                "statement_timeout_s": settings.statement_timeout_s,
                "telemetry_poll_s": settings.telemetry_poll_s,
                "blocked_backend_threshold": settings.blocked_backend_threshold,
                "blocked_samples_threshold": settings.blocked_samples_threshold,
                "pgbench_low_profile": {
                    "clients": settings.pgbench_low_clients,
                    "jobs": settings.pgbench_low_jobs,
                },
                "pgbench_high_profile": {
                    "clients": settings.pgbench_high_clients,
                    "jobs": settings.pgbench_high_jobs,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (evidence_dir / "timing.json").write_text(
        json.dumps(
            {
                "started_at_utc": started_at,
                "finished_at_utc": finished_at,
                "duration_s": round(duration_s, 3),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return {
        "run_id": run_id,
        "repeat_index": repeat_index,
        "scenario_id": scenario.id,
        "family": scenario.family,
        "strategy": strategy,
        "variant": scenario.variant,
        "rows": scenario.rows,
        "workload_level": scenario.workload_level,
        "success": int(success),
        "lock_violations": lock_violations,
        "unsafe_destructive_actions": unsafe_destructive_actions,
        "postcheck_failures": postcheck_failures,
        "compat_breaks": compat_breaks,
        "peak_blocked_backends": sampler.peak_blocked_backends if sampler else 0,
        "peak_lock_wait_ms": round(sampler.peak_lock_wait_ms, 3) if sampler else 0.0,
        "baseline_latency_ms": round(baseline_latency, 3) if baseline_latency is not None else "",
        "mig_latency_ms": round(mig_latency, 3) if mig_latency is not None else "",
        "latency_delta_pct": round(delta, 3) if delta is not None else "",
        "duration_s": round(duration_s, 3),
        "pg_version": actual_pg_version,
        "seed": seed,
        "started_at_utc": started_at,
        "error": error_text,
    }


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows: list[dict[str, Any]] = []
        for row in reader:
            normalized = dict(row)
            if "repeat_index" not in normalized:
                normalized["repeat_index"] = "1"
            if "unsafe_destructive_actions" not in normalized:
                normalized["unsafe_destructive_actions"] = normalized.get("data_loss_incidents", "0")
            if "postcheck_failures" not in normalized:
                normalized["postcheck_failures"] = normalized.get("data_loss_incidents", "0")

            parsed: dict[str, Any] = {}
            for key, value in normalized.items():
                if key in {
                    "repeat_index",
                    "rows",
                    "lock_violations",
                    "unsafe_destructive_actions",
                    "postcheck_failures",
                    "compat_breaks",
                    "seed",
                    "peak_blocked_backends",
                }:
                    parsed[key] = int(value)
                elif key in {"success"}:
                    parsed[key] = bool(int(value))
                elif key in {
                    "peak_lock_wait_ms",
                    "baseline_latency_ms",
                    "mig_latency_ms",
                    "latency_delta_pct",
                    "duration_s",
                }:
                    parsed[key] = float(value) if value not in {"", "None"} else None
                else:
                    parsed[key] = value
            rows.append(parsed)
        return rows


def _first_error_line(error: str | None) -> str:
    if not error:
        return ""
    return error.splitlines()[0].strip()


def _detect_systemic_break(rows: list[dict[str, Any]]) -> str | None:
    # Abort when one runtime error pattern dominates failures across families,
    # which usually indicates infrastructure/code bugs rather than scenario behavior.
    by_strategy: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_strategy.setdefault(str(row.get("strategy", "")), []).append(row)

    for strategy, srows in by_strategy.items():
        successes = [r for r in srows if bool(r.get("success"))]
        failures = [r for r in srows if not bool(r.get("success"))]
        if len(failures) < 8:
            continue

        by_error: dict[str, list[dict[str, Any]]] = {}
        for row in failures:
            err = _first_error_line(str(row.get("error", "")))
            if not err:
                continue
            by_error.setdefault(err, []).append(row)

        for err, erows in by_error.items():
            families = {str(r.get("family", "")) for r in erows}
            repeat_count = len(erows)
            dominance = repeat_count / len(failures)
            if repeat_count < 8 or len(families) < 3:
                continue

            if not successes:
                return (
                    f"systemic failure detected for {strategy}: "
                    f"'{err}' repeated {repeat_count} times across {len(families)} families "
                    "with zero successful runs"
                )

            # Catch partial but still clearly systemic failures, e.g. one runtime
            # bug dominating most failed runs.
            if dominance >= 0.6:
                return (
                    f"systemic failure detected for {strategy}: "
                    f"'{err}' accounts for {repeat_count}/{len(failures)} failures "
                    f"across {len(families)} families"
                )

    return None


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            out_row = dict(row)
            if isinstance(out_row.get("success"), bool):
                out_row["success"] = int(out_row["success"])
            writer.writerow(out_row)


def _write_summary_snapshot(
    *,
    out_dir: Path,
    run_id: str,
    rows: list[dict[str, Any]],
    suite_size: int,
    repetitions: int,
    seed: int,
    pg_version: str,
    started_at: str,
    systemic_failure: str | None = None,
) -> Path:
    summary_path = out_dir / "summary.json"
    summary = aggregate_rows(
        rows,
        suite_size=suite_size,
        repetitions=repetitions,
        seed=seed,
        pg_version=pg_version,
        started_at_utc=started_at,
        finished_at_utc=_now_utc(),
    )
    summary["run_id"] = run_id
    if systemic_failure:
        summary["systemic_failure"] = systemic_failure
    write_summary(summary_path, summary)
    return summary_path


def run_suite(
    *,
    suite_size: int,
    seed: int,
    out_dir: Path,
    pg_version: str,
    sample_per_family: int | None,
    resume: bool,
    project_root: Path,
    settings: Settings,
    shutdown: bool = False,
) -> RunnerResult:
    compose_up(settings, project_root, pg_version)
    wait_until_ready(settings, project_root)
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / "benchmark_results.csv"

        existing_rows = _read_csv_rows(csv_path) if resume else []
        existing_keys = {
            (
                str(row["scenario_id"]),
                str(row["strategy"]),
                int(row.get("repeat_index", 1)),
            ): row
            for row in existing_rows
        }

        if existing_rows and resume:
            run_id = str(existing_rows[0]["run_id"])
            started_at = str(existing_rows[0]["started_at_utc"])
        else:
            run_id = str(uuid.uuid4())
            started_at = _now_utc()
            existing_rows = []
            existing_keys = {}

        with psycopg.connect(settings.dsn, autocommit=True) as conn:
            _init_db(conn)
            actual_pg_version = _actual_server_version(conn)

            scenarios = generate_suite(
                seed=seed,
                suite_size=suite_size,
                sample_per_family=sample_per_family,
            )
            effective_suite_size = len(scenarios)
            all_rows = list(existing_rows)
            repetitions = max(1, settings.repetitions_per_case)
            targets = [
                (scenario, strategy, repeat_index)
                for scenario in scenarios
                for strategy in ("baseline_a", "baseline_b")
                for repeat_index in range(1, repetitions + 1)
            ]
            total_targets = len(targets)
            resume_hits = sum(
                1
                for scenario, strategy, repeat_index in targets
                if (scenario.id, strategy, repeat_index) in existing_keys
            )
            preflight_systemic = _detect_systemic_break(all_rows)
            if preflight_systemic:
                (out_dir / "systemic_failure.json").write_text(
                    json.dumps(
                        {
                            "run_id": run_id,
                            "detected_at_utc": _now_utc(),
                            "message": preflight_systemic,
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                _write_summary_snapshot(
                    out_dir=out_dir,
                    run_id=run_id,
                    rows=all_rows,
                    suite_size=effective_suite_size,
                    repetitions=repetitions,
                    seed=seed,
                    pg_version=actual_pg_version,
                    started_at=started_at,
                    systemic_failure=preflight_systemic,
                )
                raise RuntimeError(preflight_systemic)

            print(
                (
                    f"starting benchmark run_id={run_id} "
                    f"targets={total_targets} resume_hits={resume_hits} "
                    f"suite_size={effective_suite_size} repetitions={repetitions} "
                    f"sample_per_family={sample_per_family} "
                    f"seed={seed} pg_version={actual_pg_version}"
                ),
                flush=True,
            )

            for index, (scenario, strategy, repeat_index) in enumerate(targets, start=1):
                key = (scenario.id, strategy, repeat_index)
                if key in existing_keys:
                    print(
                        (
                            f"[{index}/{total_targets}] skip "
                            f"scenario={scenario.id} strategy={strategy} rep={repeat_index} "
                            "(already completed)"
                        ),
                        flush=True,
                    )
                    continue

                print(
                    (
                        f"[{index}/{total_targets}] start "
                        f"scenario={scenario.id} family={scenario.family} "
                        f"strategy={strategy} rep={repeat_index} "
                        f"rows={scenario.rows} workload={scenario.workload_level}"
                    ),
                    flush=True,
                )
                row = _run_single(
                    conn=conn,
                    settings=settings,
                    project_root=project_root,
                    out_dir=out_dir,
                    run_id=run_id,
                    repeat_index=repeat_index,
                    scenario=scenario,
                    strategy=strategy,
                    seed=seed,
                    requested_pg_version=pg_version,
                    actual_pg_version=actual_pg_version,
                )
                all_rows.append(row)
                _write_csv(csv_path, all_rows)
                systemic_break = _detect_systemic_break(all_rows)
                if systemic_break:
                    (out_dir / "systemic_failure.json").write_text(
                        json.dumps(
                            {
                                "run_id": run_id,
                                "detected_at_utc": _now_utc(),
                                "message": systemic_break,
                                "executed_rows": len(all_rows),
                            },
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                    _write_summary_snapshot(
                        out_dir=out_dir,
                        run_id=run_id,
                        rows=all_rows,
                        suite_size=effective_suite_size,
                        repetitions=repetitions,
                        seed=seed,
                        pg_version=actual_pg_version,
                        started_at=started_at,
                        systemic_failure=systemic_break,
                    )
                    raise RuntimeError(systemic_break)
                print(
                    (
                        f"[{index}/{total_targets}] done "
                        f"status={'success' if row['success'] else 'failed'} "
                        f"rep={row['repeat_index']} "
                        f"lock_viol={row['lock_violations']} "
                        f"unsafe={row['unsafe_destructive_actions']} "
                        f"postcheck={row['postcheck_failures']} "
                        f"compat={row['compat_breaks']} "
                        f"duration_s={row['duration_s']}"
                    ),
                    flush=True,
                )

        summary_path = _write_summary_snapshot(
            out_dir=out_dir,
            run_id=run_id,
            rows=all_rows,
            suite_size=effective_suite_size,
            repetitions=repetitions,
            seed=seed,
            pg_version=actual_pg_version,
            started_at=started_at,
        )
        return RunnerResult(csv_path=csv_path, summary_path=summary_path, run_id=run_id)
    finally:
        if shutdown:
            compose_down(settings, project_root, remove_volumes=False)
