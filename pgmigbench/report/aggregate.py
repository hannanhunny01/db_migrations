from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pgmigbench.report.stats import fmt_ci, pct, wilson_interval_95


def _percentile(values: list[float], pct_value: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    values = sorted(values)
    rank = (pct_value / 100.0) * (len(values) - 1)
    low = int(rank)
    high = min(low + 1, len(values) - 1)
    if low == high:
        return values[low]
    weight = rank - low
    return values[low] * (1.0 - weight) + values[high] * weight


def _median(values: list[float]) -> float | None:
    return _percentile(values, 50.0)


def aggregate_rows(
    rows: list[dict[str, Any]],
    *,
    suite_size: int,
    repetitions: int,
    seed: int,
    pg_version: str,
    started_at_utc: str,
    finished_at_utc: str,
) -> dict[str, Any]:
    strategy_totals: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "runs": 0,
            "run_successes": 0,
            "lock_violations": 0,
            "unsafe_destructive_actions": 0,
            "postcheck_failures": 0,
            "compat_breaks": 0,
            "peak_lock_wait_ms": [],
            "latency_delta_pct": [],
            "duration_s": [],
        }
    )
    family_breakdown: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(
            lambda: {
                "runs": 0,
                "scenario_pairs": 0,
                "completed_pairs": 0,
                "success": 0,
                "failure": 0,
            }
        )
    )
    scenario_groups: dict[tuple[str, str], dict[int, dict[str, Any]]] = defaultdict(dict)

    for row in rows:
        strategy = row["strategy"]
        family = row["family"]
        success = bool(row["success"])
        unsafe_destructive_actions = int(
            row.get("unsafe_destructive_actions", row.get("data_loss_incidents", 0))
        )
        postcheck_failures = int(
            row.get("postcheck_failures", row.get("data_loss_incidents", 0))
        )

        agg = strategy_totals[strategy]
        agg["runs"] += 1
        agg["run_successes"] += int(success)
        agg["lock_violations"] += int(row["lock_violations"])
        agg["unsafe_destructive_actions"] += unsafe_destructive_actions
        agg["postcheck_failures"] += postcheck_failures
        agg["compat_breaks"] += int(row["compat_breaks"])

        peak_lock = row.get("peak_lock_wait_ms")
        if peak_lock not in (None, ""):
            agg["peak_lock_wait_ms"].append(float(peak_lock))

        latency_delta = row.get("latency_delta_pct")
        if latency_delta not in (None, ""):
            agg["latency_delta_pct"].append(float(latency_delta))

        duration_s = row.get("duration_s")
        if duration_s not in (None, ""):
            agg["duration_s"].append(float(duration_s))

        fam = family_breakdown[family][strategy]
        fam["runs"] += 1
        repeat_index = int(row.get("repeat_index", 1))
        scenario_groups[(strategy, str(row["scenario_id"]))][repeat_index] = row

    scenario_totals: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "scenario_pairs_total": 0,
            "scenario_pairs_completed": 0,
            "safe_success": 0,
        }
    )

    for (strategy, _scenario_id), repeats in scenario_groups.items():
        rows_by_repeat = list(repeats.values())
        family = str(rows_by_repeat[0]["family"])
        completed = len(repeats) >= repetitions
        safe_success = completed and all(bool(row["success"]) for row in rows_by_repeat)

        scenario_totals[strategy]["scenario_pairs_total"] += 1
        scenario_totals[strategy]["scenario_pairs_completed"] += int(completed)
        scenario_totals[strategy]["safe_success"] += int(safe_success)

        fam = family_breakdown[family][strategy]
        fam["scenario_pairs"] += 1
        fam["completed_pairs"] += int(completed)
        fam["success"] += int(safe_success)
        fam["failure"] += int(completed and not safe_success)

    strategy_summary: dict[str, dict[str, Any]] = {}
    for strategy, agg in strategy_totals.items():
        runs = int(agg["runs"])
        safe = int(scenario_totals[strategy]["safe_success"])
        lo, hi = wilson_interval_95(safe, suite_size)
        strategy_summary[strategy] = {
            "runs": runs,
            "run_successes": int(agg["run_successes"]),
            "run_success_ratio": (int(agg["run_successes"]) / runs) if runs else 0.0,
            "scenario_pairs_total": int(scenario_totals[strategy]["scenario_pairs_total"]),
            "scenario_pairs_completed": int(
                scenario_totals[strategy]["scenario_pairs_completed"]
            ),
            "safe_success": safe,
            "safe_success_ratio": (safe / suite_size) if suite_size else 0.0,
            "safe_success_pct": pct((safe / suite_size) if suite_size else 0.0),
            "safe_success_ci_95": fmt_ci(lo, hi),
            "safe_success_ci_95_bounds": [lo, hi],
            "lock_violations": int(agg["lock_violations"]),
            "unsafe_destructive_actions": int(agg["unsafe_destructive_actions"]),
            "postcheck_failures": int(agg["postcheck_failures"]),
            "compat_breaks": int(agg["compat_breaks"]),
            "ops": {
                "peak_lock_wait_ms_p95": _percentile(list(agg["peak_lock_wait_ms"]), 95.0),
                "latency_delta_pct_p95": _percentile(list(agg["latency_delta_pct"]), 95.0),
                "duration_s_median": _median(list(agg["duration_s"])),
            },
        }

    families_out = {
        family: {strategy: dict(values) for strategy, values in by_strategy.items()}
        for family, by_strategy in family_breakdown.items()
    }

    return {
        "suite_size": suite_size,
        "repetitions_per_scenario_strategy": repetitions,
        "seed": seed,
        "pg_version": pg_version,
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategies": strategy_summary,
        "families": families_out,
        "rows": rows,
    }


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
