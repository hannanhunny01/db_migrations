from __future__ import annotations


def latency_delta_pct(baseline_ms: float | None, migration_ms: float | None) -> float | None:
    if baseline_ms is None or migration_ms is None:
        return None
    if baseline_ms == 0:
        return None
    return ((migration_ms - baseline_ms) / baseline_ms) * 100.0
