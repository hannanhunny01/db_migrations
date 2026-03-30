from __future__ import annotations

import math


# z-score for 95% two-sided confidence interval.
_Z_95 = 1.959963984540054


def wilson_interval_95(successes: int, trials: int) -> tuple[float, float]:
    if trials <= 0:
        return (0.0, 0.0)

    p = successes / trials
    z2 = _Z_95 * _Z_95
    denom = 1.0 + z2 / trials
    center = (p + z2 / (2.0 * trials)) / denom
    margin = (_Z_95 / denom) * math.sqrt((p * (1.0 - p) / trials) + (z2 / (4.0 * trials * trials)))
    lo = max(0.0, center - margin)
    hi = min(1.0, center + margin)
    return (lo, hi)


def pct(value: float) -> str:
    return f"{value * 100.0:.1f}%"


def fmt_ci(lo: float, hi: float) -> str:
    return f"[{lo * 100.0:.1f}%, {hi * 100.0:.1f}%]"
