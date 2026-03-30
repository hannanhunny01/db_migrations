from __future__ import annotations

import random
from dataclasses import replace

from pgmigbench.scenarios.base import Scenario
from pgmigbench.scenarios.families import (
    ADD_FK,
    ADD_NOT_NULL,
    DROP_LEGACY,
    HOT_RENAME,
    TYPE_NARROW,
)


def _row_size(i: int) -> int:
    return 100_000 if i < 10 else 1_000_000


def _workload(i: int) -> str:
    return "high" if i % 2 else "low"


def _family_block(family: str) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for i in range(20):
        rows = _row_size(i)
        workload = _workload(i)
        params: dict[str, object]

        if family == HOT_RENAME:
            params = {
                "index_present": i % 2 == 0,
                "batch_size": 2_500 if rows == 100_000 else 10_000,
                "batch_sleep_ms": 20 if workload == "high" else 5,
            }
        elif family == ADD_NOT_NULL:
            params = {
                "with_default": i % 4 != 0,
                "fill_value": "basic" if i % 3 else "premium",
                "batch_size": 2_500 if rows == 100_000 else 10_000,
                "batch_sleep_ms": 20 if workload == "high" else 5,
            }
        elif family == TYPE_NARROW:
            params = {
                "overflow_variant": i % 5 == 0,
                "batch_size": 2_500 if rows == 100_000 else 10_000,
                "batch_sleep_ms": 20 if workload == "high" else 5,
            }
        elif family == DROP_LEGACY:
            params = {
                "replacement_prewarm": i % 2 == 0,
                "batch_size": 2_500 if rows == 100_000 else 10_000,
                "batch_sleep_ms": 20 if workload == "high" else 5,
            }
        elif family == ADD_FK:
            params = {
                "has_violations": i % 2 == 0,
                "cleanup_mode": "set_null" if i % 4 < 2 else "delete",
                "index_present": i % 3 != 0,
            }
        else:
            raise ValueError(f"unknown family: {family}")

        scenarios.append(
            Scenario(
                id=f"tmp_{family}_{i:02d}",
                family=family,
                variant=f"v{i+1:02d}",
                rows=rows,
                workload_level=workload,
                params=params,
            )
        )
    return scenarios


def generate_suite(
    seed: int,
    suite_size: int = 100,
    sample_per_family: int | None = None,
) -> list[Scenario]:
    if suite_size <= 0:
        return []

    families = [HOT_RENAME, ADD_NOT_NULL, TYPE_NARROW, DROP_LEGACY, ADD_FK]
    rng = random.Random(seed)
    if sample_per_family is not None:
        if sample_per_family <= 0:
            return []

        selected: list[Scenario] = []
        for family in families:
            family_scenarios = _family_block(family)
            rng.shuffle(family_scenarios)
            selected.extend(family_scenarios[: min(sample_per_family, len(family_scenarios))])
        rng.shuffle(selected)
    else:
        all_scenarios: list[Scenario] = []
        for family in families:
            all_scenarios.extend(_family_block(family))

        rng.shuffle(all_scenarios)
        selected = all_scenarios[: min(suite_size, len(all_scenarios))]

    final: list[Scenario] = []
    for idx, scenario in enumerate(selected, start=1):
        final.append(replace(scenario, id=f"{idx:03d}_{scenario.family}_{scenario.variant}"))
    return final
