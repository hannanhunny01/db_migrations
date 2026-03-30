from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Scenario:
    id: str
    family: str
    variant: str
    rows: int
    workload_level: str
    params: dict[str, Any] = field(default_factory=dict)

    def parent_rows(self) -> int:
        return max(1_000, self.rows // 10)

    def signature(self) -> tuple[Any, ...]:
        return (
            self.id,
            self.family,
            self.variant,
            self.rows,
            self.workload_level,
            tuple(sorted(self.params.items())),
        )
