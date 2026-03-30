from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class MigrationStep:
    name: str
    sql: str
    phase: str
    autocommit: bool = True
    batch_size: int | None = None
    sleep_ms: int = 0


@dataclass(frozen=True)
class MigrationPlan:
    strategy: str
    family: str
    phases: dict[str, list[MigrationStep]] = field(default_factory=dict)

    def steps(self) -> list[MigrationStep]:
        ordered: list[MigrationStep] = []
        for phase in ("expand", "migrate", "cutover", "contract"):
            ordered.extend(self.phases.get(phase, []))
        for phase, items in self.phases.items():
            if phase not in {"expand", "migrate", "cutover", "contract"}:
                ordered.extend(items)
        return ordered
