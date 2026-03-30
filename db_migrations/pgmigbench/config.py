from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    compose_file: Path = Path("docker/docker-compose.yml")
    service_name: str = "postgres"
    container_name: str = "pgmigbench_postgres"
    db_host: str = "127.0.0.1"
    db_port: int = 55432
    db_user: str = "postgres"
    db_password: str = "postgres"
    db_name: str = "postgres"

    lock_timeout_ms: int = 1000
    statement_timeout_s: int = 120
    telemetry_poll_s: float = 0.1
    blocked_backend_threshold: int = 2
    blocked_samples_threshold: int = 10

    warmup_window_s: int = 5
    baseline_window_s: int = 20
    migration_observation_buffer_s: int = 10
    cutover_window_s: int = 20
    post_window_s: int = 20

    pgbench_low_clients: int = 8
    pgbench_low_jobs: int = 4
    pgbench_high_clients: int = 16
    pgbench_high_jobs: int = 8

    repetitions_per_case: int = 5

    default_small_rows: int = 100_000
    default_large_rows: int = 1_000_000

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_host=os.getenv("PGMIGBENCH_DB_HOST", cls.db_host),
            db_port=int(os.getenv("PGMIGBENCH_DB_PORT", str(cls.db_port))),
            db_user=os.getenv("PGMIGBENCH_DB_USER", cls.db_user),
            db_password=os.getenv("PGMIGBENCH_DB_PASSWORD", cls.db_password),
            db_name=os.getenv("PGMIGBENCH_DB_NAME", cls.db_name),
            warmup_window_s=int(
                os.getenv("PGMIGBENCH_WARMUP_WINDOW_S", str(cls.warmup_window_s))
            ),
            baseline_window_s=int(
                os.getenv("PGMIGBENCH_BASELINE_WINDOW_S", str(cls.baseline_window_s))
            ),
            migration_observation_buffer_s=int(
                os.getenv(
                    "PGMIGBENCH_MIGRATION_BUFFER_S",
                    os.getenv(
                        "PGMIGBENCH_MIGRATION_WINDOW_S",
                        str(cls.migration_observation_buffer_s),
                    ),
                )
            ),
            cutover_window_s=int(
                os.getenv("PGMIGBENCH_CUTOVER_WINDOW_S", str(cls.cutover_window_s))
            ),
            post_window_s=int(
                os.getenv("PGMIGBENCH_POST_WINDOW_S", str(cls.post_window_s))
            ),
            lock_timeout_ms=int(
                os.getenv("PGMIGBENCH_LOCK_TIMEOUT_MS", str(cls.lock_timeout_ms))
            ),
            statement_timeout_s=int(
                os.getenv("PGMIGBENCH_STATEMENT_TIMEOUT_S", str(cls.statement_timeout_s))
            ),
            telemetry_poll_s=float(
                os.getenv("PGMIGBENCH_TELEMETRY_POLL_S", str(cls.telemetry_poll_s))
            ),
            blocked_backend_threshold=int(
                os.getenv(
                    "PGMIGBENCH_BLOCKED_BACKEND_THRESHOLD",
                    str(cls.blocked_backend_threshold),
                )
            ),
            blocked_samples_threshold=int(
                os.getenv(
                    "PGMIGBENCH_BLOCKED_SAMPLES_THRESHOLD",
                    str(cls.blocked_samples_threshold),
                )
            ),
            pgbench_low_clients=int(
                os.getenv(
                    "PGMIGBENCH_PGBENCH_LOW_CLIENTS",
                    str(cls.pgbench_low_clients),
                )
            ),
            pgbench_low_jobs=int(
                os.getenv("PGMIGBENCH_PGBENCH_LOW_JOBS", str(cls.pgbench_low_jobs))
            ),
            pgbench_high_clients=int(
                os.getenv(
                    "PGMIGBENCH_PGBENCH_HIGH_CLIENTS",
                    str(cls.pgbench_high_clients),
                )
            ),
            pgbench_high_jobs=int(
                os.getenv("PGMIGBENCH_PGBENCH_HIGH_JOBS", str(cls.pgbench_high_jobs))
            ),
            repetitions_per_case=int(
                os.getenv(
                    "PGMIGBENCH_REPETITIONS",
                    str(cls.repetitions_per_case),
                )
            ),
        )

    def pgbench_profile(self, workload_level: str) -> tuple[int, int]:
        if workload_level == "high":
            return (self.pgbench_high_clients, self.pgbench_high_jobs)
        return (self.pgbench_low_clients, self.pgbench_low_jobs)
