from __future__ import annotations

import csv
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class LockSample:
    ts_utc: str
    blocked_backends: int
    lock_waiters: int
    max_lock_wait_ms: float


class TelemetrySampler:
    def __init__(
        self,
        *,
        dsn: str,
        poll_interval_s: float,
        evidence_dir: Path,
    ) -> None:
        self._dsn = dsn
        self._poll_interval_s = poll_interval_s
        self._evidence_dir = evidence_dir
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self.samples: list[LockSample] = []

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._write_csv()

    def _loop(self) -> None:
        # Imported lazily so unit tests without DB deps remain lightweight.
        import psycopg

        try:
            with psycopg.connect(self._dsn, autocommit=True) as conn:
                while not self._stop.is_set():
                    sample = self._sample_once(conn)
                    if sample is not None:
                        self.samples.append(sample)
                    time.sleep(self._poll_interval_s)
        except Exception:
            # Telemetry failure should not crash benchmark execution.
            return

    def _sample_once(self, conn) -> LockSample | None:
        query = """
SELECT
  COALESCE((SELECT count(*) FROM pg_locks WHERE NOT granted), 0) AS blocked_backends,
  COALESCE((SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'), 0) AS lock_waiters,
  COALESCE((
    SELECT max(extract(epoch FROM (clock_timestamp() - query_start)) * 1000.0)
    FROM pg_stat_activity
    WHERE wait_event_type = 'Lock' AND query_start IS NOT NULL
  ), 0) AS max_lock_wait_ms
""".strip()
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
        if row is None:
            return None

        return LockSample(
            ts_utc=datetime.now(timezone.utc).isoformat(),
            blocked_backends=int(row[0]),
            lock_waiters=int(row[1]),
            max_lock_wait_ms=float(row[2]),
        )

    @property
    def peak_blocked_backends(self) -> int:
        if not self.samples:
            return 0
        return max(sample.blocked_backends for sample in self.samples)

    @property
    def peak_lock_wait_ms(self) -> float:
        if not self.samples:
            return 0.0
        return max(sample.max_lock_wait_ms for sample in self.samples)

    def blocked_over_threshold_count(self, threshold: int) -> int:
        return sum(1 for sample in self.samples if sample.blocked_backends > threshold)

    def max_consecutive_blocked_over_threshold(self, threshold: int) -> int:
        longest = 0
        current = 0
        for sample in self.samples:
            if sample.blocked_backends > threshold:
                current += 1
                longest = max(longest, current)
            else:
                current = 0
        return longest

    def _write_csv(self) -> None:
        self._evidence_dir.mkdir(parents=True, exist_ok=True)
        out_path = self._evidence_dir / "lock_samples.csv"
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["ts_utc", "blocked_backends", "lock_waiters", "max_lock_wait_ms"])
            for sample in self.samples:
                writer.writerow(
                    [
                        sample.ts_utc,
                        sample.blocked_backends,
                        sample.lock_waiters,
                        f"{sample.max_lock_wait_ms:.3f}",
                    ]
                )
