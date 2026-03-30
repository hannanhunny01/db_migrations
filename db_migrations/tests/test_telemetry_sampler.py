from pathlib import Path

from pgmigbench.telemetry.sampler import LockSample, TelemetrySampler


def test_max_consecutive_blocked_over_threshold_counts_streaks() -> None:
    sampler = TelemetrySampler(
        dsn="postgresql://unused",
        poll_interval_s=0.1,
        evidence_dir=Path("."),
    )
    sampler.samples = [
        LockSample("2026-01-01T00:00:00+00:00", blocked_backends=1, lock_waiters=0, max_lock_wait_ms=0.0),
        LockSample("2026-01-01T00:00:01+00:00", blocked_backends=3, lock_waiters=1, max_lock_wait_ms=10.0),
        LockSample("2026-01-01T00:00:02+00:00", blocked_backends=4, lock_waiters=2, max_lock_wait_ms=12.0),
        LockSample("2026-01-01T00:00:03+00:00", blocked_backends=0, lock_waiters=0, max_lock_wait_ms=0.0),
        LockSample("2026-01-01T00:00:04+00:00", blocked_backends=5, lock_waiters=3, max_lock_wait_ms=15.0),
    ]

    assert sampler.max_consecutive_blocked_over_threshold(2) == 2
