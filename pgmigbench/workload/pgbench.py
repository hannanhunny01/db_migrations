from __future__ import annotations

import math
import re
import subprocess
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from statistics import mean

from pgmigbench.config import Settings
from pgmigbench.docker import copy_to_container, exec_in_container


_LATENCY_RE = re.compile(r"latency average\s*=\s*([0-9.]+)\s*ms", re.IGNORECASE)
_TPS_RE = re.compile(r"tps\s*=\s*([0-9.]+)", re.IGNORECASE)


@dataclass
class PgbenchResult:
    ok: bool
    latency_ms: float | None
    p95_latency_ms: float | None
    tps: float | None
    stdout: str
    stderr: str
    tx_log_text: str = ""


@dataclass
class AsyncPgbenchHandle:
    proc: subprocess.Popen[str]
    stdout_path: Path
    stderr_path: Path
    container_tmp_dir: str
    clients: int
    jobs: int


@dataclass
class LoopingPgbenchHandle:
    thread: threading.Thread
    stop_event: threading.Event
    results: list[PgbenchResult]
    errors: list[str]


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    rank = (pct / 100.0) * (len(values) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return values[low]
    weight = rank - low
    return values[low] * (1 - weight) + values[high] * weight


def _parse_metrics(stdout: str, log_text: str) -> tuple[float | None, float | None, float | None]:
    latency_match = _LATENCY_RE.search(stdout)
    tps_match = _TPS_RE.search(stdout)

    latency_ms = float(latency_match.group(1)) if latency_match else None
    tps = float(tps_match.group(1)) if tps_match else None

    p95_latency_ms: float | None = None
    latencies_ms: list[float] = []
    for line in log_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            latency_us = float(parts[2])
        except ValueError:
            continue
        latencies_ms.append(latency_us / 1000.0)
    if latencies_ms:
        latencies_ms.sort()
        p95_latency_ms = _percentile(latencies_ms, 95.0)
        if latency_ms is None:
            latency_ms = mean(latencies_ms)

    return latency_ms, p95_latency_ms, tps


def render_script(template_text: str, *, max_id: int) -> str:
    return template_text.replace("__MAX_ID__", str(max_id))


def _prepare_script(
    *,
    settings: Settings,
    project_root: Path,
    evidence_dir: Path,
    script_text: str,
    tag: str,
) -> str:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    local_script = evidence_dir / f"pgbench_{tag}.sql"
    local_script.write_text(script_text, encoding="utf-8")

    container_tmp_dir = f"/tmp/pgmigbench_{tag}_{uuid.uuid4().hex[:8]}"
    exec_in_container(settings, project_root, f"mkdir -p {container_tmp_dir}")
    copy_to_container(settings, project_root, local_script, f"{container_tmp_dir}/script.sql")
    return container_tmp_dir


def _collect_pgbench_logs(
    *,
    settings: Settings,
    project_root: Path,
    container_tmp_dir: str,
) -> str:
    result = exec_in_container(
        settings,
        project_root,
        f"cd {container_tmp_dir} && cat pgbench_log.* 2>/dev/null || true",
        check=False,
    )
    return result.stdout or ""


def _cleanup_tmp(
    *, settings: Settings, project_root: Path, container_tmp_dir: str
) -> None:
    exec_in_container(settings, project_root, f"rm -rf {container_tmp_dir}", check=False)


def start_pgbench(
    *,
    settings: Settings,
    project_root: Path,
    evidence_dir: Path,
    script_text: str,
    duration_s: int,
    workload_level: str,
    tag: str,
    random_seed: int | None = None,
) -> AsyncPgbenchHandle:
    clients, jobs = settings.pgbench_profile(workload_level)
    container_tmp_dir = _prepare_script(
        settings=settings,
        project_root=project_root,
        evidence_dir=evidence_dir,
        script_text=script_text,
        tag=tag,
    )

    stdout_path = evidence_dir / f"pgbench_{tag}.stdout.log"
    stderr_path = evidence_dir / f"pgbench_{tag}.stderr.log"

    seed_arg = f" --random-seed={random_seed}" if random_seed is not None else ""
    cmd = [
        "docker",
        "exec",
        settings.container_name,
        "bash",
        "-lc",
        (
            f"export PGPASSWORD='{settings.db_password}'; "
            f"cd {container_tmp_dir}; "
            f"pgbench -h localhost -U {settings.db_user} -d {settings.db_name} "
            f"-n -T {duration_s} -c {clients} -j {jobs} -f script.sql -l{seed_arg}"
        ),
    ]

    stdout_f = stdout_path.open("w", encoding="utf-8")
    stderr_f = stderr_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(cmd, cwd=str(project_root), text=True, stdout=stdout_f, stderr=stderr_f)
    stdout_f.close()
    stderr_f.close()

    return AsyncPgbenchHandle(
        proc=proc,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        container_tmp_dir=container_tmp_dir,
        clients=clients,
        jobs=jobs,
    )


def finish_pgbench(
    *,
    settings: Settings,
    project_root: Path,
    handle: AsyncPgbenchHandle,
    timeout_s: int = 300,
) -> PgbenchResult:
    try:
        handle.proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        handle.proc.kill()
        handle.proc.wait(timeout=5)

    stdout = handle.stdout_path.read_text(encoding="utf-8") if handle.stdout_path.exists() else ""
    stderr = handle.stderr_path.read_text(encoding="utf-8") if handle.stderr_path.exists() else ""
    log_text = _collect_pgbench_logs(
        settings=settings,
        project_root=project_root,
        container_tmp_dir=handle.container_tmp_dir,
    )
    (handle.stdout_path.parent / (handle.stdout_path.stem + ".tx.log")).write_text(
        log_text,
        encoding="utf-8",
    )
    _cleanup_tmp(
        settings=settings,
        project_root=project_root,
        container_tmp_dir=handle.container_tmp_dir,
    )

    latency_ms, p95_latency_ms, tps = _parse_metrics(stdout, log_text)
    return PgbenchResult(
        ok=handle.proc.returncode == 0,
        latency_ms=latency_ms,
        p95_latency_ms=p95_latency_ms,
        tps=tps,
        stdout=stdout,
        stderr=stderr,
        tx_log_text=log_text,
    )


def run_pgbench(
    *,
    settings: Settings,
    project_root: Path,
    evidence_dir: Path,
    script_text: str,
    duration_s: int,
    workload_level: str,
    tag: str,
    random_seed: int | None = None,
) -> PgbenchResult:
    handle = start_pgbench(
        settings=settings,
        project_root=project_root,
        evidence_dir=evidence_dir,
        script_text=script_text,
        duration_s=duration_s,
        workload_level=workload_level,
        tag=tag,
        random_seed=random_seed,
    )
    return finish_pgbench(settings=settings, project_root=project_root, handle=handle)


def _combine_pgbench_results(results: list[PgbenchResult], errors: list[str]) -> PgbenchResult:
    stdout = "\n".join(result.stdout for result in results if result.stdout)
    stderr_parts = [result.stderr for result in results if result.stderr]
    stderr_parts.extend(errors)
    stderr = "\n".join(stderr_parts)
    tx_log_text = "\n".join(result.tx_log_text for result in results if result.tx_log_text)
    latency_ms, p95_latency_ms, _ = _parse_metrics("", tx_log_text)
    ok = not errors and all(result.ok for result in results)

    return PgbenchResult(
        ok=ok,
        latency_ms=latency_ms,
        p95_latency_ms=p95_latency_ms,
        tps=None,
        stdout=stdout,
        stderr=stderr,
        tx_log_text=tx_log_text,
    )


def start_looping_pgbench(
    *,
    settings: Settings,
    project_root: Path,
    evidence_dir: Path,
    script_text: str,
    workload_level: str,
    tag: str,
    random_seed: int | None = None,
    chunk_duration_s: int = 1,
) -> LoopingPgbenchHandle:
    stop_event = threading.Event()
    results: list[PgbenchResult] = []
    errors: list[str] = []

    def _seed_for_chunk(chunk_index: int) -> int | None:
        if random_seed is None:
            return None
        seed = (random_seed + chunk_index) % 2147483647
        return seed if seed > 0 else 1

    def _worker() -> None:
        chunk_index = 0
        while not stop_event.is_set():
            try:
                result = run_pgbench(
                    settings=settings,
                    project_root=project_root,
                    evidence_dir=evidence_dir,
                    script_text=script_text,
                    duration_s=chunk_duration_s,
                    workload_level=workload_level,
                    tag=f"{tag}_chunk_{chunk_index:04d}",
                    random_seed=_seed_for_chunk(chunk_index),
                )
                results.append(result)
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
                return
            chunk_index += 1

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    return LoopingPgbenchHandle(
        thread=thread,
        stop_event=stop_event,
        results=results,
        errors=errors,
    )


def stop_looping_pgbench(handle: LoopingPgbenchHandle) -> PgbenchResult:
    handle.stop_event.set()
    handle.thread.join(timeout=60)
    return _combine_pgbench_results(handle.results, handle.errors)
