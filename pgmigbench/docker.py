from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from pgmigbench.config import Settings


class DockerError(RuntimeError):
    pass


def _run(
    args: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    capture: bool = True,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    result = subprocess.run(
        args,
        cwd=str(cwd),
        env=merged_env,
        text=True,
        capture_output=capture,
        check=False,
    )
    if check and result.returncode != 0:
        raise DockerError(
            f"command failed: {' '.join(args)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def compose_up(settings: Settings, project_root: Path, pg_version: str) -> None:
    env = {"PGMIGBENCH_PG_IMAGE": f"postgres:{pg_version}", "PGMIGBENCH_DB_PORT": str(settings.db_port)}
    _run(
        [
            "docker",
            "compose",
            "-f",
            str(settings.compose_file),
            "up",
            "-d",
            settings.service_name,
        ],
        cwd=project_root,
        env=env,
    )


def compose_down(settings: Settings, project_root: Path, remove_volumes: bool = False) -> None:
    args = ["docker", "compose", "-f", str(settings.compose_file), "down"]
    if remove_volumes:
        args.append("-v")
    _run(args, cwd=project_root, check=False)


def wait_until_ready(
    settings: Settings,
    project_root: Path,
    *,
    timeout_s: int = 120,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = _run(
            [
                "docker",
                "exec",
                settings.container_name,
                "pg_isready",
                "-h",
                "localhost",
                "-p",
                "5432",
                "-U",
                settings.db_user,
                "-d",
                settings.db_name,
            ],
            cwd=project_root,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(2)
    raise DockerError("postgres did not become ready within timeout")


def exec_in_container(
    settings: Settings,
    project_root: Path,
    cmd: str,
    *,
    capture: bool = True,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return _run(
        ["docker", "exec", settings.container_name, "bash", "-lc", cmd],
        cwd=project_root,
        capture=capture,
        check=check,
    )


def copy_to_container(settings: Settings, project_root: Path, src: Path, dst: str) -> None:
    _run(
        ["docker", "cp", str(src), f"{settings.container_name}:{dst}"],
        cwd=project_root,
    )
