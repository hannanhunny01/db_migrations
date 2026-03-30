from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.integration
@pytest.mark.skipif(os.getenv("PGMIGBENCH_RUN_INTEGRATION") != "1", reason="set PGMIGBENCH_RUN_INTEGRATION=1")
def test_end_to_end_smoke(tmp_path: Path) -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker not available")

    out_dir = tmp_path / "artifacts"
    env = os.environ.copy()
    env["PGMIGBENCH_WARMUP_WINDOW_S"] = "1"
    env["PGMIGBENCH_BASELINE_WINDOW_S"] = "1"
    env["PGMIGBENCH_MIGRATION_BUFFER_S"] = "1"
    env["PGMIGBENCH_CUTOVER_WINDOW_S"] = "1"
    env["PGMIGBENCH_POST_WINDOW_S"] = "1"
    env["PGMIGBENCH_REPETITIONS"] = "1"

    subprocess.run(
        [
            "python3",
            "bench.py",
            "run",
            "--suite-size",
            "1",
            "--seed",
            "2026",
            "--out",
            str(out_dir),
            "--pg-version",
            "16.3",
            "--repetitions",
            "1",
            "--shutdown",
        ],
        check=True,
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
    )

    summary = out_dir / "summary.json"
    results_tex = tmp_path / "results.tex"

    subprocess.run(
        [
            "python3",
            "bench.py",
            "report",
            "--input",
            str(summary),
            "--out",
            str(results_tex),
        ],
        check=True,
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
    )

    assert summary.exists()
    assert results_tex.exists()
