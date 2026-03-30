from __future__ import annotations

import argparse
from pathlib import Path

from pgmigbench.config import Settings
from pgmigbench.report.latex import load_summary, write_results_tex


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pgmigbench")
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="run benchmark suite")
    run_p.add_argument("--suite-size", type=int, default=100)
    run_p.add_argument("--seed", type=int, default=2026)
    run_p.add_argument("--out", type=Path, default=Path("artifacts"))
    run_p.add_argument("--pg-version", type=str, default="16.3")
    run_p.add_argument("--repetitions", type=int, default=None)
    run_p.add_argument(
        "--sample-per-family",
        type=int,
        default=None,
        help="sample this many scenarios from each family; overrides suite-size balancing",
    )
    run_p.add_argument("--resume", action="store_true")
    run_p.add_argument("--shutdown", action="store_true", help="stop docker compose after run")

    report_p = sub.add_parser("report", help="generate LaTeX results macros")
    report_p.add_argument("--input", type=Path, required=True)
    report_p.add_argument("--out", type=Path, required=True)

    return parser


def _cmd_run(args: argparse.Namespace) -> int:
    from pgmigbench.runner import run_suite

    settings = Settings.from_env()
    if args.repetitions is not None:
        settings = type(settings)(**{**settings.__dict__, "repetitions_per_case": args.repetitions})
    result = run_suite(
        suite_size=args.suite_size,
        seed=args.seed,
        out_dir=args.out,
        pg_version=args.pg_version,
        sample_per_family=args.sample_per_family,
        resume=bool(args.resume),
        project_root=Path.cwd(),
        settings=settings,
        shutdown=bool(args.shutdown),
    )
    print(f"run_id={result.run_id}")
    print(f"benchmark csv: {result.csv_path}")
    print(f"benchmark summary: {result.summary_path}")
    return 0


def _cmd_report(args: argparse.Namespace) -> int:
    summary = load_summary(args.input)
    write_results_tex(summary, args.out)
    print(f"wrote {args.out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        return _cmd_run(args)
    if args.command == "report":
        return _cmd_report(args)

    parser.error(f"unknown command: {args.command}")
    return 2
