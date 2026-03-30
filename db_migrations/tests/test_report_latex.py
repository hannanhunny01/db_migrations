from pgmigbench.report.latex import generate_results_tex


def test_results_tex_contains_required_macros() -> None:
    summary = {
        "pg_version": "16.3",
        "suite_size": 100,
        "repetitions_per_scenario_strategy": 5,
        "families": {
            "add_non_null_column": {
                "baseline_a": {"runs": 100, "scenario_pairs": 20, "completed_pairs": 20, "success": 0, "failure": 20},
                "baseline_b": {"runs": 100, "scenario_pairs": 20, "completed_pairs": 20, "success": 16, "failure": 4},
            }
        },
        "strategies": {
            "baseline_a": {
                "safe_success": 39,
                "scenario_pairs_total": 100,
                "scenario_pairs_completed": 100,
                "runs": 500,
                "safe_success_pct": "39.0%",
                "safe_success_ci_95": "[30.0%, 48.8%]",
                "lock_violations": 61,
                "unsafe_destructive_actions": 40,
                "compat_breaks": 0,
                "ops": {
                    "peak_lock_wait_ms_p95": 120.2,
                    "latency_delta_pct_p95": 15.4,
                    "duration_s_median": 7.8,
                },
            },
            "baseline_b": {
                "safe_success": 87,
                "scenario_pairs_total": 100,
                "scenario_pairs_completed": 100,
                "runs": 500,
                "safe_success_pct": "87.0%",
                "safe_success_ci_95": "[79.0%, 92.2%]",
                "lock_violations": 4,
                "unsafe_destructive_actions": 0,
                "compat_breaks": 0,
                "ops": {
                    "peak_lock_wait_ms_p95": 12.8,
                    "latency_delta_pct_p95": 6.2,
                    "duration_s_median": 8.5,
                },
            },
        },
    }

    tex = generate_results_tex(summary)

    for key in [
        r"\providecommand{\PGVersion}{PostgreSQL~16.3}",
        r"\providecommand{\SuiteSize}{100}",
        r"\providecommand{\BenchmarkRepetitions}{5}",
        r"\providecommand{\A_Safe}{39/100}",
        r"\providecommand{\B_Safe}{87/100}",
        r"\providecommand{\A_LockViol}{61}",
        r"\providecommand{\B_LockViol}{4}",
        r"\providecommand{\A_UnsafeDestructive}{40}",
        r"\providecommand{\B_UnsafeDestructive}{0}",
        r"\providecommand{\A_AddNNSuccess}{0/20}",
        r"\providecommand{\A_AddNNFail}{20}",
        r"\providecommand{\B_AddNNSuccess}{16/20}",
        r"\providecommand{\B_AddNNFail}{4}",
    ]:
        assert key in tex


def test_results_tex_keeps_family_denominator_at_20() -> None:
    summary = {
        "pg_version": "16.3",
        "suite_size": 10,
        "repetitions_per_scenario_strategy": 3,
        "families": {
            "hot_column_rename": {
                "baseline_a": {"runs": 9, "scenario_pairs": 3, "completed_pairs": 3, "success": 0, "failure": 3},
                "baseline_b": {"runs": 9, "scenario_pairs": 3, "completed_pairs": 3, "success": 3, "failure": 0},
            }
        },
        "strategies": {
            "baseline_a": {
                "safe_success": 3,
                "safe_success_pct": "30.0%",
                "safe_success_ci_95": "[10.0%, 60.0%]",
                "lock_violations": 1,
                "unsafe_destructive_actions": 2,
                "compat_breaks": 5,
                "ops": {},
            },
            "baseline_b": {
                "safe_success": 10,
                "safe_success_pct": "100.0%",
                "safe_success_ci_95": "[72.2%, 100.0%]",
                "lock_violations": 0,
                "unsafe_destructive_actions": 0,
                "compat_breaks": 0,
                "ops": {},
            },
        },
    }

    tex = generate_results_tex(summary)
    assert r"\providecommand{\A_RenameSuccess}{0/3}" in tex
    assert r"\providecommand{\B_RenameSuccess}{3/3}" in tex
