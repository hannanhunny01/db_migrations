from pgmigbench.scenarios.generator import generate_suite
from pgmigbench.strategies.baseline_a import build_baseline_a_plan
from pgmigbench.strategies.baseline_b import build_baseline_b_plan


def _pick_one_per_family():
    chosen = {}
    for s in generate_suite(seed=2026, suite_size=100):
        if s.family not in chosen:
            chosen[s.family] = s
    return list(chosen.values())


def test_baseline_a_plan_is_monolithic_contract() -> None:
    for scenario in _pick_one_per_family():
        plan = build_baseline_a_plan(scenario)
        assert plan.strategy == "baseline_a"
        assert plan.family == scenario.family
        assert set(plan.phases.keys()) == {"contract"}
        assert len(plan.phases["contract"]) >= 1
        for step in plan.phases["contract"]:
            assert step.sql.strip()


def test_baseline_b_plan_has_all_stages() -> None:
    for scenario in _pick_one_per_family():
        plan = build_baseline_b_plan(scenario)
        assert plan.strategy == "baseline_b"
        for phase in ("expand", "migrate", "cutover", "contract"):
            assert phase in plan.phases
            assert len(plan.phases[phase]) >= 1
            for step in plan.phases[phase]:
                assert step.sql.strip()


def test_hot_rename_index_variant_builds_new_column_index() -> None:
    scenario = next(
        s
        for s in generate_suite(seed=2026, suite_size=100)
        if s.family == "hot_column_rename" and bool(s.params.get("index_present"))
    )
    plan = build_baseline_b_plan(scenario)

    expand_sql = "\n".join(step.sql for step in plan.phases["expand"])
    assert "idx_child_hot_col_new" in expand_sql


def test_add_non_null_with_default_variant_sets_default_in_staged_plan() -> None:
    scenario = next(
        s
        for s in generate_suite(seed=2026, suite_size=100)
        if s.family == "add_non_null_column" and bool(s.params.get("with_default"))
    )
    plan = build_baseline_b_plan(scenario)

    expand_sql = "\n".join(step.sql for step in plan.phases["expand"])
    assert "SET DEFAULT" in expand_sql


def test_monolithic_type_narrow_targets_new_workload_interface() -> None:
    scenario = next(
        s for s in generate_suite(seed=2026, suite_size=100) if s.family == "type_narrowing"
    )
    plan = build_baseline_a_plan(scenario)

    contract_sql = "\n".join(step.sql for step in plan.phases["contract"])
    assert "ADD COLUMN legacy_col_int int" in contract_sql
    assert "SET legacy_col_int = legacy_col::int" in contract_sql
    assert "DROP COLUMN legacy_col" in contract_sql


def test_monolithic_drop_legacy_targets_new_workload_interface() -> None:
    scenario = next(
        s for s in generate_suite(seed=2026, suite_size=100) if s.family == "drop_legacy_column"
    )
    plan = build_baseline_a_plan(scenario)

    contract_sql = "\n".join(step.sql for step in plan.phases["contract"])
    assert "ADD COLUMN IF NOT EXISTS legacy_col_new bigint" in contract_sql
    assert "SET legacy_col_new = legacy_col" in contract_sql
    assert "DROP COLUMN legacy_col" in contract_sql
