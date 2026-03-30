from collections import Counter

from pgmigbench.scenarios.generator import generate_suite


def test_suite_has_5_families_x_20() -> None:
    suite = generate_suite(seed=2026, suite_size=100)
    assert len(suite) == 100

    family_counts = Counter(s.family for s in suite)
    assert set(family_counts.values()) == {20}
    assert len(family_counts) == 5


def test_suite_has_workload_and_scale_coverage_per_family() -> None:
    suite = generate_suite(seed=2026, suite_size=100)
    by_family: dict[str, list] = {}
    for s in suite:
        by_family.setdefault(s.family, []).append(s)

    for family, scenarios in by_family.items():
        levels = {s.workload_level for s in scenarios}
        row_sizes = {s.rows for s in scenarios}
        assert levels == {"low", "high"}, family
        assert row_sizes == {100_000, 1_000_000}, family


def test_suite_ids_are_unique() -> None:
    suite = generate_suite(seed=2026, suite_size=100)
    ids = [s.id for s in suite]
    assert len(ids) == len(set(ids))
    assert ids[0].startswith("001_")
    assert ids[-1].startswith("100_")


def test_balanced_sample_has_one_per_family() -> None:
    suite = generate_suite(seed=2026, sample_per_family=1)
    assert len(suite) == 5

    family_counts = Counter(s.family for s in suite)
    assert len(family_counts) == 5
    assert set(family_counts.values()) == {1}
