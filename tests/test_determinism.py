from pgmigbench.scenarios.generator import generate_suite


def test_suite_is_deterministic_for_same_seed() -> None:
    a = generate_suite(seed=2026, suite_size=100)
    b = generate_suite(seed=2026, suite_size=100)
    assert [x.signature() for x in a] == [x.signature() for x in b]


def test_suite_changes_order_for_different_seed() -> None:
    a = generate_suite(seed=2026, suite_size=100)
    b = generate_suite(seed=2027, suite_size=100)
    assert [x.id for x in a] != [x.id for x in b]


def test_balanced_sample_is_deterministic_for_same_seed() -> None:
    a = generate_suite(seed=2026, sample_per_family=1)
    b = generate_suite(seed=2026, sample_per_family=1)
    assert [x.signature() for x in a] == [x.signature() for x in b]
