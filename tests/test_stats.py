from pgmigbench.report.stats import wilson_interval_95


def test_wilson_ci_for_39_of_100() -> None:
    lo, hi = wilson_interval_95(39, 100)
    assert abs(lo - 0.300) < 0.01
    assert abs(hi - 0.488) < 0.01


def test_wilson_ci_for_87_of_100() -> None:
    lo, hi = wilson_interval_95(87, 100)
    assert abs(lo - 0.790) < 0.01
    assert abs(hi - 0.922) < 0.01
