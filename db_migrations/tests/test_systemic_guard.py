from pgmigbench.runner import _detect_systemic_break


def _failure_row(*, family: str, strategy: str, error: str, success: bool = False) -> dict[str, object]:
    return {
        "family": family,
        "strategy": strategy,
        "success": success,
        "error": error,
    }


def test_detects_zero_success_repeated_runtime_error() -> None:
    rows: list[dict[str, object]] = []
    families = [
        "hot_column_rename",
        "add_non_null_column",
        "type_narrowing",
        "drop_legacy_column",
    ]
    for family in families:
        rows.append(
            _failure_row(
                family=family,
                strategy="baseline_a",
                error="Can't locate revision identified by 'abc123'",
            )
        )
        rows.append(
            _failure_row(
                family=family,
                strategy="baseline_a",
                error="Can't locate revision identified by 'abc123'\nmore context",
            )
        )

    message = _detect_systemic_break(rows)
    assert message is not None
    assert "baseline_a" in message
    assert "Can't locate revision identified by 'abc123'" in message


def test_does_not_trigger_if_no_dominant_error_pattern() -> None:
    rows: list[dict[str, object]] = []
    families = [
        "hot_column_rename",
        "add_non_null_column",
        "type_narrowing",
        "drop_legacy_column",
    ]
    errors = [
        "can't cast bigint to int",
        "lock timeout",
        "duplicate key value violates unique constraint",
        "foreign key violation",
    ]
    for family, error in zip(families, errors):
        rows.append(
            _failure_row(
                family=family,
                strategy="baseline_a",
                error=error,
            )
        )
        rows.append(
            _failure_row(
                family=family,
                strategy="baseline_a",
                error=error,
            )
        )
    rows.append(
        _failure_row(
            family="add_foreign_key",
            strategy="baseline_a",
            error="",
            success=True,
        )
    )

    assert _detect_systemic_break(rows) is None


def test_does_not_trigger_for_single_family_repetition() -> None:
    rows = [
        _failure_row(
            family="hot_column_rename",
            strategy="baseline_b",
            error="the connection is closed",
        )
        for _ in range(10)
    ]
    assert _detect_systemic_break(rows) is None


def test_detects_dominant_repeated_error_even_with_some_successes() -> None:
    rows: list[dict[str, object]] = []
    families = [
        "hot_column_rename",
        "add_non_null_column",
        "type_narrowing",
        "drop_legacy_column",
        "add_foreign_key",
    ]

    for family in families:
        rows.append(
            _failure_row(
                family=family,
                strategy="baseline_b",
                error="psycopg.ProgrammingError: only '%s', '%b', '%t' are allowed as placeholders",
            )
        )
        rows.append(
            _failure_row(
                family=family,
                strategy="baseline_b",
                error="psycopg.ProgrammingError: only '%s', '%b', '%t' are allowed as placeholders",
            )
        )

    rows.extend(
        [
            _failure_row(
                family="hot_column_rename",
                strategy="baseline_b",
                error="",
                success=True,
            ),
            _failure_row(
                family="add_non_null_column",
                strategy="baseline_b",
                error="",
                success=True,
            ),
            _failure_row(
                family="type_narrowing",
                strategy="baseline_b",
                error="",
                success=True,
            ),
            _failure_row(
                family="drop_legacy_column",
                strategy="baseline_b",
                error="",
                success=True,
            ),
        ]
    )

    message = _detect_systemic_break(rows)
    assert message is not None
    assert "baseline_b" in message
    assert "accounts for" in message
