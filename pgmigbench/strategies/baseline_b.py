from __future__ import annotations

from pgmigbench.scenarios.base import Scenario
from pgmigbench.scenarios.families import ADD_FK, ADD_NOT_NULL, DROP_LEGACY, HOT_RENAME, TYPE_NARROW
from pgmigbench.strategies.mir import MigrationPlan, MigrationStep


def _hot_rename_plan(scenario: Scenario) -> MigrationPlan:
    batch_size = int(scenario.params.get("batch_size", 5000))
    sleep_ms = int(scenario.params.get("batch_sleep_ms", 10))
    expand_steps = [
        MigrationStep(
            name="add_new_column",
            phase="expand",
            sql="ALTER TABLE bench.child ADD COLUMN hot_col_new bigint",
        ),
        MigrationStep(
            name="create_dualwrite_function",
            phase="expand",
            sql="""
CREATE OR REPLACE FUNCTION bench.dualwrite_hot_col() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.hot_col_new := NEW.hot_col;
  RETURN NEW;
END;
$$
""".strip(),
        ),
        MigrationStep(
            name="drop_existing_trigger",
            phase="expand",
            sql="DROP TRIGGER IF EXISTS trg_dualwrite_hot_col ON bench.child",
        ),
        MigrationStep(
            name="install_trigger",
            phase="expand",
            sql="""
CREATE TRIGGER trg_dualwrite_hot_col
BEFORE INSERT OR UPDATE ON bench.child
FOR EACH ROW
EXECUTE FUNCTION bench.dualwrite_hot_col()
""".strip(),
        ),
    ]
    if bool(scenario.params.get("index_present", False)):
        expand_steps.append(
            MigrationStep(
                name="create_new_column_index",
                phase="expand",
                sql=(
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_child_hot_col_new "
                    "ON bench.child(hot_col_new)"
                ),
                autocommit=True,
            )
        )
    return MigrationPlan(
        strategy="baseline_b",
        family=scenario.family,
        phases={
            "expand": expand_steps,
            "migrate": [
                MigrationStep(
                    name="backfill_hot_col_new",
                    phase="migrate",
                    sql=(
                        "UPDATE bench.child "
                        "SET hot_col_new = hot_col "
                        "WHERE hot_col_new IS NULL AND id BETWEEN {lo} AND {hi}"
                    ),
                    batch_size=batch_size,
                    sleep_ms=sleep_ms,
                )
            ],
            "cutover": [MigrationStep(name="cutover_marker", phase="cutover", sql="SELECT 1")],
            "contract": [
                MigrationStep(
                    name="remove_dualwrite_trigger",
                    phase="contract",
                    sql="DROP TRIGGER IF EXISTS trg_dualwrite_hot_col ON bench.child",
                ),
                MigrationStep(
                    name="remove_dualwrite_function",
                    phase="contract",
                    sql="DROP FUNCTION IF EXISTS bench.dualwrite_hot_col",
                ),
                MigrationStep(
                    name="drop_old_column",
                    phase="contract",
                    sql="ALTER TABLE bench.child DROP COLUMN hot_col",
                ),
            ],
        },
    )


def _add_not_null_plan(scenario: Scenario) -> MigrationPlan:
    batch_size = int(scenario.params.get("batch_size", 5000))
    sleep_ms = int(scenario.params.get("batch_sleep_ms", 10))
    fill_value = str(scenario.params.get("fill_value", "basic"))
    expand_steps = [
        MigrationStep(
            name="add_nullable_column",
            phase="expand",
            sql="ALTER TABLE bench.child ADD COLUMN customer_tier text",
        )
    ]
    if bool(scenario.params.get("with_default", True)):
        expand_steps.append(
            MigrationStep(
                name="set_column_default",
                phase="expand",
                sql=(
                    "ALTER TABLE bench.child "
                    f"ALTER COLUMN customer_tier SET DEFAULT '{fill_value}'"
                ),
            )
        )
    return MigrationPlan(
        strategy="baseline_b",
        family=scenario.family,
        phases={
            "expand": expand_steps,
            "migrate": [
                MigrationStep(
                    name="backfill_customer_tier",
                    phase="migrate",
                    sql=(
                        "UPDATE bench.child "
                        f"SET customer_tier = '{fill_value}' "
                        "WHERE customer_tier IS NULL AND id BETWEEN {lo} AND {hi}"
                    ),
                    batch_size=batch_size,
                    sleep_ms=sleep_ms,
                )
            ],
            "cutover": [MigrationStep(name="cutover_marker", phase="cutover", sql="SELECT 1")],
            "contract": [
                MigrationStep(
                    name="check_not_null_not_valid",
                    phase="contract",
                    sql=(
                        "ALTER TABLE bench.child "
                        "ADD CONSTRAINT child_customer_tier_nn "
                        "CHECK (customer_tier IS NOT NULL) NOT VALID"
                    ),
                ),
                MigrationStep(
                    name="validate_check",
                    phase="contract",
                    sql="ALTER TABLE bench.child VALIDATE CONSTRAINT child_customer_tier_nn",
                ),
                MigrationStep(
                    name="set_not_null",
                    phase="contract",
                    sql="ALTER TABLE bench.child ALTER COLUMN customer_tier SET NOT NULL",
                ),
            ],
        },
    )


def _type_narrow_plan(scenario: Scenario) -> MigrationPlan:
    batch_size = int(scenario.params.get("batch_size", 5000))
    sleep_ms = int(scenario.params.get("batch_sleep_ms", 10))
    return MigrationPlan(
        strategy="baseline_b",
        family=scenario.family,
        phases={
            "expand": [
                MigrationStep(
                    name="add_shadow_int_column",
                    phase="expand",
                    sql="ALTER TABLE bench.child ADD COLUMN legacy_col_int int",
                ),
                MigrationStep(
                    name="create_dualwrite_function",
                    phase="expand",
                    sql="""
CREATE OR REPLACE FUNCTION bench.dualwrite_legacy_col_int() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.legacy_col BETWEEN -2147483648 AND 2147483647 THEN
    NEW.legacy_col_int := NEW.legacy_col::int;
  ELSE
    NEW.legacy_col_int := NULL;
  END IF;
  RETURN NEW;
END;
$$
""".strip(),
                ),
                MigrationStep(
                    name="drop_existing_trigger",
                    phase="expand",
                    sql="DROP TRIGGER IF EXISTS trg_dualwrite_legacy_col_int ON bench.child",
                ),
                MigrationStep(
                    name="install_trigger",
                    phase="expand",
                    sql="""
CREATE TRIGGER trg_dualwrite_legacy_col_int
BEFORE INSERT OR UPDATE ON bench.child
FOR EACH ROW
EXECUTE FUNCTION bench.dualwrite_legacy_col_int()
""".strip(),
                ),
                MigrationStep(
                    name="create_quarantine_table",
                    phase="expand",
                    sql="""
CREATE TABLE IF NOT EXISTS bench.type_narrow_quarantine (
  id bigint primary key,
  legacy_col bigint not null,
  captured_at timestamptz not null default now()
)
""".strip(),
                ),
            ],
            "migrate": [
                MigrationStep(
                    name="backfill_shadow_column",
                    phase="migrate",
                    sql=(
                        "UPDATE bench.child "
                        "SET legacy_col_int = CASE "
                        "WHEN legacy_col BETWEEN -2147483648 AND 2147483647 THEN legacy_col::int "
                        "ELSE NULL END "
                        "WHERE id BETWEEN {lo} AND {hi}"
                    ),
                    batch_size=batch_size,
                    sleep_ms=sleep_ms,
                ),
                MigrationStep(
                    name="quarantine_invalid_rows",
                    phase="migrate",
                    sql="""
INSERT INTO bench.type_narrow_quarantine(id, legacy_col)
SELECT id, legacy_col
FROM bench.child
WHERE legacy_col NOT BETWEEN -2147483648 AND 2147483647
ON CONFLICT (id) DO NOTHING
""".strip(),
                ),
            ],
            "cutover": [MigrationStep(name="cutover_marker", phase="cutover", sql="SELECT 1")],
            "contract": [
                MigrationStep(
                    name="drop_dualwrite_trigger",
                    phase="contract",
                    sql="DROP TRIGGER IF EXISTS trg_dualwrite_legacy_col_int ON bench.child",
                ),
                MigrationStep(
                    name="drop_dualwrite_function",
                    phase="contract",
                    sql="DROP FUNCTION IF EXISTS bench.dualwrite_legacy_col_int",
                ),
                MigrationStep(
                    name="drop_old_legacy_column",
                    phase="contract",
                    sql="ALTER TABLE bench.child DROP COLUMN legacy_col",
                ),
            ],
        },
    )


def _drop_legacy_plan(scenario: Scenario) -> MigrationPlan:
    batch_size = int(scenario.params.get("batch_size", 5000))
    sleep_ms = int(scenario.params.get("batch_sleep_ms", 10))
    return MigrationPlan(
        strategy="baseline_b",
        family=scenario.family,
        phases={
            "expand": [
                MigrationStep(
                    name="add_replacement_column",
                    phase="expand",
                    sql="ALTER TABLE bench.child ADD COLUMN IF NOT EXISTS legacy_col_new bigint",
                )
            ],
            "migrate": [
                MigrationStep(
                    name="backfill_replacement",
                    phase="migrate",
                    sql=(
                        "UPDATE bench.child "
                        "SET legacy_col_new = legacy_col "
                        "WHERE legacy_col_new IS NULL AND id BETWEEN {lo} AND {hi}"
                    ),
                    batch_size=batch_size,
                    sleep_ms=sleep_ms,
                )
            ],
            "cutover": [MigrationStep(name="cutover_marker", phase="cutover", sql="SELECT 1")],
            "contract": [
                MigrationStep(
                    name="drop_legacy_column",
                    phase="contract",
                    sql="ALTER TABLE bench.child DROP COLUMN legacy_col",
                )
            ],
        },
    )


def _add_fk_plan(scenario: Scenario) -> MigrationPlan:
    cleanup_mode = str(scenario.params.get("cleanup_mode", "set_null"))
    cleanup_sql = (
        "UPDATE bench.child c SET parent_id = NULL "
        "WHERE parent_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM bench.parent p WHERE p.id = c.parent_id)"
    )
    if cleanup_mode == "delete":
        cleanup_sql = (
            "DELETE FROM bench.child c "
            "WHERE parent_id IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM bench.parent p WHERE p.id = c.parent_id)"
        )

    return MigrationPlan(
        strategy="baseline_b",
        family=scenario.family,
        phases={
            "expand": [
                MigrationStep(
                    name="index_parent_id_concurrently",
                    phase="expand",
                    sql=(
                        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_child_parent_id "
                        "ON bench.child(parent_id)"
                    ),
                    autocommit=True,
                )
            ],
            "migrate": [MigrationStep(name="clean_invalid_rows", phase="migrate", sql=cleanup_sql)],
            "cutover": [
                MigrationStep(
                    name="add_fk_not_valid",
                    phase="cutover",
                    sql=(
                        "ALTER TABLE bench.child "
                        "ADD CONSTRAINT fk_child_parent "
                        "FOREIGN KEY (parent_id) REFERENCES bench.parent(id) NOT VALID"
                    ),
                )
            ],
            "contract": [
                MigrationStep(
                    name="validate_fk",
                    phase="contract",
                    sql="ALTER TABLE bench.child VALIDATE CONSTRAINT fk_child_parent",
                )
            ],
        },
    )


def build_baseline_b_plan(scenario: Scenario) -> MigrationPlan:
    if scenario.family == HOT_RENAME:
        return _hot_rename_plan(scenario)
    if scenario.family == ADD_NOT_NULL:
        return _add_not_null_plan(scenario)
    if scenario.family == TYPE_NARROW:
        return _type_narrow_plan(scenario)
    if scenario.family == DROP_LEGACY:
        return _drop_legacy_plan(scenario)
    if scenario.family == ADD_FK:
        return _add_fk_plan(scenario)
    raise ValueError(f"unsupported family: {scenario.family}")
