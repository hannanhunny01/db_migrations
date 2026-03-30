from __future__ import annotations

from pgmigbench.scenarios.base import Scenario
from pgmigbench.scenarios.families import ADD_FK, ADD_NOT_NULL, DROP_LEGACY, HOT_RENAME, TYPE_NARROW
from pgmigbench.strategies.mir import MigrationPlan, MigrationStep


def build_baseline_a_plan(scenario: Scenario) -> MigrationPlan:
    steps: list[MigrationStep]
    if scenario.family == HOT_RENAME:
        steps = [
            MigrationStep(
                name="rename_hot_column",
                phase="contract",
                sql="ALTER TABLE bench.child RENAME COLUMN hot_col TO hot_col_new",
            )
        ]
    elif scenario.family == ADD_NOT_NULL:
        fill_value = str(scenario.params.get("fill_value", "basic"))
        if bool(scenario.params.get("with_default", True)):
            sql = (
                "ALTER TABLE bench.child "
                f"ADD COLUMN customer_tier text NOT NULL DEFAULT '{fill_value}'"
            )
        else:
            sql = "ALTER TABLE bench.child ADD COLUMN customer_tier text NOT NULL"
        steps = [MigrationStep(name="add_not_null_column", phase="contract", sql=sql)]
    elif scenario.family == TYPE_NARROW:
        steps = [
            MigrationStep(
                name="add_shadow_int_column",
                phase="contract",
                sql="ALTER TABLE bench.child ADD COLUMN legacy_col_int int",
            ),
            MigrationStep(
                name="backfill_shadow_int_column",
                phase="contract",
                sql="UPDATE bench.child SET legacy_col_int = legacy_col::int",
            ),
            MigrationStep(
                name="drop_old_legacy_column",
                phase="contract",
                sql="ALTER TABLE bench.child DROP COLUMN legacy_col",
            )
        ]
    elif scenario.family == DROP_LEGACY:
        steps = [
            MigrationStep(
                name="add_replacement_column",
                phase="contract",
                sql="ALTER TABLE bench.child ADD COLUMN IF NOT EXISTS legacy_col_new bigint",
            ),
            MigrationStep(
                name="backfill_replacement_column",
                phase="contract",
                sql=(
                    "UPDATE bench.child "
                    "SET legacy_col_new = legacy_col "
                    "WHERE legacy_col_new IS NULL"
                ),
            ),
            MigrationStep(
                name="drop_legacy_column",
                phase="contract",
                sql="ALTER TABLE bench.child DROP COLUMN legacy_col",
            )
        ]
    elif scenario.family == ADD_FK:
        steps = [
            MigrationStep(
                name="add_fk_direct",
                phase="contract",
                sql=(
                    "ALTER TABLE bench.child "
                    "ADD CONSTRAINT fk_child_parent "
                    "FOREIGN KEY (parent_id) REFERENCES bench.parent(id)"
                ),
            )
        ]
    else:
        raise ValueError(f"unsupported family: {scenario.family}")

    return MigrationPlan(strategy="baseline_a", family=scenario.family, phases={"contract": steps})
