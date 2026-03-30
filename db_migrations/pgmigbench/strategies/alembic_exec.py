from __future__ import annotations

import json
import re
import subprocess
import sys
import uuid
from pathlib import Path

from pgmigbench.strategies.mir import MigrationPlan


def _sqlalchemy_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+psycopg://"):
        return dsn
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    return dsn


def _extract_primary_error(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for line in lines:
        if re.search(r"psycopg\.errors\.[A-Za-z0-9_]+:", line):
            return line
    for line in lines:
        if re.search(r"sqlalchemy\.exc\.[A-Za-z0-9_]+:", line):
            return line

    skip_prefixes = (
        "Traceback (most recent call last):",
        "File ",
        "return ",
        "main(",
        "self.",
        "raise ",
        "The above exception was the direct cause of the following exception:",
        "^",
    )
    for line in lines:
        if line.startswith(skip_prefixes):
            continue
        if line.startswith("INFO  ["):
            continue
        if line.startswith("WARNING:"):
            continue
        return line
    if lines:
        return lines[0]
    return "alembic upgrade failed"


def execute_alembic_monolith(
    *,
    plan: MigrationPlan,
    dsn: str,
    evidence_dir: Path,
    migration_sql_log: Path,
) -> None:
    workdir = (evidence_dir / "alembic_work").resolve()
    versions_dir = workdir / "alembic" / "versions"
    versions_dir.mkdir(parents=True, exist_ok=True)

    alembic_ini = workdir / "alembic.ini"
    alembic_ini.write_text(
        """
[alembic]
script_location = alembic
sqlalchemy.url = {dsn}

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
""".strip().format(dsn=_sqlalchemy_dsn(dsn)),
        encoding="utf-8",
    )

    env_py = workdir / "alembic" / "env.py"
    env_py.write_text(
        """
from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config
target_metadata = None


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
""".strip()
        + "\n",
        encoding="utf-8",
    )

    revision = uuid.uuid4().hex[:12]
    revision_file = versions_dir / f"{revision}_monolith.py"

    steps = plan.phases.get("contract", [])
    if not steps:
        raise RuntimeError("baseline_a plan must have at least one contract step")

    with migration_sql_log.open("a", encoding="utf-8") as f:
        for step in steps:
            f.write(f"-- {step.name}\n{step.sql};\n\n")

    step_sql_lines = "\n".join(f"    op.execute({json.dumps(step.sql)})" for step in steps)
    revision_file.write_text(
        f"""\
\"\"\"monolith\"\"\"

from __future__ import annotations

from alembic import op

revision = {revision!r}
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
{step_sql_lines}


def downgrade() -> None:
    pass
""",
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "alembic.ini", "upgrade", "head"],
        cwd=str(workdir),
        text=True,
        capture_output=True,
        check=False,
    )

    (evidence_dir / "alembic.stdout.log").write_text(result.stdout or "", encoding="utf-8")
    (evidence_dir / "alembic.stderr.log").write_text(result.stderr or "", encoding="utf-8")

    if result.returncode != 0:
        msg = _extract_primary_error(result.stderr or result.stdout or "")
        raise RuntimeError(msg)
