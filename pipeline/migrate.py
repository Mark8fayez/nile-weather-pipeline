"""
Apply database schema changes (migrations) in order, exactly once each.

Why: sql/init only runs when the database volume is created for the first time.
Every later change to the schema lives in sql/migrations/NNN_description.sql
(001 is the initial schema in sql/init). This script remembers which files were
already applied in a table called schema_migrations, so running it again is safe.

Run from the project folder:
  python -m pipeline.migrate
"""

import logging
from pathlib import Path

from pipeline.extract_load import connect

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "sql" / "migrations"

log = logging.getLogger("migrate")


def pending(applied: set[str]) -> list[Path]:
    """Migration files not applied yet, in version order (002, 003, ...)."""
    return [f for f in sorted(MIGRATIONS_DIR.glob("*.sql")) if f.stem not in applied]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS public.schema_migrations (
                version     TEXT        PRIMARY KEY,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )""")
        applied = {row[0] for row in conn.execute("SELECT version FROM public.schema_migrations")}
        todo = pending(applied)
        if not todo:
            log.info("database is up to date (%d migrations applied)", len(applied))
            return
        for f in todo:
            with conn.transaction():  # the change and its bookkeeping row succeed or fail together
                conn.execute(f.read_text(encoding="utf-8"))
                conn.execute("INSERT INTO public.schema_migrations (version) VALUES (%s)", (f.stem,))
            log.info("applied %s", f.name)
        log.info("done: %d new migrations applied", len(todo))


if __name__ == "__main__":
    main()
