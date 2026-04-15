"""
01_init_db.py
-------------
Creates SpamGuard_DW.db and executes schema.sql.
Drops any existing DB first — idempotent.
"""
import sqlite3
from config import DB_PATH, SCHEMA_SQL


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"[init] old DB removed: {DB_PATH.name}")

    sql = SCHEMA_SQL.read_text(encoding="utf-8")

    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(sql)
        conn.commit()

        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()

    print(f"[init] DB created: {DB_PATH}")
    print(f"[init] tables ({len(tables)}):")
    for (t,) in tables:
        print(f"       - {t}")


if __name__ == "__main__":
    main()
