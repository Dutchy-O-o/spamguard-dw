"""
SpamGuard DW — DW sanity tests.
Skips gracefully if the warehouse hasn't been built (e.g. in CI).
Run: pytest -q
"""
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "etl"))
from config import DB_PATH  # noqa: E402


@pytest.fixture(scope="module")
def conn():
    if not DB_PATH.exists():
        pytest.skip(f"DW not built (DB missing: {DB_PATH})")
    c = sqlite3.connect(DB_PATH)
    yield c
    c.close()


def test_core_tables_present(conn):
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    for t in ("stg_email_raw", "stg_spam_labels",
              "DimSender", "DimDate", "DimSubject", "FactEmail"):
        assert t in tables, f"{t} missing"


def test_no_orphan_fks(conn):
    (o_sender,)  = conn.execute("SELECT COUNT(*) FROM FactEmail f LEFT JOIN DimSender  s USING(sender_key)  WHERE s.sender_key  IS NULL").fetchone()
    (o_date,)    = conn.execute("SELECT COUNT(*) FROM FactEmail f LEFT JOIN DimDate    d USING(date_key)    WHERE d.date_key    IS NULL").fetchone()
    (o_subject,) = conn.execute("SELECT COUNT(*) FROM FactEmail f LEFT JOIN DimSubject j USING(subject_key) WHERE j.subject_key IS NULL").fetchone()
    assert o_sender  == 0, f"{o_sender} orphan sender_key"
    assert o_date    == 0, f"{o_date} orphan date_key"
    assert o_subject == 0, f"{o_subject} orphan subject_key"


def test_views_present(conn):
    views = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='view'")}
    for v in ("v_spam_overview", "v_spam_by_domain",
              "v_spam_by_weekday", "v_top_senders"):
        assert v in views, f"view {v} missing"


def test_overview_sanity(conn):
    r = conn.execute("SELECT total_emails, spam_count, ham_count, unlabeled_count "
                     "FROM v_spam_overview").fetchone()
    total, spam, ham, un = r
    assert total > 0
    assert (spam + ham + un) == total


def test_dim_sender_is_internal_enum(conn):
    rows = {r[0] for r in conn.execute(
        "SELECT DISTINCT is_internal FROM DimSender")}
    assert rows.issubset({0, 1})
