"""
04_build_dims.py
----------------
Builds DimDate, DimSender and DimSubject from stg_email_raw.

- Uses INSERT OR IGNORE, so re-running never creates duplicates.
- Surrogate keys are AUTOINCREMENT (DimSender, DimSubject) /
  natural YYYYMMDD integer (DimDate).
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
from calendar import day_name as CAL_DAY, month_name as CAL_MONTH
from email.utils import parseaddr, parsedate_to_datetime

from config import DB_PATH


URGENT_KW = re.compile(r"\b(urgent|asap|immediately|important|action required)\b", re.I)
MONEY_KW  = re.compile(r"(\$|\bfree\b|\bcash\b|\bwin(ner)?\b|\bprize\b|\bmoney\b)", re.I)


# ============================================================
# DIMDATE
# ============================================================
def build_dim_date(conn: sqlite3.Connection) -> int:
    dates = set()
    for (raw_date,) in conn.execute(
        "SELECT DISTINCT hdr_date FROM stg_email_raw WHERE hdr_date IS NOT NULL"
    ):
        dt = _safe_parse_date(raw_date)
        if dt is None:
            continue
        dates.add(dt.date())

    rows = []
    for d in dates:
        date_key = d.year * 10000 + d.month * 100 + d.day
        dow = d.weekday()                     # 0=Mon..6=Sun
        rows.append((
            date_key,
            d.isoformat(),
            d.year,
            (d.month - 1) // 3 + 1,
            d.month,
            CAL_MONTH[d.month],
            d.day,
            dow,
            CAL_DAY[dow],
            1 if dow >= 5 else 0,
        ))

    conn.executemany("""
        INSERT OR IGNORE INTO DimDate
            (date_key, full_date, year, quarter, month, month_name,
             day, day_of_week, day_name, is_weekend)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    return len(rows)


def _safe_parse_date(s: str):
    try:
        return parsedate_to_datetime(s)
    except (TypeError, ValueError):
        return None


# ============================================================
# DIMSENDER
# ============================================================
def build_dim_sender(conn: sqlite3.Connection) -> int:
    # {email: [display_name, min_date, max_date, count]}
    senders: dict[str, list] = {}

    for raw_from, raw_date in conn.execute(
        "SELECT hdr_from, hdr_date FROM stg_email_raw WHERE hdr_from IS NOT NULL"
    ):
        name, addr = parseaddr(raw_from)
        addr = addr.strip().lower()
        if "@" not in addr:
            continue

        dt = _safe_parse_date(raw_date)
        d_iso = dt.date().isoformat() if dt else None

        rec = senders.get(addr)
        if rec is None:
            senders[addr] = [name.strip() or None, d_iso, d_iso, 1]
        else:
            if not rec[0] and name.strip():
                rec[0] = name.strip()
            if d_iso:
                if rec[1] is None or d_iso < rec[1]:
                    rec[1] = d_iso
                if rec[2] is None or d_iso > rec[2]:
                    rec[2] = d_iso
            rec[3] += 1

    rows = []
    for addr, (name, first_d, last_d, cnt) in senders.items():
        domain = addr.split("@", 1)[1]
        is_internal = 1 if domain == "enron.com" else 0
        rows.append((addr, name, domain, is_internal, first_d, last_d, cnt))

    conn.executemany("""
        INSERT OR IGNORE INTO DimSender
            (email_address, display_name, domain, is_internal,
             first_seen_date, last_seen_date, email_count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)
    return len(rows)


# ============================================================
# DIMSUBJECT
# ============================================================
def build_dim_subject(conn: sqlite3.Connection) -> int:
    seen: dict[str, tuple] = {}

    for (subject,) in conn.execute(
        "SELECT hdr_subject FROM stg_email_raw"
    ):
        subj = subject or ""
        subj_hash = _subject_hash(subj)
        if subj_hash in seen:
            continue
        seen[subj_hash] = _subject_features(subj, subj_hash)

    rows = list(seen.values())

    conn.executemany("""
        INSERT OR IGNORE INTO DimSubject
            (subject_hash, subject_text, word_count, char_count,
             is_reply, is_forward, is_empty,
             has_urgent_keyword, has_money_keyword, uppercase_ratio)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    return len(rows)


def _subject_hash(s: str) -> str:
    norm = _normalize_subject(s)
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()


def _normalize_subject(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"^(re|fw|fwd)\s*:\s*", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _subject_features(subject: str, subj_hash: str) -> tuple:
    s = subject or ""
    stripped = s.strip()
    is_empty = 1 if stripped == "" else 0
    is_reply = 1 if re.match(r"^\s*re\s*:", s, re.I) else 0
    is_forward = 1 if re.match(r"^\s*fw(d)?\s*:", s, re.I) else 0

    letters = [c for c in s if c.isalpha()]
    upper_ratio = (sum(1 for c in letters if c.isupper()) / len(letters)) if letters else 0.0

    return (
        subj_hash,
        s[:500],                           # truncate subject text
        len(s.split()),
        len(s),
        is_reply,
        is_forward,
        is_empty,
        1 if URGENT_KW.search(s) else 0,
        1 if MONEY_KW.search(s) else 0,
        round(upper_ratio, 3),
    )


# ============================================================
def main() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = ON")

        # sanity: is the staging layer populated?
        (stg_n,) = conn.execute("SELECT COUNT(*) FROM stg_email_raw").fetchone()
        if stg_n == 0:
            raise SystemExit("[ERROR] stg_email_raw is empty — run 02_load_staging.py first.")

        print(f"[dim] source: stg_email_raw = {stg_n:,}")

        n_date    = build_dim_date(conn)
        n_sender  = build_dim_sender(conn)
        n_subject = build_dim_subject(conn)
        conn.commit()

        # verify
        (d,) = conn.execute("SELECT COUNT(*) FROM DimDate").fetchone()
        (s,) = conn.execute("SELECT COUNT(*) FROM DimSender").fetchone()
        (j,) = conn.execute("SELECT COUNT(*) FROM DimSubject").fetchone()
        (internal,) = conn.execute(
            "SELECT COUNT(*) FROM DimSender WHERE is_internal=1"
        ).fetchone()

    print(f"[dim] DimDate    : processed={n_date:,}   total={d:,}")
    print(f"[dim] DimSender  : processed={n_sender:,} total={s:,}   (is_internal=1 : {internal:,})")
    print(f"[dim] DimSubject : processed={n_subject:,} total={j:,}")


if __name__ == "__main__":
    main()
