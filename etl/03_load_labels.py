"""
03_load_labels.py
-----------------
Populates the stg_spam_labels table.

TWO MODES:
  [real]  Scans the Enron-Spam folder (enron1..6/spam, /ham)
  [mock]  If no folder is present: picks a random subset of stg_email_raw rows
          and labels them spam/ham — used only to test downstream ETL steps.

Usage:
  python 03_load_labels.py           # auto: real if folder present, else mock
  python 03_load_labels.py --mock    # force mock
  python 03_load_labels.py --real    # force real (errors out if folder missing)
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import random
import re
import sqlite3
import sys
from pathlib import Path

from config import DB_PATH, ENRON_SPAM_CSV, ENRON_SPAM_DIR


# ---- MOCK parameters ----
MOCK_SAMPLE_RATIO = 0.20   # label 20% of stg_email_raw
MOCK_SPAM_RATIO   = 0.30   # of those labeled, 30% become spam
MOCK_SEED         = 42


INSERT_SQL = """
INSERT INTO stg_spam_labels
    (message_id, source_file, subject_hash, label, label_source)
VALUES (?, ?, ?, ?, ?)
"""


# ============================================================
# REAL CSV MODE — Metsis/Wiechmann single-file version
# Format: Message ID, Subject, Message, Spam/Ham, Date
# ============================================================
def extract_csv(conn: sqlite3.Connection) -> int:
    """
    NOTE: this CSV variant turned out to have corrupted labels (only 6 unique
    'spam' rows repeated 17K times). Kept here only as a fallback.
    """
    csv.field_size_limit(10_000_000)
    rows = []
    with ENRON_SPAM_CSV.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        # column names may vary between versions — be lenient:
        for r in reader:
            label = (r.get("Spam/Ham") or r.get("label") or "").strip().lower()
            if label not in ("spam", "ham"):
                continue
            subject = (r.get("Subject") or r.get("subject") or "") or ""
            rows.append((
                None,                    # no RFC822 Message-ID (only a row number)
                None,                    # no source_file either
                _sha1(subject),          # primary join key
                label,
                "enron-spam-csv",
            ))
    conn.executemany(INSERT_SQL, rows)
    return len(rows)


# ============================================================
# REAL FOLDER MODE — spam/ham subfolder structure
# ============================================================
def extract_real(conn: sqlite3.Connection) -> int:
    """
    Enron-Spam preprocessed folder layout:
        enron1/spam/0001.1999-12-10.farmer.spam.txt
        enron1/ham /0002.1999-12-14.kaminski.ham.txt

    NOTE: this preprocessed variant has NO 'Message-ID' header; the first line
          is 'Subject: ...'. Therefore subject_hash is used as the join key
          (fallback). When a new dataset is downloaded, only the folder path
          needs to change — this function stays the same.
    """
    files = sorted(ENRON_SPAM_DIR.rglob("*.txt"))
    if not files:
        raise FileNotFoundError(f"No Enron-Spam .txt files found under: {ENRON_SPAM_DIR}")

    rows = []
    for path in files:
        label = _label_from_path(path)
        if label is None:
            continue

        subject = _extract_subject(path)
        subj_hash = _sha1(subject) if subject else None

        rows.append((
            None,                  # no Message-ID in preprocessed corpus
            path.name,             # source_file fallback
            subj_hash,             # primary join key
            label,
            "enron-spam-preprocessed",
        ))

    conn.executemany(INSERT_SQL, rows)
    return len(rows)


def _label_from_path(path: Path) -> str | None:
    parts = {p.lower() for p in path.parts}
    if "spam" in parts:
        return "spam"
    if "ham" in parts:
        return "ham"
    return None


def _extract_subject(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            first = f.readline().strip()
    except Exception:
        return ""
    return first[len("Subject:"):].strip() if first.lower().startswith("subject:") else ""


# ============================================================
# MOCK MODE — randomly labels rows already in stg_email_raw
# ============================================================
def extract_mock(conn: sqlite3.Connection) -> int:
    rng = random.Random(MOCK_SEED)

    rows = conn.execute("""
        SELECT message_id, source_file, hdr_subject
        FROM stg_email_raw
        WHERE message_id IS NOT NULL
    """).fetchall()

    if not rows:
        raise RuntimeError("stg_email_raw is empty — run 02_load_staging.py first.")

    sample_size = max(1, int(len(rows) * MOCK_SAMPLE_RATIO))
    sampled = rng.sample(rows, sample_size)

    to_insert = []
    for msgid, srcfile, subject in sampled:
        label = "spam" if rng.random() < MOCK_SPAM_RATIO else "ham"
        subj_hash = _sha1(subject) if subject else None
        to_insert.append((msgid, srcfile, subj_hash, label, "mock-random"))

    conn.executemany(INSERT_SQL, to_insert)
    return len(to_insert)


# ============================================================
def _sha1(text: str) -> str:
    return hashlib.sha1(_normalize_subject(text).encode("utf-8")).hexdigest()


def _normalize_subject(s: str) -> str:
    """Strip Re:/Fwd: prefixes and collapse whitespace — for consistent hashing."""
    s = (s or "").lower().strip()
    s = re.sub(r"^(re|fw|fwd)\s*:\s*", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


# ============================================================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mock",   action="store_true", help="force mock mode")
    ap.add_argument("--real",   action="store_true", help="force real mode (CSV or folder)")
    args = ap.parse_args()

    if args.mock and args.real:
        sys.exit("[ERROR] --mock and --real cannot be combined")

    folder_ready = ENRON_SPAM_DIR.exists() and any(ENRON_SPAM_DIR.rglob("*.txt"))
    csv_ready    = ENRON_SPAM_CSV.exists()

    if args.real and not (folder_ready or csv_ready):
        sys.exit(f"[ERROR] real mode requested but no source found.\n"
                 f"       FOLDER : {ENRON_SPAM_DIR}\n"
                 f"       CSV    : {ENRON_SPAM_CSV}")

    use_mock = args.mock or (not args.real and not folder_ready and not csv_ready)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM stg_spam_labels")

        # PRIORITY: folder (Metsis original, correct) > CSV (broken, fallback)
        if use_mock:
            print("[lbl] MOCK mode — no real labels available")
            n = extract_mock(conn)
        elif folder_ready:
            print(f"[lbl] REAL-FOLDER mode — {ENRON_SPAM_DIR}")
            n = extract_real(conn)
        else:
            print(f"[lbl] REAL-CSV mode (fallback) — {ENRON_SPAM_CSV.name}")
            n = extract_csv(conn)

        conn.commit()

        spam = conn.execute("SELECT COUNT(*) FROM stg_spam_labels WHERE label='spam'").fetchone()[0]
        ham  = conn.execute("SELECT COUNT(*) FROM stg_spam_labels WHERE label='ham'").fetchone()[0]

    print(f"[lbl] labels inserted: {n:,}  (spam={spam:,}  ham={ham:,})")


if __name__ == "__main__":
    main()
