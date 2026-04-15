"""
06_infer_labels.py
------------------
Predicts is_spam for FactEmail rows where is_spam IS NULL using the
saved ML model.

- label_source = 'model-prediction:p=<prob>'
- Only updates currently NULL rows (never overwrites real labels).
- A row is marked spam if spam_probability >= THRESHOLD.
"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

import joblib

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "etl"))
from config import DB_PATH  # noqa: E402

MODEL_PATH = Path(__file__).resolve().parent.parent / "models" / "spam_model.pkl"
THRESHOLD  = 0.5
BATCH      = 5000


def main() -> None:
    if not MODEL_PATH.exists():
        raise SystemExit(f"[ERROR] model not found: {MODEL_PATH}")

    print(f"[infer] loading model: {MODEL_PATH.name}")
    model = joblib.load(MODEL_PATH)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous  = OFF")

        (total_null,) = conn.execute(
            "SELECT COUNT(*) FROM FactEmail WHERE is_spam IS NULL"
        ).fetchone()
        print(f"[infer] unlabeled rows: {total_null:,}")
        if total_null == 0:
            return

        # For each unlabeled FactEmail, pull matching staging text
        cur = conn.execute("""
            SELECT f.email_key,
                   COALESCE(e.hdr_subject,'') || '  ' || COALESCE(e.body,'') AS text
            FROM FactEmail f
            JOIN stg_email_raw e ON e.message_id = f.message_id
            WHERE f.is_spam IS NULL
        """)

        start = time.time()
        batch_keys: list = []
        batch_text: list = []
        updated = 0
        spam_n  = 0

        def flush():
            nonlocal updated, spam_n
            if not batch_keys:
                return
            probs = model.predict_proba(batch_text)[:, 1]
            rows = []
            for k, p in zip(batch_keys, probs):
                is_spam = 1 if p >= THRESHOLD else 0
                spam_n += is_spam
                rows.append((is_spam, float(p), k))
            conn.executemany("""
                UPDATE FactEmail
                SET is_spam = ?,
                    label_source = 'model-prediction:p=' || printf('%.3f', ?)
                WHERE email_key = ? AND is_spam IS NULL
            """, rows)
            updated += len(rows)
            batch_keys.clear()
            batch_text.clear()

        for key, text in cur:
            batch_keys.append(key)
            batch_text.append(text)
            if len(batch_keys) >= BATCH:
                flush()
                elapsed = time.time() - start
                rate = updated / elapsed if elapsed else 0
                print(f"[infer] {updated:>7,} / {total_null:,}  | {rate:,.0f} rows/s", end="\r")

        flush()
        conn.commit()

    elapsed = time.time() - start
    print(" " * 60, end="\r")
    print(f"[infer] done: {updated:,} rows, {elapsed:,.1f} s")
    print(f"[infer] model spam predictions: {spam_n:,}  ham: {updated - spam_n:,}")


if __name__ == "__main__":
    main()
