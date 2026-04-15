"""
02_load_staging.py
------------------
Reads emails.csv (CMU Enron) row by row,
parses RFC822 headers, and writes them into the stg_email_raw table.

- Pure stdlib, no pandas (portable).
- csv.field_size_limit is raised for large message bodies.
- Batched inserts inside a single transaction for speed.
"""
import csv
import sqlite3
import sys
import time
from email import policy
from email.parser import Parser

from config import CMU_EMAILS_CSV, DB_PATH, STAGING_BATCH, CSV_FIELD_LIMIT


INSERT_SQL = """
INSERT INTO stg_email_raw (
    source_file, message_id,
    hdr_from, hdr_to, hdr_cc, hdr_bcc,
    hdr_subject, hdr_date, hdr_x_folder,
    body, raw_message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def parse_email(raw: str) -> dict:
    """RFC822 string -> dict of headers + body."""
    msg = Parser(policy=policy.default).parsestr(raw)

    # Extract visible text/plain body
    if msg.is_multipart():
        parts = [p.get_content() for p in msg.walk()
                 if p.get_content_type() == "text/plain"]
        body = "\n".join(parts) if parts else ""
    else:
        try:
            body = msg.get_content()
        except Exception:
            body = msg.get_payload(decode=False) or ""

    return {
        "message_id":  msg.get("Message-ID"),
        "from":        msg.get("From"),
        "to":          msg.get("To"),
        "cc":          msg.get("Cc"),
        "bcc":         msg.get("Bcc"),
        "subject":     msg.get("Subject"),
        "date":        msg.get("Date"),
        "x_folder":    msg.get("X-Folder"),
        "body":        body,
    }


def main(limit: int | None = None) -> None:
    if not CMU_EMAILS_CSV.exists():
        sys.exit(f"[ERROR] emails.csv not found: {CMU_EMAILS_CSV}")

    csv.field_size_limit(CSV_FIELD_LIMIT)
    start = time.time()
    batch, total, errors = [], 0, 0

    with sqlite3.connect(DB_PATH) as conn, \
         open(CMU_EMAILS_CSV, encoding="utf-8", errors="replace", newline="") as f:

        # SQLite tuning for bulk import (safe for single-writer use case)
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous  = OFF")
        conn.execute("PRAGMA temp_store   = MEMORY")

        reader = csv.DictReader(f)          # columns: file, message

        for row in reader:
            try:
                parsed = parse_email(row["message"])
                batch.append((
                    row["file"],
                    parsed["message_id"],
                    parsed["from"], parsed["to"], parsed["cc"], parsed["bcc"],
                    parsed["subject"], parsed["date"], parsed["x_folder"],
                    parsed["body"], row["message"],
                ))
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"[warn] parse error ({row.get('file')}): {e}")

            if len(batch) >= STAGING_BATCH:
                conn.executemany(INSERT_SQL, batch)
                total += len(batch)
                batch.clear()
                elapsed = time.time() - start
                rate = total / elapsed if elapsed else 0
                print(f"[stg] {total:>7,} rows  |  {rate:,.0f} rows/s", end="\r")

            if limit and (total + len(batch)) >= limit:
                break

        if batch:
            conn.executemany(INSERT_SQL, batch)
            total += len(batch)

        conn.commit()

        stg_count = conn.execute("SELECT COUNT(*) FROM stg_email_raw").fetchone()[0]
        with_msgid = conn.execute(
            "SELECT COUNT(*) FROM stg_email_raw WHERE message_id IS NOT NULL"
        ).fetchone()[0]

    elapsed = time.time() - start
    print(" " * 60, end="\r")
    print(f"[stg] done: {total:,} rows, {errors} errors, {elapsed:,.1f} s")
    print(f"[stg] stg_email_raw     = {stg_count:,}")
    print(f"[stg] rows with msg-id  = {with_msgid:,}")


if __name__ == "__main__":
    # smoke test: `python 02_load_staging.py 5000`
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    main(limit)
