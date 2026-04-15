"""
05_build_fact.py
----------------
stg_email_raw  +  Dim*  +  stg_spam_labels  ->  FactEmail

Approach:
  1) Pull dim natural-key → surrogate-key lookup maps into memory.
  2) Walk stg_email_raw row by row; for each row compute:
       - sender_key    from the sender email address
       - date_key      from hdr_date (YYYYMMDD)
       - subject_key   from subject_hash
       - is_spam       from stg_spam_labels (message_id > subject_hash fallback)
     along with body-level measures.
  3) Batch-insert into FactEmail.

Idempotent: truncates FactEmail before re-inserting.
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from email.utils import getaddresses, parseaddr, parsedate_to_datetime

from config import DB_PATH, STAGING_BATCH


LINK_RE       = re.compile(r"https?://|www\.", re.I)
ATTACHMENT_RE = re.compile(r"attach(ed|ment)|enclosed|\.pdf\b|\.xls[xm]?\b|\.docx?\b", re.I)


INSERT_FACT = """
INSERT INTO FactEmail (
    message_id, sender_key, date_key, subject_key,
    recipient_count, cc_count, bcc_count,
    body_char_count, body_word_count, link_count,
    has_attachment_hint,
    is_spam, label_source, source_file
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# ============================================================
def load_lookups(conn: sqlite3.Connection):
    date_keys = {k for (k,) in conn.execute("SELECT date_key FROM DimDate")}

    sender_map = {
        email: key
        for key, email in conn.execute("SELECT sender_key, email_address FROM DimSender")
    }

    subject_map = {
        h: key
        for key, h in conn.execute("SELECT subject_key, subject_hash FROM DimSubject")
    }

    # 2-level label map: by message_id (if present) and by subject_hash (fallback)
    label_by_msgid: dict = {}
    label_by_subj:  dict = {}
    for msgid, subj_hash, label, src in conn.execute("""
        SELECT message_id, subject_hash, label, label_source
        FROM stg_spam_labels
    """):
        if msgid:
            label_by_msgid[msgid] = (label, src)
        if subj_hash and subj_hash not in label_by_subj:
            label_by_subj[subj_hash] = (label, src)

    return date_keys, sender_map, subject_map, label_by_msgid, label_by_subj


# ============================================================
def normalize_subject(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"^(re|fw|fwd)\s*:\s*", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def subject_hash(s: str) -> str:
    return hashlib.sha1(normalize_subject(s).encode("utf-8")).hexdigest()


def safe_date_key(raw: str) -> int | None:
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    return dt.year * 10000 + dt.month * 100 + dt.day


def address_count(raw: str) -> int:
    if not raw:
        return 0
    return len([a for _, a in getaddresses([raw]) if a and "@" in a])


# ============================================================
def main() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")  # off during bulk insert
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous  = OFF")

        (stg_n,) = conn.execute("SELECT COUNT(*) FROM stg_email_raw").fetchone()
        if stg_n == 0:
            raise SystemExit("[ERROR] stg_email_raw is empty.")

        print(f"[fact] source = {stg_n:,}")
        print("[fact] loading lookups...")
        date_keys, sender_map, subject_map, label_by_msgid, label_by_subj = load_lookups(conn)
        print(f"       DimDate={len(date_keys):,}  DimSender={len(sender_map):,}  "
              f"DimSubject={len(subject_map):,}  "
              f"Labels: msgid={len(label_by_msgid):,} subj={len(label_by_subj):,}")

        conn.execute("DELETE FROM FactEmail")

        start = time.time()
        batch = []
        inserted = 0
        skipped  = {"no_sender": 0, "no_date": 0, "no_subject": 0}

        cur = conn.execute("""
            SELECT source_file, message_id, hdr_from,
                   hdr_to, hdr_cc, hdr_bcc,
                   hdr_subject, hdr_date, body
            FROM stg_email_raw
        """)

        for (src_file, msgid, hdr_from,
             hdr_to, hdr_cc, hdr_bcc,
             hdr_subject, hdr_date, body) in cur:

            # ---- surrogate key lookups ----
            _, addr = parseaddr(hdr_from or "")
            sender_key = sender_map.get(addr.strip().lower())
            if sender_key is None:
                skipped["no_sender"] += 1
                continue

            date_key = safe_date_key(hdr_date)
            if date_key is None or date_key not in date_keys:
                skipped["no_date"] += 1
                continue

            subj_h = subject_hash(hdr_subject or "")
            subject_key = subject_map.get(subj_h)
            if subject_key is None:
                skipped["no_subject"] += 1
                continue

            # ---- measures ----
            body = body or ""
            body_words = len(body.split())
            body_chars = len(body)
            link_count = len(LINK_RE.findall(body))
            has_attach = 1 if ATTACHMENT_RE.search(body) else 0

            # ---- label join: message_id first, then subject_hash ----
            lbl = label_by_msgid.get(msgid) or label_by_subj.get(subj_h)
            if lbl:
                is_spam = 1 if lbl[0] == "spam" else 0
                label_source = lbl[1]
            else:
                is_spam = None
                label_source = None

            batch.append((
                msgid, sender_key, date_key, subject_key,
                address_count(hdr_to), address_count(hdr_cc), address_count(hdr_bcc),
                body_chars, body_words, link_count,
                has_attach,
                is_spam, label_source, src_file,
            ))

            if len(batch) >= STAGING_BATCH:
                conn.executemany(INSERT_FACT, batch)
                inserted += len(batch)
                batch.clear()
                rate = inserted / (time.time() - start)
                print(f"[fact] {inserted:>7,} rows | {rate:,.0f} rows/s", end="\r")

        if batch:
            conn.executemany(INSERT_FACT, batch)
            inserted += len(batch)

        conn.commit()

        # ---- summary ----
        (total,)   = conn.execute("SELECT COUNT(*) FROM FactEmail").fetchone()
        (spam,)    = conn.execute("SELECT COUNT(*) FROM FactEmail WHERE is_spam=1").fetchone()
        (ham,)     = conn.execute("SELECT COUNT(*) FROM FactEmail WHERE is_spam=0").fetchone()
        (unlab,)   = conn.execute("SELECT COUNT(*) FROM FactEmail WHERE is_spam IS NULL").fetchone()
        (avg_w,)   = conn.execute("SELECT AVG(body_word_count) FROM FactEmail").fetchone()
        (avg_l,)   = conn.execute("SELECT AVG(link_count)      FROM FactEmail").fetchone()

    elapsed = time.time() - start
    print(" " * 60, end="\r")
    print(f"[fact] done: {inserted:,} rows, {elapsed:,.1f} s")
    print(f"[fact] skipped: no_sender={skipped['no_sender']}, "
          f"no_date={skipped['no_date']}, no_subject={skipped['no_subject']}")
    print(f"[fact] FactEmail total = {total:,}")
    print(f"       spam={spam:,}  ham={ham:,}  unlabeled={unlab:,}")
    print(f"       avg word_count={avg_w:.1f}   avg link_count={avg_l:.2f}")


if __name__ == "__main__":
    main()
