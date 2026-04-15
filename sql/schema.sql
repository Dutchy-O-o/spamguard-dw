-- ============================================================
-- SpamGuard_DW  —  Star Schema for Enron Spam Detection
-- Source 1 : CMU Enron Email Dataset  (emails.csv, unlabeled)
-- Source 2 : Enron-Spam labels         (external, joined via subject_hash)
-- Engine   : SQLite (portable)
-- ============================================================

PRAGMA foreign_keys = ON;

-- ------------------------------------------------------------
-- STAGING LAYER
-- Raw source data lands here 1:1, then is transformed into dim/fact.
-- ------------------------------------------------------------

DROP TABLE IF EXISTS stg_email_raw;
CREATE TABLE stg_email_raw (
    staging_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file     TEXT,                  -- 'file' column from emails.csv (e.g. allen-p/_sent_mail/1.)
    message_id      TEXT,                  -- RFC822 Message-ID  (primary label join key)
    hdr_from        TEXT,
    hdr_to          TEXT,
    hdr_cc          TEXT,
    hdr_bcc         TEXT,
    hdr_subject     TEXT,
    hdr_date        TEXT,                  -- original date string, not yet parsed
    hdr_x_folder    TEXT,
    body            TEXT,
    raw_message     TEXT,                  -- full RFC822 (kept for debugging / re-parse)
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stg_message_id ON stg_email_raw(message_id);
CREATE INDEX idx_stg_source_file ON stg_email_raw(source_file);


DROP TABLE IF EXISTS stg_spam_labels;
CREATE TABLE stg_spam_labels (
    label_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      TEXT,                  -- primary join key (when available)
    source_file     TEXT,                  -- fallback join key
    subject_hash    TEXT,                  -- last-resort join key
    label           TEXT CHECK(label IN ('spam','ham')),
    label_source    TEXT,                  -- e.g. 'enron-spam-preprocessed'
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stg_label_msgid ON stg_spam_labels(message_id);


-- ------------------------------------------------------------
-- DIMENSIONS
-- ------------------------------------------------------------

DROP TABLE IF EXISTS DimSender;
CREATE TABLE DimSender (
    sender_key       INTEGER PRIMARY KEY AUTOINCREMENT,
    email_address    TEXT UNIQUE NOT NULL,
    display_name     TEXT,
    domain           TEXT,
    is_internal      INTEGER,              -- 1 = @enron.com
    first_seen_date  TEXT,
    last_seen_date   TEXT,
    email_count      INTEGER DEFAULT 0
);


DROP TABLE IF EXISTS DimDate;
CREATE TABLE DimDate (
    date_key         INTEGER PRIMARY KEY,  -- YYYYMMDD
    full_date        TEXT NOT NULL,
    year             INTEGER,
    quarter          INTEGER,
    month            INTEGER,
    month_name       TEXT,
    day              INTEGER,
    day_of_week      INTEGER,              -- 0=Mon..6=Sun
    day_name         TEXT,
    is_weekend       INTEGER
);


DROP TABLE IF EXISTS DimSubject;
CREATE TABLE DimSubject (
    subject_key        INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_hash       TEXT UNIQUE,        -- SHA1 — merges duplicates
    subject_text       TEXT,
    word_count         INTEGER,
    char_count         INTEGER,
    is_reply           INTEGER,            -- starts with "Re:"
    is_forward         INTEGER,            -- starts with "Fw:/Fwd:"
    is_empty           INTEGER,
    has_urgent_keyword INTEGER,            -- urgent / asap / important
    has_money_keyword  INTEGER,            -- $, free, cash, win
    uppercase_ratio    REAL
);


-- ------------------------------------------------------------
-- FACT
-- ------------------------------------------------------------

DROP TABLE IF EXISTS FactEmail;
CREATE TABLE FactEmail (
    email_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id          TEXT,               -- degenerate dimension

    sender_key          INTEGER NOT NULL,
    date_key            INTEGER NOT NULL,
    subject_key         INTEGER NOT NULL,

    -- measures
    recipient_count     INTEGER,
    cc_count            INTEGER,
    bcc_count           INTEGER,
    body_char_count     INTEGER,
    body_word_count     INTEGER,
    link_count          INTEGER,
    has_attachment_hint INTEGER,

    -- target variable
    is_spam             INTEGER,            -- 1=spam, 0=ham, NULL=unlabeled
    label_source        TEXT,

    source_file         TEXT,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (sender_key)  REFERENCES DimSender(sender_key),
    FOREIGN KEY (date_key)    REFERENCES DimDate(date_key),
    FOREIGN KEY (subject_key) REFERENCES DimSubject(subject_key)
);

CREATE INDEX idx_fact_sender  ON FactEmail(sender_key);
CREATE INDEX idx_fact_date    ON FactEmail(date_key);
CREATE INDEX idx_fact_subject ON FactEmail(subject_key);
CREATE INDEX idx_fact_is_spam ON FactEmail(is_spam);
