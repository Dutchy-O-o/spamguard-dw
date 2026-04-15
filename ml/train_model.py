"""
train_model.py
--------------
Trains the spam detection model and serializes it to disk.

Pipeline: TF-IDF  +  MultinomialNB
Input   : Enron-Spam preprocessed folders (subject + body text files)
Output  : models/spam_model.pkl

When the preprocessed folder is present, it is the preferred source
(correct labels from Metsis et al. 2006). If it is missing we fall back to
the single-CSV version (known to be broken), then to the DB join.
"""
from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path

import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "etl"))
from config import DB_PATH, ENRON_SPAM_CSV, ENRON_SPAM_DIR  # noqa: E402

MODEL_PATH = Path(__file__).resolve().parent.parent / "models" / "spam_model.pkl"


# Fallback — when neither folder nor CSV is present, pull labeled rows from DB
SQL_TRAINING_SET = """
SELECT
    COALESCE(e.hdr_subject, '') || '  ' || COALESCE(e.body, '') AS text,
    l.label
FROM stg_email_raw e
JOIN stg_spam_labels l ON l.message_id = e.message_id
WHERE l.label IN ('spam','ham')
"""


def load_from_folders():
    """Metsis original: enron1..6/spam/*.txt, enron1..6/ham/*.txt — preferred."""
    X, y = [], []
    for path in ENRON_SPAM_DIR.rglob("*.txt"):
        parts = {p.lower() for p in path.parts}
        if "spam" in parts:
            label = 1
        elif "ham" in parts:
            label = 0
        else:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        # The first line is typically "Subject: ..." — we feed the whole file
        # to the vectorizer and let the model learn from it.
        X.append(text)
        y.append(label)
    return X, y


def load_from_csv():
    """BROKEN CSV variant (fallback only)."""
    csv.field_size_limit(10_000_000)
    X, y = [], []
    with ENRON_SPAM_CSV.open("r", encoding="utf-8", errors="replace", newline="") as f:
        for r in csv.DictReader(f):
            label = (r.get("Spam/Ham") or r.get("label") or "").strip().lower()
            if label not in ("spam", "ham"):
                continue
            subj = r.get("Subject")  or r.get("subject") or ""
            body = r.get("Message")  or r.get("body")    or ""
            X.append(f"{subj}  {body}")
            y.append(1 if label == "spam" else 0)
    return X, y


def load_from_db():
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(SQL_TRAINING_SET).fetchall()
    if not rows:
        raise SystemExit("[ml] no labeled rows found — no source available.")
    X = [r[0] for r in rows]
    y = [1 if r[1] == "spam" else 0 for r in rows]
    return X, y


def load_data():
    # PRIORITY: original folder > CSV (broken) > DB
    if ENRON_SPAM_DIR.exists() and any(ENRON_SPAM_DIR.rglob("*.txt")):
        print(f"[ml] source: FOLDER ({ENRON_SPAM_DIR.name})")
        return load_from_folders()
    if ENRON_SPAM_CSV.exists():
        print(f"[ml] source: CSV ({ENRON_SPAM_CSV.name})  — BROKEN!")
        return load_from_csv()
    print("[ml] source: DB join")
    return load_from_db()


def build_pipeline() -> Pipeline:
    return Pipeline([
        ("tfidf", TfidfVectorizer(
            lowercase=True,
            ngram_range=(1, 2),
            min_df=2,
            max_df=0.95,
            max_features=20_000,
            stop_words="english",
        )),
        ("clf", MultinomialNB(alpha=0.1)),
    ])


def main() -> None:
    X, y = load_data()
    print(f"[ml] training set: {len(X):,}  (spam={sum(y)}  ham={len(y) - sum(y)})")

    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    pipe = build_pipeline()
    pipe.fit(X_tr, y_tr)

    y_pred = pipe.predict(X_te)
    print("\n[ml] --- classification report ---")
    print(classification_report(y_te, y_pred, target_names=["ham", "spam"], digits=3))
    print("[ml] confusion matrix [[TN FP][FN TP]]:")
    print(confusion_matrix(y_te, y_pred))

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, MODEL_PATH)
    print(f"\n[ml] model saved: {MODEL_PATH}")


if __name__ == "__main__":
    main()
