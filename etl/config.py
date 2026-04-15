"""SpamGuard_DW — central configuration."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data sources
CMU_EMAILS_CSV   = PROJECT_ROOT / "emails.csv"
ENRON_SPAM_DIR   = PROJECT_ROOT / "SpamData" / "enron-spam-original"  # Metsis original spam/ham folders
ENRON_SPAM_CSV   = PROJECT_ROOT / "SpamData" / "enron_spam_data.csv"  # BROKEN — no longer used

# DB
DB_PATH          = PROJECT_ROOT / "db" / "SpamGuard_DW.db"
SCHEMA_SQL       = PROJECT_ROOT / "sql" / "schema.sql"

# ETL parameters
STAGING_BATCH    = 2000       # rows per commit
CSV_FIELD_LIMIT  = 10_000_000 # max CSV field size (large message bodies)
