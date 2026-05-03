"""
SpamGuard DW — one-shot setup script.

Usage:
    python setup.py            # interactive: choose mini or full
    python setup.py --mini     # 3 min, scanner only
    python setup.py --full     # 25 min, full DW + dashboard

Skips steps whose outputs already exist, so re-running is cheap.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def step(n: int, total: int, label: str) -> None:
    print(f"\n[{n}/{total}] {label}")
    print("-" * 60)


def run(cmd: list[str], cwd: Path = ROOT) -> None:
    t0 = time.time()
    proc = subprocess.run([sys.executable, *cmd] if cmd[0].endswith(".py") else cmd, cwd=cwd)
    dt = time.time() - t0
    if proc.returncode != 0:
        print(f"\n[error] step failed: {' '.join(cmd)} (exit {proc.returncode})")
        sys.exit(proc.returncode)
    print(f"[ok] done in {dt:.1f}s")


def ensure_env() -> None:
    env_path = ROOT / ".env"
    example  = ROOT / ".env.example"
    if env_path.exists():
        print("[skip] .env already exists")
        return
    if example.exists():
        shutil.copy(example, env_path)
        print(f"[ok] copied .env.example -> .env  (edit it to add ANTHROPIC_API_KEY for the AI assistant)")
    else:
        env_path.write_text("# Optional — paste your Anthropic key for the AI assistant\nANTHROPIC_API_KEY=\n")
        print("[ok] created empty .env  (add ANTHROPIC_API_KEY for the AI assistant)")


def install_requirements() -> None:
    if (ROOT / ".setup_deps_ok").exists():
        print("[skip] dependencies already installed")
        return
    run(["pip", "install", "-r", "requirements.txt"])
    (ROOT / ".setup_deps_ok").touch()


def db_exists() -> bool:
    return (ROOT / "db" / "SpamGuard_DW.db").exists()


def model_exists() -> bool:
    return (ROOT / "models" / "spam_model.pkl").exists()


def staging_loaded() -> bool:
    """Cheap check: emails.csv was downloaded and loaded."""
    return (ROOT / "emails.csv").exists() and db_exists()


def mini_pipeline() -> None:
    """Scanner-only: trains the model on the 25 MB Metsis subset."""
    total = 4
    step(1, total, "install requirements")
    install_requirements()

    step(2, total, "init SQLite database")
    run(["etl/01_init_db.py"])

    step(3, total, "download Enron-Spam corpus (~25 MB)")
    run(["etl/download_enron_spam.py"])

    step(4, total, "train spam model (~90s)")
    if model_exists():
        print("[skip] models/spam_model.pkl already exists — delete it to retrain")
    else:
        run(["ml/train_model.py"])


def full_pipeline() -> None:
    """Full DW: dashboard, drill-downs, AI assistant all working."""
    total = 9
    step(1, total, "install requirements")
    install_requirements()

    step(2, total, "init SQLite database")
    run(["etl/01_init_db.py"])

    step(3, total, "download Enron-Spam corpus (~25 MB)")
    run(["etl/download_enron_spam.py"])

    step(4, total, "load CMU Enron staging (~18 min, 517K emails)")
    if staging_loaded():
        print("[skip] staging already loaded")
    else:
        run(["etl/02_load_staging.py"])

    step(5, total, "load spam labels")
    run(["etl/03_load_labels.py", "--real"])

    step(6, total, "build dimension tables")
    run(["etl/04_build_dims.py"])

    step(7, total, "train spam model (~90s)")
    if model_exists():
        print("[skip] model already trained — delete models/spam_model.pkl to retrain")
    else:
        run(["ml/train_model.py"])

    step(8, total, "build fact table")
    run(["etl/05_build_fact.py"])

    step(9, total, "infer labels for unlabeled emails + create analytical views")
    run(["etl/06_infer_labels.py"])

    sql_file = ROOT / "sql" / "analytical_views.sql"
    if sql_file.exists():
        print("[ok] applying analytical_views.sql")
        # try sqlite3 cli first; if missing, use Python's sqlite3
        if shutil.which("sqlite3"):
            subprocess.run(["sqlite3", str(ROOT / "db" / "SpamGuard_DW.db")],
                           input=sql_file.read_text(), text=True, check=False)
        else:
            import sqlite3
            with sqlite3.connect(ROOT / "db" / "SpamGuard_DW.db") as conn:
                conn.executescript(sql_file.read_text())
            print("[ok] applied via Python sqlite3 (sqlite3 CLI not on PATH)")


def main() -> None:
    p = argparse.ArgumentParser(description="SpamGuard DW one-shot setup")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--mini", action="store_true", help="scanner-only, ~3 minutes")
    g.add_argument("--full", action="store_true", help="full DW + dashboard, ~25 minutes")
    args = p.parse_args()

    print("SpamGuard DW — automated setup")
    print("=" * 60)

    ensure_env()

    mode = None
    if args.mini:
        mode = "mini"
    elif args.full:
        mode = "full"
    else:
        print("\nWhich setup do you want?")
        print("  1) MINI (~3 min) — spam checker + scanner only, dashboard empty")
        print("  2) FULL (~25 min) — entire DW: dashboard, drill-downs, all 517K emails")
        choice = input("\nEnter 1 or 2: ").strip()
        mode = "mini" if choice == "1" else "full"

    print(f"\nRunning {mode.upper()} setup...\n")

    if mode == "mini":
        mini_pipeline()
    else:
        full_pipeline()

    print("\n" + "=" * 60)
    print("[done] setup complete")
    print()
    print("To start the web app:")
    print("    python webapp/app.py")
    print()
    print("Then open http://127.0.0.1:5000 in your browser.")
    if mode == "mini":
        print()
        print("Note: dashboard pages will be empty in mini setup.")
        print("Try the /scanner page with docs/demo_scan.csv to test bulk scanning.")


if __name__ == "__main__":
    main()
