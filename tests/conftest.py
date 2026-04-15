"""Shared pytest fixtures."""
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "webapp"))
sys.path.insert(0, str(ROOT / "etl"))


@pytest.fixture(scope="session")
def app():
    from config import DB_PATH
    if not DB_PATH.exists():
        pytest.skip(f"DW not built (DB missing: {DB_PATH})")
    from app import app as flask_app
    flask_app.config.update(TESTING=True)
    return flask_app


@pytest.fixture(scope="session")
def client(app):
    return app.test_client()
