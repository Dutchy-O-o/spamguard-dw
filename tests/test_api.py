"""
HTTP smoke tests using Flask's in-process test client.
No standalone server needed — `pytest -q` is enough.
"""
import json

import pytest


def _json(resp):
    return json.loads(resp.get_data(as_text=True))


def test_index_page(client):
    r = client.get("/")
    assert r.status_code == 200


def test_stats(client):
    r = client.get("/api/stats")
    assert r.status_code == 200
    j = _json(r)
    assert "overview" in j
    assert j["overview"]["total_emails"] > 0


def test_check_valid(client, app):
    if app.view_functions.get("api_check") is None:
        pytest.skip("api_check route missing")
    r = client.post("/api/check", json={"subject": "urgent win prize",
                                        "body":    "click now viagra"})
    if r.status_code == 503:
        pytest.skip("model not loaded in this environment")
    assert r.status_code == 200
    j = _json(r)
    assert "spam_probability" in j
    assert 0 <= j["spam_probability"] <= 1


def test_check_empty_400(client):
    r = client.post("/api/check", json={})
    if r.status_code == 503:
        pytest.skip("model not loaded")
    assert r.status_code == 400


def test_drilldown_domain(client):
    r = client.get("/api/drilldown?type=domain&value=aol.com")
    if r.status_code == 404:
        pytest.skip("aol.com not present in this DB snapshot")
    assert r.status_code == 200
    j = _json(r)
    assert j["head"]["domain"] == "aol.com"


def test_trend_and_anomalies(client):
    r1 = client.get("/api/trend")
    assert r1.status_code == 200
    assert _json(r1)["points"]

    r2 = client.get("/api/anomalies")
    assert r2.status_code == 200
    assert "anomalies" in _json(r2)


def test_openapi(client):
    r = client.get("/openapi.json")
    assert r.status_code == 200
    j = _json(r)
    assert j["openapi"].startswith("3.")
