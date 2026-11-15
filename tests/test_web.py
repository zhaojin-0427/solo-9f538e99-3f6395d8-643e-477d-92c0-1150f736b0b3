"""Web-layer tests through Flask's test client."""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import app as app_module


@pytest.fixture
def web(tmp_path, sample, monkeypatch):
    monkeypatch.setenv("COLLECTOR_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("COLLECTOR_PARTIALS", str(tmp_path / "partials"))
    application = app_module.create_app()
    application.config.update(TESTING=False)
    return application, sample


def test_index_renders(web):
    client, _ = web[0].test_client(), web[1]
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Manifest URL" in resp.data


def test_api_collect_publishes_and_downloads(web):
    application, sample = web
    client = application.test_client()
    resp = client.post(
        "/api/collect",
        json={"manifest_url": sample.base + "/manifest.json", "day": "2026-11-10"},
    )
    assert resp.status_code == 200, resp.data
    payload = resp.get_json()
    assert payload["status"] == "published"
    assert payload["revision"]["rows"] == 12

    download = client.get(payload["download_url"])
    assert download.status_code == 200
    assert download.headers["X-Collected-Revision"] == payload["revision"]["revision"]
    assert download.data.decode().startswith("date,market,product,price")
    assert "attachment" in download.headers["Content-Disposition"]


def test_api_collect_reports_unchanged(web):
    client, sample = web[0].test_client(), web[1]
    body = {"manifest_url": sample.base + "/manifest.json", "day": "2026-11-10"}
    assert client.post("/api/collect", json=body).status_code == 200
    second = client.post("/api/collect", json=body)
    assert second.get_json()["status"] == "unchanged"


def test_api_collect_rejects_bad_day(web):
    client, sample = web[0].test_client(), web[1]
    resp = client.post(
        "/api/collect",
        json={"manifest_url": sample.base + "/manifest.json", "day": "2026-02-30"},
    )
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "invalid_input"


def test_api_collect_rejects_file_url(web):
    client = web[0].test_client()
    resp = client.post("/api/collect", json={"manifest_url": "file:///etc/passwd", "day": "2026-11-10"})
    assert resp.status_code == 400


def test_form_collect_redirects_to_source(web):
    application, sample = web
    client = application.test_client()
    resp = client.post(
        "/collect",
        data={"manifest_url": sample.base + "/manifest.json", "day": "2026-11-10"},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert "/sources/" in resp.headers["Location"]
    page = client.get(resp.headers["Location"])
    assert page.status_code == 200
    assert b"current" in page.data


def test_source_page_switch_and_old_download_still_works(web):
    application, sample = web
    client = application.test_client()
    body = {"manifest_url": sample.base + "/manifest.json", "day": "2026-11-10"}
    first = client.post("/api/collect", json=body).get_json()
    source_url = first["source_url"]
    page = client.get(source_url)
    assert page.status_code == 200
    source_id = int(source_url.rstrip("/").rsplit("/", 1)[-1])

    sample.app.config["SAMPLE"].switch("2026-11-10-a")
    second = client.post("/api/collect", json=body).get_json()
    old_id = first["revision"]["id"]
    newest_id = second["revision"]["id"]

    # The older revision is still downloadable after a new one is published.
    assert client.get("/download/%d" % old_id).status_code == 200

    resp = client.post(
        "/sources/%d/switch" % source_id,
        data={"revision_id": str(old_id)},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    api = client.get("/api/sources/%d" % source_id).get_json()
    by_id = {r["id"]: r for r in api["revisions"]}
    assert by_id[old_id]["current"] is True
    assert by_id[newest_id]["current"] is False


def test_download_missing_revision_returns_404(web):
    client = web[0].test_client()
    assert client.get("/download/9999").status_code == 404


def test_partial_never_listed_in_ui(web, monkeypatch):
    application, sample = web
    client = application.test_client()
    sample.app.config["SAMPLE"].arm_interrupt(90)
    resp = client.post(
        "/collect",
        data={"manifest_url": sample.base + "/manifest.json", "day": "2026-11-10"},
    )
    assert resp.status_code == 502
    page = client.get("/").data.decode()
    assert "No sources collected yet" in page or "none" in page
    # ... but the attempt is recorded.
    assert "IncompleteDownload" not in page
    assert "length_mismatch: download ended after 90" in page
    assert "/download/" not in page


def test_healthz(web):
    assert web[0].test_client().get("/healthz").get_json()["ok"] is True
