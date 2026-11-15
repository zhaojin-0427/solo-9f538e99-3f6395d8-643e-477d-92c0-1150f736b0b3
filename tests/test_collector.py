import datetime as dt
import hashlib
import json
import os
import sqlite3
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import downloader
from errors import (
    BusyError,
    IncompleteDownload,
    InputError,
    ManifestError,
    SourceChanged,
    ValidationError,
)
from ingest import Collector
from manifest import parse_day, parse_manifest, validate_csv
from store import Store
from transport import fetch_manifest


DAY = dt.date(2026, 11, 10)


def make_store(tmp_path):
    return Store(str(tmp_path / "db.sqlite3"), str(tmp_path / "files"))


def manifest_url(server):
    return server.base + "/manifest.json"


# --------------------------------------------------------------------------
# Manifest / input parsing
# --------------------------------------------------------------------------
@pytest.mark.parametrize("bad", ["2024-1-1", "2024-02-30", "20240101", "", "2024/01/01", None, 5])
def test_parse_day_rejects_bad_dates(bad):
    with pytest.raises(InputError):
        parse_day(bad)


def test_parse_day_accepts_valid_date():
    assert parse_day("2026-11-10") == dt.date(2026, 11, 10)


def test_parse_manifest_requires_fields():
    with pytest.raises(ManifestError):
        parse_manifest({"revision": "r1"}, "http://x/manifest.json")


@pytest.mark.parametrize(
    "doc",
    [
        {"revision": "r1", "file": "/f.csv", "bytes": 0, "sha256": "0" * 64},
        {"revision": "r1", "file": "/f.csv", "bytes": -5, "sha256": "0" * 64},
        {"revision": "r1", "file": "/f.csv", "bytes": "5", "sha256": "0" * 64},
        {"revision": "r1", "file": "/f.csv", "bytes": 5, "sha256": "zz"},
        {"revision": "", "file": "/f.csv", "bytes": 5, "sha256": "0" * 64},
        {"revision": "bad revision!", "file": "/f.csv", "bytes": 5, "sha256": "0" * 64},
        {"revision": "r1", "file": "file:///etc/passwd", "bytes": 5, "sha256": "0" * 64},
        {"revision": "r1", "file": "javascript:alert(1)", "bytes": 5, "sha256": "0" * 64},
    ],
)
def test_parse_manifest_rejects_bad_documents(doc):
    with pytest.raises(ManifestError):
        parse_manifest(doc, "http://example.test/manifest.json")


def test_parse_manifest_resolves_relative_file():
    doc = {"revision": "r1", "file": "data/f.csv", "bytes": 10, "sha256": "AB" * 32}
    parsed = parse_manifest(doc, "http://example.test/dir/manifest.json")
    assert parsed["file_url"] == "http://example.test/dir/data/f.csv"
    assert parsed["sha256"] == "ab" * 32


# --------------------------------------------------------------------------
# CSV validation
# --------------------------------------------------------------------------
def _write(tmp_path, text, name="x.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_validate_csv_ok_with_bom_and_blank_line(tmp_path):
    path = _write(
        tmp_path,
        "﻿date,market,product,price\n"
        "2026-11-09,Beijing,cabbage,2.40\n"
        "\n"
        "2026-11-10,Shanghai,tomato,4.8\n",
    )
    summary = validate_csv(path, expected_day=DAY)
    assert summary.row_count == 2
    assert summary.day_min == "2026-11-09"
    assert summary.day_max == "2026-11-10"


@pytest.mark.parametrize(
    "text",
    [
        "date,market,product,price\n",  # header only
        "date,market,product\n2026-11-10,a,b\n",  # missing price column
        "date,market,product,price\n2026-11-10,a,b,-1\n",  # negative price
        "date,market,product,price\n2026-11-10,a,b,nan\n",
        "date,market,product,price\n2026-11-10,a,b,inf\n",
        "date,market,product,price\n2026-11-10,a,b,abc\n",
        "date,market,product,price\n2026-13-01,a,b,1\n",
        "date,market,product,price\n2026-11-10,,b,1\n",  # empty market
        "date,market,product,price\n2026-11-10,a,b,1,extra\n",  # ragged
    ],
)
def test_validate_csv_rejects_bad_files(tmp_path, text):
    path = _write(tmp_path, text)
    with pytest.raises(ValidationError):
        validate_csv(path, expected_day=DAY)


def test_validate_csv_rejects_wrong_day(tmp_path):
    path = _write(tmp_path, "date,market,product,price\n2026-11-01,a,b,1\n")
    with pytest.raises(ValidationError):
        validate_csv(path, expected_day=DAY)


# --------------------------------------------------------------------------
# Happy path and idempotence
# --------------------------------------------------------------------------
def test_collect_publishes_and_download_is_intact(tmp_path, sample):
    store = make_store(tmp_path)
    collector = Collector(store, str(tmp_path / "partials"))
    outcome = collector.collect(manifest_url(sample), DAY)

    row = outcome["revision"]
    assert row["revision"] == "2026-11-10-c"
    assert row["is_current"] == 1
    assert row["row_count"] == 12
    assert outcome["unchanged"] is False

    path = store.revision_path(row)
    data = open(path, "rb").read()
    assert len(data) == row["byte_count"]
    assert hashlib.sha256(data).hexdigest() == row["sha256"]


def test_collect_same_revision_is_unchanged_no_redownload(tmp_path, sample):
    store = make_store(tmp_path)
    collector = Collector(store, str(tmp_path / "partials"))
    first = collector.collect(manifest_url(sample), DAY)
    mtime = os.path.getmtime(store.revision_path(first["revision"]))
    time.sleep(0.01)
    second = collector.collect(manifest_url(sample), DAY)
    assert second["unchanged"] is True
    assert second["revision"]["id"] == first["revision"]["id"]
    assert os.path.getmtime(store.revision_path(second["revision"])) == mtime
    assert len(store.list_revisions(first["revision"]["source_id"])) == 1


# --------------------------------------------------------------------------
# Resume behaviour
# --------------------------------------------------------------------------
def test_interrupted_download_resumes_from_saved_bytes(tmp_path, sample):
    store = make_store(tmp_path)
    partials = tmp_path / "partials"
    collector = Collector(store, str(partials))

    sample.app.config["SAMPLE"].arm_interrupt(150)
    with pytest.raises(IncompleteDownload):
        collector.collect(manifest_url(sample), DAY)

    parts = list(partials.glob("*.part"))
    assert len(parts) == 1
    saved = parts[0].stat().st_size
    assert 0 < saved < 500
    assert store.list_revisions(store.find_source_by_url(manifest_url(sample))["id"]) == []

    outcome = collector.collect(manifest_url(sample), DAY)
    assert outcome["resumed_bytes"] == saved
    row = outcome["revision"]
    data = open(store.revision_path(row), "rb").read()
    assert hashlib.sha256(data).hexdigest() == row["sha256"]
    assert not list(partials.glob("*.part"))


def test_changed_revision_never_concatenates_old_partial(tmp_path, sample):
    store = make_store(tmp_path)
    partials = tmp_path / "partials"
    collector = Collector(store, str(partials))

    # First failure: a short read leaves a resumable partial for revision c.
    sample.app.config["SAMPLE"].arm_interrupt(120)
    with pytest.raises(IncompleteDownload):
        collector.collect(manifest_url(sample), DAY)
    assert len(list(partials.glob("*.part"))) == 1

    # Upstream now advertises a different revision with a new strong ETag.
    sample.app.config["SAMPLE"].switch("2026-11-10-a")
    outcome = collector.collect(manifest_url(sample), DAY)
    assert outcome["resumed_bytes"] == 0
    assert outcome["revision"]["revision"] == "2026-11-10-a"
    row = outcome["revision"]
    data = open(store.revision_path(row), "rb").read()
    assert hashlib.sha256(data).hexdigest() == row["sha256"]
    assert len(store.list_revisions(row["source_id"])) == 1
    # The stale partial for the other revision is gone.
    assert not list(partials.glob("*.part"))
    assert not list(partials.glob("*.json"))


def test_server_without_range_support_restarts_download(tmp_path, monkeypatch, sample):
    store = make_store(tmp_path)
    partials = tmp_path / "partials"
    Collector(store, str(partials))

    doc, final_url = fetch_manifest(manifest_url(sample))
    manifest = parse_manifest(doc, final_url)
    etag = '"%s"' % manifest["sha256"]

    # Seed a partial that would otherwise look resumable...
    part = partials / "x.part"
    part.write_bytes(b"x" * 50)
    (partials / "x.json").write_text(
        json.dumps(
            {
                "revision": manifest["revision"],
                "etag": etag,
                "bytes": manifest["bytes"],
                "sha256": manifest["sha256"],
                "file_url": manifest["file_url"],
                "received": 50,
            }
        )
    )
    # ...but the server no longer advertises Range support.
    monkeypatch.setattr(
        downloader, "probe", lambda url, timeout=20: (False, etag, manifest["bytes"])
    )
    result = downloader.download(manifest, str(partials), "x")
    assert result["resumed_bytes"] == 0
    data = open(result["path"], "rb").read()
    assert len(data) == manifest["bytes"]
    assert hashlib.sha256(data).hexdigest() == manifest["sha256"]


def test_etag_change_between_attempts_discards_partial(tmp_path, sample):
    store = make_store(tmp_path)
    partials = tmp_path / "partials"
    collector = Collector(store, str(partials))
    sample.app.config["SAMPLE"].arm_interrupt(140)
    with pytest.raises(IncompleteDownload):
        collector.collect(manifest_url(sample), DAY)

    # Same revision token and same byte count, but the payload/ETag changed
    # (bad upstream behaviour).  The collector must restart, not append.
    state = sample.app.config["SAMPLE"]
    name = state.current
    payload = state.revisions[name]["bytes"].replace(b"cabbage", b"CABBAGE")
    state.revisions[name] = {
        "bytes": payload,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "label": state.revisions[name]["label"],
    }
    outcome = collector.collect(manifest_url(sample), DAY)
    assert outcome["resumed_bytes"] == 0
    row = outcome["revision"]
    assert row["sha256"] == hashlib.sha256(payload).hexdigest()


# --------------------------------------------------------------------------
# Integrity failures leave the previous revision current
# --------------------------------------------------------------------------
def test_bad_manifest_leaves_previous_revision_downloadable(tmp_path, sample):
    store = make_store(tmp_path)
    collector = Collector(store, str(tmp_path / "partials"))
    first = collector.collect(manifest_url(sample), DAY)
    old_id = first["revision"]["id"]

    # Corrupt the advertised checksum by swapping in a mismatching digest.
    state = sample.app.config["SAMPLE"]
    name = state.current
    original = state.revisions[name]["sha256"]
    state.revisions[name]["sha256"] = "f" * 64
    with pytest.raises(SourceChanged):
        collector.collect(manifest_url(sample), DAY)
    state.revisions[name]["sha256"] = original

    current = store.current_revision(first["revision"]["source_id"])
    assert current["id"] == old_id
    assert os.path.exists(store.revision_path(current))


def test_csv_validation_failure_does_not_publish(tmp_path, sample):
    store = make_store(tmp_path)
    collector = Collector(store, str(tmp_path / "partials"))
    state = sample.app.config["SAMPLE"]
    name = state.current
    bad = b"date,market,product,price\n2026-11-10,a,b,-3\n"
    state.revisions[name] = {
        "bytes": bad,
        "sha256": hashlib.sha256(bad).hexdigest(),
        "label": "bad",
    }
    with pytest.raises(ValidationError):
        collector.collect(manifest_url(sample), DAY)
    source = store.find_source_by_url(manifest_url(sample))
    assert store.current_revision(source["id"]) is None
    assert not list((tmp_path / "files").iterdir())
    # A rejected download is discarded; a later corrected revision must not
    # inherit any of its bytes.
    state.revisions[name] = {
        "bytes": b"date,market,product,price\n2026-11-10,a,b,3\n",
        "sha256": hashlib.sha256(b"date,market,product,price\n2026-11-10,a,b,3\n").hexdigest(),
        "label": "fixed",
    }
    fixed = collector.collect(manifest_url(sample), DAY)
    assert fixed["resumed_bytes"] == 0


def test_publish_failure_keeps_old_file_and_hides_new(tmp_path, sample, monkeypatch):
    from errors import StorageError

    store = make_store(tmp_path)
    collector = Collector(store, str(tmp_path / "partials"))
    first = collector.collect(manifest_url(sample), DAY)
    old = first["revision"]

    def boom(**kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(store, "publish_revision", boom)
    sample.app.config["SAMPLE"].switch("2026-11-10-a")
    with pytest.raises(StorageError):
        collector.collect(manifest_url(sample), DAY)

    current = store.current_revision(old["source_id"])
    assert current["id"] == old["id"]
    assert os.path.exists(store.revision_path(current))
    # Only the previously published file is present in files/.
    names = [n for n in os.listdir(tmp_path / "files")]
    assert names == [old["stored_name"]]

    # The retry does not need to download again: the verified partial is reused
    # and only the publication is attempted.  The old revision stays current.
    monkeypatch.undo()
    retry = collector.collect(manifest_url(sample), DAY)
    assert retry["revision"]["revision"] == "2026-11-10-a"
    assert store.current_revision(old["source_id"])["revision"] == "2026-11-10-a"
    assert len(store.list_revisions(old["source_id"])) == 2
    assert os.path.exists(store.revision_path(old))


def test_partial_is_invisible_until_published(tmp_path, sample):
    store = make_store(tmp_path)
    partials = tmp_path / "partials"
    collector = Collector(store, str(partials))
    sample.app.config["SAMPLE"].arm_interrupt(100)
    with pytest.raises(IncompleteDownload):
        collector.collect(manifest_url(sample), DAY)
    assert os.listdir(tmp_path / "files") == []
    source = store.find_source_by_url(manifest_url(sample))
    assert store.current_revision(source["id"]) is None


# --------------------------------------------------------------------------
# Revision switching
# --------------------------------------------------------------------------
def test_switch_back_to_earlier_revision(tmp_path, sample):
    store = make_store(tmp_path)
    collector = Collector(store, str(tmp_path / "partials"))
    collector.collect(manifest_url(sample), DAY)  # newest revision (c)
    sample.app.config["SAMPLE"].switch("2026-11-10-a")
    second = collector.collect(manifest_url(sample), DAY)  # revision a
    source_id = second["revision"]["source_id"]
    revisions = store.list_revisions(source_id)
    assert len(revisions) == 2
    current = store.current_revision(source_id)
    assert current["revision"] == "2026-11-10-a"

    newest = next(r for r in revisions if r["revision"] == "2026-11-10-c")
    row = store.switch_current(source_id, newest["id"])
    assert row["revision"] == "2026-11-10-c"
    assert store.current_revision(source_id)["id"] == newest["id"]
    current_flags = [r["is_current"] for r in store.list_revisions(source_id)]
    assert sum(current_flags) == 1


def test_concurrent_collection_is_rejected(tmp_path, sample):
    store = make_store(tmp_path)
    collector = Collector(store, str(tmp_path / "partials"))
    with collector._source_lock(manifest_url(sample)):
        with pytest.raises(BusyError):
            Collector(store, str(tmp_path / "partials")).collect(manifest_url(sample), DAY)


def test_restart_with_new_process_resumes_saved_partial(tmp_path, sample):
    store = make_store(tmp_path)
    partials = tmp_path / "partials"
    sample.app.config["SAMPLE"].arm_interrupt(180)
    with pytest.raises(IncompleteDownload):
        Collector(store, str(partials)).collect(manifest_url(sample), DAY)
    saved = next(partials.glob("*.part")).stat().st_size
    assert saved == 180

    # A fresh Store + Collector stands in for a restarted process; the same
    # data volume is reused.
    restarted = Collector(
        Store(str(tmp_path / "db.sqlite3"), str(tmp_path / "files")), str(partials)
    )
    outcome = restarted.collect(manifest_url(sample), DAY)
    assert outcome["resumed_bytes"] == saved
    row = outcome["revision"]
    data = open(restarted.store.revision_path(row), "rb").read()
    assert hashlib.sha256(data).hexdigest() == row["sha256"]


def test_partial_larger_than_checkpoint_is_truncated_and_resumed(tmp_path, sample):
    store = make_store(tmp_path)
    partials = tmp_path / "partials"
    collector = Collector(store, str(partials))
    sample.app.config["SAMPLE"].arm_interrupt(100)
    with pytest.raises(IncompleteDownload):
        collector.collect(manifest_url(sample), DAY)

    # Simulate a crash after more bytes reached disk than the sidecar knows.
    part = next(partials.glob("*.part"))
    with open(part, "ab") as fh:
        fh.write(b"GARBAGE-FROM-AN-UNFINISHED-WRITE")
    padding = len(b"GARBAGE-FROM-AN-UNFINISHED-WRITE")

    outcome = collector.collect(manifest_url(sample), DAY)
    assert outcome["resumed_bytes"] == 100
    row = outcome["revision"]
    data = open(store.revision_path(row), "rb").read()
    assert len(data) == row["byte_count"]
    assert b"GARBAGE" not in data
    assert hashlib.sha256(data).hexdigest() == row["sha256"]
    assert padding > 0
