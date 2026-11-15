"""Resumable, revision-safe download of one CSV revision.

Resume rules
------------
A partial download may only be continued when *all* of the following hold:

1. the sidecar state was written for the **same revision** and the same
   manifest-provided byte count / SHA-256;
2. the server advertises a **strong ETag** (``"..."``, not ``W/"..."``) and
   the value is identical to the one recorded when the partial was created;
3. the server supports Range requests and answers the resumed request with
   ``206 Partial Content`` whose ``Content-Range`` starts exactly at the number
   of bytes already stored.

If any of these is false the partial file is discarded and the revision is
downloaded again in full.  Two revisions are never concatenated, and a partial
is never promoted to "complete" without byte-count and SHA-256 verification.
"""

import hashlib
import http.client
import json
import os
import re
import urllib.error
import urllib.request

from errors import IncompleteDownload, SourceChanged, UpstreamError
from manifest import is_strong_etag
from store import sha256_file
from transport import USER_AGENT, opener

CHUNK = 256 * 1024
# The sidecar is fsynced periodically rather than after every chunk; on resume
# the partial file is truncated back to the last recorded offset, so bytes
# written after the last checkpoint are simply fetched again.
STATE_INTERVAL = 1024 * 1024
_CONTENT_RANGE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)


class PartialState:
    """Sidecar metadata describing an unfinished download."""

    def __init__(self, *, revision, etag, bytes_expected, sha256, file_url, received):
        self.revision = revision
        self.etag = etag
        self.bytes_expected = bytes_expected
        self.sha256 = sha256
        self.file_url = file_url
        self.received = received

    def to_json(self):
        return {
            "revision": self.revision,
            "etag": self.etag,
            "bytes": self.bytes_expected,
            "sha256": self.sha256,
            "file_url": self.file_url,
            "received": self.received,
        }

    @classmethod
    def from_json(cls, doc):
        if not isinstance(doc, dict):
            return None
        try:
            return cls(
                revision=doc["revision"],
                etag=doc["etag"],
                bytes_expected=int(doc["bytes"]),
                sha256=doc["sha256"],
                file_url=doc["file_url"],
                received=int(doc["received"]),
            )
        except (KeyError, TypeError, ValueError):
            return None


def partial_paths(partial_dir, key):
    base = os.path.join(partial_dir, key)
    return base + ".part", base + ".json"


def _read_state(state_path):
    try:
        with open(state_path, "r", encoding="utf-8") as fh:
            return PartialState.from_json(json.load(fh))
    except (OSError, ValueError):
        return None


def _write_state(state_path, state):
    tmp = state_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state.to_json(), fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, state_path)


def _discard(part_path, state_path):
    for path in (part_path, state_path):
        try:
            os.unlink(path)
        except OSError:
            pass


def _head(url, timeout):
    req = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    )
    try:
        with opener().open(req, timeout=timeout) as resp:
            return resp.status, resp.headers
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers or {}
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise UpstreamError("HEAD request failed: %s" % exc, code="upstream_unreachable") from exc


def probe(url, timeout=20):
    """Return ``(supports_range, etag, length)`` for a data file."""
    status, headers = _head(url, timeout)
    if status == 200:
        accept = (headers.get("Accept-Ranges") or "").lower()
        etag = headers.get("ETag")
        try:
            length = int(headers.get("Content-Length")) if headers.get("Content-Length") else None
        except ValueError:
            length = None
        return accept == "bytes", etag, length
    if status in (403, 405, 501):
        # HEAD not allowed: we cannot learn the validator safely, so no resume.
        return False, None, None
    if status == 404:
        raise UpstreamError("data file not found (HTTP 404)", code="file_not_found")
    raise UpstreamError("data file probe returned HTTP %s" % status, code="probe_failed")


def _response_meta(resp):
    """Extract ETag and declared Content-Length from a GET response."""
    etag = resp.headers.get("ETag")
    declared = resp.headers.get("Content-Length")
    try:
        declared_len = int(declared) if declared is not None else None
    except ValueError:
        declared_len = None
    return etag, declared_len


def download(manifest, partial_dir, key, *, timeout=30, progress=None):
    """Download (or resume) ``manifest['file_url']`` into a partial file.

    Returns a dict with ``path``, ``resumed_bytes`` and ``etag``.  The returned
    file has the expected byte count and SHA-256; otherwise an exception is
    raised and the partial is removed.
    """
    os.makedirs(partial_dir, exist_ok=True)
    part_path, state_path = partial_paths(partial_dir, key)
    revision = manifest["revision"]
    expected = manifest["bytes"]
    expected_sha = manifest["sha256"]
    file_url = manifest["file_url"]

    supports_range, etag, head_len = probe(file_url, timeout=timeout)
    if head_len is not None and head_len != expected:
        # The representation the server would serve is not the one the
        # manifest describes.  Any stored partial belongs to a different
        # body, so drop it rather than looping on an unusable resume.
        _discard(part_path, state_path)
        raise SourceChanged(
            "server reports %d bytes, manifest declares %d" % (head_len, expected),
            code="length_mismatch",
        )

    state = _read_state(state_path)
    part_size = os.path.getsize(part_path) if os.path.exists(part_path) else -1

    # A previous run may have downloaded the whole body but failed to publish
    # it (e.g. a database error).  The bytes are still verified below rather
    # than trusted, so reusing them is as safe as fetching them again.
    if (
        state is not None
        and part_size >= expected
        and state.revision == revision
        and state.file_url == file_url
        and state.bytes_expected == expected
        and state.sha256 == expected_sha
        and state.received == expected
    ):
        if part_size > expected:
            with open(part_path, "r+b") as fh:
                fh.truncate(expected)
                fh.flush()
                os.fsync(fh.fileno())
        if sha256_file(part_path) == expected_sha:
            return {"path": part_path, "resumed_bytes": 0, "etag": state.etag, "reused": True}

    resumable = (
        state is not None
        and part_size >= 0
        and state.revision == revision
        and state.file_url == file_url
        and state.bytes_expected == expected
        and state.sha256 == expected_sha
        and is_strong_etag(state.etag)
        and state.etag == etag
        and supports_range
        and 0 < state.received < expected
        # The file may hold a few bytes written after the last checkpoint; the
        # offset recorded in the sidecar is authoritative, and the file is
        # truncated back to it below.
        and part_size >= state.received
    )

    start = 0
    if resumable:
        start = state.received
        resumed_bytes = start
        if part_size > start:
            with open(part_path, "r+b") as fh:
                fh.truncate(start)
                fh.flush()
                os.fsync(fh.fileno())
    else:
        # Any partial state belongs to something we can no longer trust:
        # another revision, another validator, or a server that cannot resume.
        _discard(part_path, state_path)
        resumed_bytes = 0

    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    if start:
        headers["Range"] = "bytes=%d-" % start
        headers["If-Range"] = etag

    req = urllib.request.Request(file_url, method="GET", headers=headers)
    try:
        resp = opener().open(req, timeout=timeout)
    except urllib.error.HTTPError as exc:
        if exc.code == 416 and start:
            # Stored partial is not usable against this representation.
            _discard(part_path, state_path)
            return download(manifest, partial_dir, key, timeout=timeout, progress=progress)
        if exc.code == 404:
            raise UpstreamError("data file not found (HTTP 404)", code="file_not_found") from exc
        raise UpstreamError(
            "data file request failed with HTTP %s" % exc.code, code="download_failed"
        ) from exc
    except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as exc:
        raise UpstreamError("cannot reach data file: %s" % exc, code="upstream_unreachable") from exc

    with resp:
        status = resp.status
        resp_etag, resp_len = _response_meta(resp)
        if start and status == 206:
            # The response must be a continuation of exactly our byte range.
            content_range = resp.headers.get("Content-Range", "")
            match = _CONTENT_RANGE.match(content_range.strip())
            if not match:
                _discard(part_path, state_path)
                raise IncompleteDownload("server sent 206 without a usable Content-Range")
            cr_start, cr_total = int(match.group(1)), match.group(3)
            if cr_start != start or (cr_total != "*" and int(cr_total) != expected):
                _discard(part_path, state_path)
                raise SourceChanged(
                    "server resumed at byte %d of %s, expected %d of %d"
                    % (cr_start, cr_total, start, expected),
                    code="resume_mismatch",
                )
            if is_strong_etag(resp_etag) and resp_etag != etag:
                _discard(part_path, state_path)
                raise SourceChanged(
                    "ETag changed during download; partial discarded", code="etag_changed"
                )
            remaining = expected - start
            if resp_len is not None and resp_len != remaining:
                _discard(part_path, state_path)
                raise IncompleteDownload(
                    "server announced %d remaining bytes, expected %d" % (resp_len, remaining),
                    code="length_mismatch",
                )
            mode = "ab"
            received = start
        else:
            # Either we asked for the whole file, or the server ignored Range /
            # If-Range and sent a fresh 200.  In both cases start over.
            if start and status == 200:
                _discard(part_path, state_path)
                resumed_bytes = 0
            elif status not in (200, 206):
                _discard(part_path, state_path)
                raise UpstreamError(
                    "data file request returned HTTP %s" % status, code="download_failed"
                )
            if status == 206 and not start:
                _discard(part_path, state_path)
                raise UpstreamError(
                    "server sent an unsolicited partial response", code="download_failed"
                )
            if resp_len is not None and resp_len != expected:
                _discard(part_path, state_path)
                raise IncompleteDownload(
                    "server announced %d bytes, manifest says %d" % (resp_len, expected),
                    code="length_mismatch",
                )
            if not is_strong_etag(resp_etag) or resp_etag != etag:
                # Without a stable strong validator we cannot ever resume this
                # body safely; record None so the next attempt restarts.
                etag = resp_etag if is_strong_etag(resp_etag) else None
            mode = "wb"
            received = 0
            if os.path.exists(state_path):
                try:
                    os.unlink(state_path)
                except OSError:
                    pass

        _write_state(
            state_path,
            PartialState(
                revision=revision,
                etag=etag,
                bytes_expected=expected,
                sha256=expected_sha,
                file_url=file_url,
                received=received,
            ),
        )

        digest = hashlib.sha256()
        if mode == "ab" and os.path.exists(part_path):
            with open(part_path, "rb") as existing:
                while True:
                    block = existing.read(CHUNK)
                    if not block:
                        break
                    digest.update(block)

        def checkpoint(offset):
            _write_state(
                state_path,
                PartialState(
                    revision=revision,
                    etag=etag,
                    bytes_expected=expected,
                    sha256=expected_sha,
                    file_url=file_url,
                    received=offset,
                ),
            )

        try:
            with open(part_path, mode) as fh:
                last_checkpoint = received
                try:
                    while True:
                        block = resp.read(CHUNK)
                        if not block:
                            break
                        fh.write(block)
                        digest.update(block)
                        received += len(block)
                        if received > expected:
                            _discard(part_path, state_path)
                            raise IncompleteDownload(
                                "server sent more bytes than the manifest declares",
                                code="length_mismatch",
                            )
                        if received - last_checkpoint >= STATE_INTERVAL:
                            fh.flush()
                            os.fsync(fh.fileno())
                            checkpoint(received)
                            last_checkpoint = received
                        if progress:
                            progress(received, expected)
                except (urllib.error.URLError, http.client.HTTPException, OSError) as exc:
                    # Persist what actually reached disk and record that exact
                    # offset, so the next attempt resumes rather than restarts.
                    try:
                        fh.flush()
                        os.fsync(fh.fileno())
                        disk_size = os.path.getsize(part_path)
                        if disk_size != received:
                            with open(part_path, "rb+") as trim:
                                trim.truncate(received)
                                trim.flush()
                                os.fsync(trim.fileno())
                        checkpoint(received)
                    except OSError:
                        _discard(part_path, state_path)
                    raise IncompleteDownload(
                        "download interrupted after %d of %d bytes: %s"
                        % (received, expected, exc),
                        code="interrupted",
                    ) from exc
                fh.flush()
                os.fsync(fh.fileno())
                checkpoint(received)
        except (urllib.error.URLError, http.client.HTTPException, OSError) as exc:
            # e.g. the partial file could not be opened or flushed at all.
            raise IncompleteDownload(
                "download failed after %d of %d bytes: %s" % (received, expected, exc),
                code="interrupted",
            ) from exc

    if received != expected:
        if received < expected:
            # Short EOF: keep the partial and its sidecar so a retry can
            # resume, provided the validator still matches next time.
            raise IncompleteDownload(
                "download ended after %d of %d bytes" % (received, expected),
                code="length_mismatch",
            )
        _discard(part_path, state_path)
        raise IncompleteDownload(
            "server sent %d bytes, manifest declares %d" % (received, expected),
            code="length_mismatch",
        )

    actual_sha = digest.hexdigest()
    if actual_sha != expected_sha:
        _discard(part_path, state_path)
        # The body was complete but does not match what the manifest promised.
        # Retrying the same URL unchanged will not help, so this is surfaced as
        # an upstream/source inconsistency rather than a transient short read.
        raise SourceChanged(
            "SHA-256 mismatch (got %s, manifest says %s)" % (actual_sha, expected_sha),
            code="checksum_mismatch",
        )

    return {"path": part_path, "resumed_bytes": resumed_bytes, "etag": etag, "reused": False}
