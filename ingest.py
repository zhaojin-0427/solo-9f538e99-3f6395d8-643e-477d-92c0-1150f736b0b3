"""End-to-end collection pipeline.

``collect`` is the single entry point used by the web UI and the CLI:

    manifest -> probe -> resumable download -> byte/SHA-256/row checks
             -> atomic publish (old revision stays current on any failure)

Concurrency is controlled by a per-source advisory file lock, so a second
collector (another request thread or another process sharing the data volume)
gets a clear 409 instead of interleaving writes to the same partial file.
"""

import contextlib
import fcntl
import hashlib
import os
import re

from downloader import download, partial_paths
from errors import BusyError, CollectError, SourceChanged, StorageError
from manifest import parse_manifest, validate_csv
from store import sha256_file
from transport import fetch_manifest

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def source_key(manifest_url):
    return hashlib.sha256(manifest_url.encode("utf-8")).hexdigest()[:16]


def partial_key(manifest_url, revision):
    return "%s-%s" % (source_key(manifest_url), _SAFE.sub("_", revision)[:80])


class Collector:
    def __init__(self, store, partial_dir, lock_dir=None, logger=None):
        self.store = store
        self.partial_dir = os.path.abspath(partial_dir)
        self.lock_dir = os.path.abspath(lock_dir or os.path.join(self.partial_dir, "locks"))
        os.makedirs(self.partial_dir, exist_ok=True)
        os.makedirs(self.lock_dir, exist_ok=True)
        self.logger = logger

    # -- locking ----------------------------------------------------------
    @contextlib.contextmanager
    def _source_lock(self, manifest_url):
        path = os.path.join(self.lock_dir, source_key(manifest_url) + ".lock")
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise BusyError("another collection for this source is running")
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    # -- housekeeping -----------------------------------------------------
    def _cleanup_stale_partials(self, manifest_url, keep_key):
        """Remove partials for the same source that belong to other revisions.

        A partial for a different revision can never be resumed (the downloader
        enforces this too); deleting it keeps the volume bounded.  Partials for
        the key we are about to use are always kept so a restart can resume.
        """
        prefix = source_key(manifest_url) + "-"
        try:
            names = os.listdir(self.partial_dir)
        except OSError:
            return
        for name in names:
            if not name.startswith(prefix):
                continue
            if name == keep_key + ".part" or name == keep_key + ".json":
                continue
            if not (name.endswith(".part") or name.endswith(".json")):
                continue
            # Partial files for another revision of the same source can never
            # be resumed and only consume disk; drop them on every new run.
            try:
                os.unlink(os.path.join(self.partial_dir, name))
            except OSError:
                pass

    def recover_jobs(self):
        """Mark jobs left ``running`` by a crash as interrupted."""
        try:
            with self.store.session() as conn:
                conn.execute(
                    "UPDATE jobs SET status = 'interrupted',"
                    " message = 'process stopped before completion; retry will resume'"
                    " WHERE status = 'running'"
                )
        except Exception:
            pass

    # -- main pipeline ----------------------------------------------------
    def collect(self, manifest_url, day, job_id=None):
        """Collect the currently advertised revision.

        Returns the published revision row plus ``resumed_bytes``.  Raises a
        :class:`CollectError` subclass on any failure; in that case the
        previously published revision is still current and downloadable.
        """
        with self._source_lock(manifest_url):
            doc, final_url = fetch_manifest(manifest_url)
            manifest = parse_manifest(doc, final_url)

            source = self.store.upsert_source(manifest_url)
            source_id = source if isinstance(source, int) else source["id"]

            existing = self.store.find_revision(source_id, manifest["revision"])
            if existing is not None:
                path = self.store.revision_path(existing)
                if (
                    os.path.exists(path)
                    and os.path.getsize(path) == existing["byte_count"] == manifest["bytes"]
                    and existing["sha256"] == manifest["sha256"]
                    and sha256_file(path) == manifest["sha256"]
                ):
                    if not existing["is_current"]:
                        existing = self.store.switch_current(source_id, existing["id"])
                    return {
                        "revision": existing,
                        "resumed_bytes": 0,
                        "unchanged": True,
                        "manifest": manifest,
                    }

            key = partial_key(manifest_url, manifest["revision"])
            self._cleanup_stale_partials(manifest_url, key)

            result = download(manifest, self.partial_dir, key)
            temp_path = result["path"]

            # Byte count is enforced by the downloader; verify again from disk
            # right before validation so a truncated file cannot slip through.
            try:
                size = os.path.getsize(temp_path)
            except OSError as exc:
                raise StorageError("partial file disappeared: %s" % exc) from exc
            if size != manifest["bytes"]:
                raise SourceChanged(
                    "downloaded %d bytes, manifest declares %d" % (size, manifest["bytes"]),
                    code="length_mismatch",
                )
            actual_sha = sha256_file(temp_path)
            if actual_sha != manifest["sha256"]:
                raise SourceChanged(
                    "downloaded file SHA-256 %s does not match manifest %s"
                    % (actual_sha, manifest["sha256"]),
                    code="checksum_mismatch",
                )

            summary = validate_csv(temp_path, expected_day=day)

            try:
                row = self.store.publish_revision(
                    source_id=source_id,
                    revision=manifest["revision"],
                    etag=result["etag"],
                    file_url=manifest["file_url"],
                    byte_count=manifest["bytes"],
                    sha256=manifest["sha256"],
                    temp_path=temp_path,
                    day_min=summary.day_min,
                    day_max=summary.day_max,
                    row_count=summary.row_count,
                )
            except CollectError:
                raise
            except Exception as exc:
                raise StorageError("could not publish revision: %s" % exc) from exc

            # Publish moved the file; a leftover sidecar is no longer needed.
            _remove_sidecar(self.partial_dir, key)

            return {
                "revision": row,
                "resumed_bytes": result["resumed_bytes"],
                "unchanged": False,
                "manifest": manifest,
            }


def _remove_sidecar(partial_dir, key):
    _, state_path = partial_paths(partial_dir, key)
    try:
        os.unlink(state_path)
    except OSError:
        pass
