"""SQLite persistence for collected agricultural wholesale price files.

Design notes
------------
* A *revision* is the unit the warehouse loads.  All revisions of one source
  are kept side by side so an operator can switch back to an earlier one.
* ``is_current`` marks exactly one published revision per source.  The partial
  unique index in the schema enforces this at the database level rather than
  relying on application care.
* Publishing is transactional: the new row is inserted and the old row's
  ``is_current`` is cleared in the same transaction.  If anything fails, the
  previous revision keeps its flag and its stored file, so downloads never
  break.
* Partial downloads never live in the same directory as published files.  A
  file only moves into ``files/`` after byte-count, SHA-256 and row-date checks
  pass.
"""

import contextlib
import datetime as _dt
import hashlib
import os
import sqlite3
import threading
import uuid

from errors import NotFound

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    manifest_url  TEXT    NOT NULL UNIQUE,
    label         TEXT    NOT NULL DEFAULT '',
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS revisions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id      INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    revision       TEXT    NOT NULL,
    etag           TEXT,
    file_url       TEXT    NOT NULL,
    byte_count     INTEGER NOT NULL,
    sha256         TEXT    NOT NULL,
    stored_name    TEXT    NOT NULL,
    day_min        TEXT,
    day_max        TEXT,
    row_count      INTEGER NOT NULL DEFAULT 0,
    is_current     INTEGER NOT NULL DEFAULT 0 CHECK (is_current IN (0, 1)),
    downloaded_at  TEXT    NOT NULL,
    published_at   TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_revisions_source_revision
    ON revisions(source_id, revision);

CREATE UNIQUE INDEX IF NOT EXISTS ux_revisions_one_current
    ON revisions(source_id) WHERE is_current = 1;

CREATE INDEX IF NOT EXISTS ix_revisions_source
    ON revisions(source_id, id DESC);

CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id     INTEGER REFERENCES sources(id) ON DELETE SET NULL,
    manifest_url  TEXT    NOT NULL,
    day           TEXT,
    status        TEXT    NOT NULL,
    message       TEXT    NOT NULL DEFAULT '',
    revision      TEXT,
    resumed_bytes INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_jobs_source ON jobs(source_id, id DESC);
"""


def utcnow():
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()


def _connect(path):
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


class Store:
    def __init__(self, db_path, files_dir):
        self.db_path = os.path.abspath(db_path)
        self.files_dir = os.path.abspath(files_dir)
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        os.makedirs(self.files_dir, exist_ok=True)
        self._init_lock = threading.Lock()
        self._initialised = False

    # -- schema -----------------------------------------------------------
    def init(self):
        with self._init_lock:
            if self._initialised:
                return
            conn = _connect(self.db_path)
            try:
                conn.executescript(SCHEMA)
            finally:
                conn.close()
            self._initialised = True

    def connect(self):
        self.init()
        return _connect(self.db_path)

    @contextlib.contextmanager
    def session(self):
        """Short-lived connection that is always closed.

        ``sqlite3.Connection`` used as a context manager commits but does not
        close, which leaks file descriptors in a long-running web process, so
        every non-transactional query goes through this wrapper instead.
        """
        conn = self.connect()
        try:
            yield conn
            if conn.in_transaction:
                conn.commit()
        except BaseException:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    # -- sources ----------------------------------------------------------
    def upsert_source(self, manifest_url, label=""):
        now = utcnow()
        with self.session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT id, label FROM sources WHERE manifest_url = ?", (manifest_url,)
                ).fetchone()
                if row is None:
                    cur = conn.execute(
                        "INSERT INTO sources(manifest_url, label, created_at, updated_at)"
                        " VALUES (?, ?, ?, ?)",
                        (manifest_url, label, now, now),
                    )
                    source_id = cur.lastrowid
                else:
                    source_id = row["id"]
                    if label and label != row["label"]:
                        conn.execute(
                            "UPDATE sources SET label = ?, updated_at = ? WHERE id = ?",
                            (label, now, source_id),
                        )
                conn.execute("COMMIT")
                return source_id
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def get_source(self, source_id):
        with self.session() as conn:
            return conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()

    def find_source_by_url(self, manifest_url):
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM sources WHERE manifest_url = ?", (manifest_url,)
            ).fetchone()

    def list_sources(self):
        with self.session() as conn:
            return conn.execute(
                """
                SELECT s.*,
                       (SELECT COUNT(*) FROM revisions r WHERE r.source_id = s.id)
                           AS revision_count,
                       (SELECT revision FROM revisions r
                         WHERE r.source_id = s.id AND r.is_current = 1) AS current_revision
                  FROM sources s
                 ORDER BY s.id DESC
                """
            ).fetchall()

    # -- revisions --------------------------------------------------------
    def find_revision(self, source_id, revision):
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM revisions WHERE source_id = ? AND revision = ?",
                (source_id, revision),
            ).fetchone()

    def list_revisions(self, source_id):
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM revisions WHERE source_id = ? ORDER BY id DESC",
                (source_id,),
            ).fetchall()

    def get_revision(self, revision_id):
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM revisions WHERE id = ?", (revision_id,)
            ).fetchone()

    def current_revision(self, source_id):
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM revisions WHERE source_id = ? AND is_current = 1",
                (source_id,),
            ).fetchone()

    def revision_path(self, row):
        # stored_name is generated by us (uuid4 + ".csv"), never taken from
        # upstream, so it cannot escape files_dir.
        return os.path.join(self.files_dir, row["stored_name"])

    def publish_revision(
        self,
        *,
        source_id,
        revision,
        etag,
        file_url,
        byte_count,
        sha256,
        temp_path,
        day_min,
        day_max,
        row_count,
    ):
        """Move a validated file into place and make it the current revision.

        The on-disk rename happens before the transaction, but under a name in
        ``files/`` that is not referenced by any row.  If the transaction then
        fails the file is moved back to ``temp_path`` (or unlinked) and the
        previous current revision is untouched.  Readers never see the new file
        until it is committed.
        """
        now = utcnow()
        stored_name = uuid.uuid4().hex + ".csv"
        final_path = os.path.join(self.files_dir, stored_name)

        # Rename first so a crash between rename and COMMIT leaves an
        # unreferenced file, never a row pointing at a missing file.
        os.replace(temp_path, final_path)
        try:
            dir_fd = os.open(self.files_dir, os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

        conn = _connect(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT id, stored_name FROM revisions WHERE source_id = ? AND revision = ?",
                (source_id, revision),
            ).fetchone()
            if existing is not None:
                old_path = os.path.join(self.files_dir, existing["stored_name"])
                row_id = existing["id"]
                conn.execute(
                    """
                    UPDATE revisions
                       SET etag = ?, file_url = ?, byte_count = ?, sha256 = ?,
                           stored_name = ?, day_min = ?, day_max = ?,
                           row_count = ?, downloaded_at = ?, published_at = ?
                     WHERE id = ?
                    """,
                    (
                        etag, file_url, byte_count, sha256, stored_name,
                        day_min, day_max, row_count, now, now, row_id,
                    ),
                )
            else:
                old_path = None
                cur = conn.execute(
                    """
                    INSERT INTO revisions(
                        source_id, revision, etag, file_url, byte_count, sha256,
                        stored_name, day_min, day_max, row_count, is_current,
                        downloaded_at, published_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                    """,
                    (
                        source_id, revision, etag, file_url, byte_count, sha256,
                        stored_name, day_min, day_max, row_count, now, now,
                    ),
                )
                row_id = cur.lastrowid
            conn.execute(
                "UPDATE revisions SET is_current = 0 WHERE source_id = ? AND id != ?",
                (source_id, row_id),
            )
            conn.execute(
                "UPDATE revisions SET is_current = 1 WHERE id = ?", (row_id,)
            )
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            conn.close()
            # Put the verified file back where the collector found it so a
            # retry can reuse it instead of downloading again.  If that move
            # fails, drop it: an unreferenced file must not linger in files/.
            try:
                os.replace(final_path, temp_path)
            except OSError:
                _safe_unlink(final_path)
            raise
        conn.close()

        if old_path and os.path.abspath(old_path) != os.path.abspath(final_path):
            _safe_unlink(old_path)
        return self.get_revision(row_id)

    def switch_current(self, source_id, revision_id):
        """Point ``is_current`` at an already-published revision.

        ``published_at`` is left alone: it records when the revision was first
        stored, not when it was last selected for loading.
        """
        with self.session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT id FROM revisions WHERE id = ? AND source_id = ?",
                    (revision_id, source_id),
                ).fetchone()
                if row is None:
                    raise NotFound("revision not found")
                conn.execute(
                    "UPDATE revisions SET is_current = 0 WHERE source_id = ?",
                    (source_id,),
                )
                conn.execute(
                    "UPDATE revisions SET is_current = 1 WHERE id = ?",
                    (revision_id,),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            return conn.execute(
                "SELECT * FROM revisions WHERE id = ?", (revision_id,)
            ).fetchone()

    # -- jobs (audit trail shown in the UI) -------------------------------
    def create_job(self, manifest_url, day, source_id=None):
        now = utcnow()
        with self.session() as conn:
            cur = conn.execute(
                "INSERT INTO jobs(source_id, manifest_url, day, status, created_at, updated_at)"
                " VALUES (?, ?, ?, 'running', ?, ?)",
                (source_id, manifest_url, day, now, now),
            )
            return cur.lastrowid

    def update_job(self, job_id, *, status, message="", revision=None, resumed_bytes=None,
                   source_id=None):
        now = utcnow()
        with self.session() as conn:
            conn.execute(
                """
                UPDATE jobs SET status = ?, message = ?, revision = COALESCE(?, revision),
                       resumed_bytes = COALESCE(?, resumed_bytes), updated_at = ?,
                       source_id = COALESCE(?, source_id)
                 WHERE id = ?
                """,
                (status, message, revision, resumed_bytes, now, source_id, job_id),
            )

    def recent_jobs(self, limit=25):
        with self.session() as conn:
            return conn.execute(
                "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()


def _safe_unlink(path):
    try:
        os.unlink(path)
    except OSError:
        pass


def sha256_file(path, chunk=1024 * 1024):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            block = fh.read(chunk)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()
