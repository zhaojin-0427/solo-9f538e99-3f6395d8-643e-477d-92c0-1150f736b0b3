"""Flask web front-end for the agriculture wholesale price collector.

Pages
-----
``/``                     enter a manifest URL and a business date, see sources
``/collect`` (POST)        run one collection and return to the source page
``/sources/<id>``          revisions of one source, switch current revision,
                           download the currently published CSV
``/download/<revision>``   stream a published file (never a partial)
``/api/collect`` (POST)    JSON API used by scripts/tests

A collection is synchronous: the page reports exactly what happened (downloaded
from scratch, resumed at byte N, or "revision unchanged").  Downloads in
progress never appear in the source page; only committed revisions do.
"""

import datetime as _dt
import hashlib
import logging
import os

from flask import (
    Flask,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from werkzeug.exceptions import HTTPException

from errors import CollectError, InputError, NotFound
from ingest import Collector
from manifest import parse_day
from store import Store
from transport import validate_source_url

log = logging.getLogger("collector")


def _csv_download_name(row):
    day = row["day_max"] or "csv"
    return "agri-prices-%s-rev-%s.csv" % (day, row["revision"])


def create_app(store=None):
    base = os.environ.get("COLLECTOR_DATA_DIR", os.path.join(os.getcwd(), "data"))
    db_path = os.environ.get("COLLECTOR_DB", os.path.join(base, "collector.sqlite3"))
    files_dir = os.environ.get("COLLECTOR_FILES", os.path.join(base, "files"))
    partial_dir = os.environ.get("COLLECTOR_PARTIALS", os.path.join(base, "partials"))

    app = Flask(__name__)
    app.config.update(
        MAX_CONTENT_LENGTH=64 * 1024,
        SECRET_KEY=os.environ.get("COLLECTOR_SECRET", os.urandom(24).hex()),
        JSON_AS_ASCII=False,
    )
    app.jinja_env.trim_blocks = True
    app.jinja_env.lstrip_blocks = True

    the_store = store or Store(db_path, files_dir)
    the_store.init()
    collector = Collector(the_store, partial_dir)
    collector.recover_jobs()
    app.extensions["collector"] = collector
    app.extensions["store"] = the_store

    # -- helpers ---------------------------------------------------------
    def error_response(exc):
        status = getattr(exc, "status", 500)
        code = getattr(exc, "code", "internal_error")
        if request.path.startswith("/api/"):
            return jsonify({"error": str(exc), "code": code}), status
        return (
            render_template(
                "error.html",
                message=str(exc),
                code=code,
                status=status,
                back=request.referrer or url_for("index"),
            ),
            status,
        )

    @app.errorhandler(CollectError)
    def handle_collect_error(exc):
        return error_response(exc)

    @app.errorhandler(404)
    def handle_404(exc):
        if request.path.startswith("/api/"):
            return jsonify({"error": "not found", "code": "not_found"}), 404
        return render_template("error.html", message="Page not found.", code="not_found",
                               status=404, back=url_for("index")), 404

    @app.errorhandler(413)
    def handle_413(exc):
        return error_response(InputError("request body too large"))

    @app.errorhandler(Exception)
    def handle_unexpected(exc):
        if isinstance(exc, HTTPException):
            return error_response(
                CollectError(
                    exc.description or exc.name,
                    status=exc.code or 500,
                    code=(exc.name or "http_error").lower().replace(" ", "_"),
                )
            )
        log.exception("unhandled error on %s", request.path)
        if request.path.startswith("/api/"):
            return jsonify({"error": "internal server error", "code": "internal_error"}), 500
        return render_template(
            "error.html",
            message="Unexpected server error. Check the application log.",
            code="internal_error",
            status=500,
            back=url_for("index"),
        ), 500

    # -- pages -----------------------------------------------------------
    @app.get("/")
    def index():
        sources = the_store.list_sources()
        # Default the date picker to a day the latest published revision
        # actually covers; otherwise today's date.
        default_day = today_iso()
        if sources and sources[0]["current_revision"]:
            latest = the_store.current_revision(sources[0]["id"])
            if latest is not None and latest["day_max"]:
                default_day = latest["day_max"]
        return render_template(
            "index.html",
            sources=sources,
            jobs=the_store.recent_jobs(10),
            default_day=default_day,
        )

    @app.get("/sources/<int:source_id>")
    def source_detail(source_id):
        source = the_store.get_source(source_id)
        if source is None:
            abort(404)
        revisions = the_store.list_revisions(source_id)
        current = next((r for r in revisions if r["is_current"]), None)
        return render_template(
            "source.html",
            source=source,
            revisions=revisions,
            current=current,
            default_day=current["day_max"] if current and current["day_max"] else today_iso(),
            jobs=[j for j in the_store.recent_jobs(25) if j["source_id"] == source_id],
        )

    @app.post("/collect")
    def collect_form():
        raw_url = request.form.get("manifest_url", "")
        raw_day = request.form.get("day", "")
        try:
            return _run_collect(raw_url, raw_day, json_mode=False)
        except CollectError as exc:
            return error_response(exc)

    @app.post("/api/collect")
    def collect_api():
        payload = request.get_json(silent=True) or {}
        try:
            return _run_collect(
                payload.get("manifest_url", ""),
                payload.get("day", ""),
                json_mode=True,
            )
        except CollectError as exc:
            return error_response(exc)

    def _record_job_failure(job_id, exc):
        # Bookkeeping must never mask the real error the caller needs to see.
        try:
            the_store.update_job(job_id, status="failed", message="%s: %s" % (exc.code, exc))
        except Exception:
            log.exception("could not record failure for job %s", job_id)

    def _run_collect(raw_url, raw_day, *, json_mode):
        url = validate_source_url(raw_url)
        day = parse_day(raw_day)
        source = the_store.find_source_by_url(url)
        job_id = the_store.create_job(url, day.isoformat(), source["id"] if source else None)
        try:
            outcome = collector.collect(url, day)
        except CollectError as exc:
            _record_job_failure(job_id, exc)
            raise
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("collection crashed")
            _record_job_failure(job_id, CollectError(str(exc)))
            raise CollectError("collection failed: %s" % exc) from exc

        row = outcome["revision"]
        if outcome["unchanged"]:
            message = "revision %s is already published and unchanged" % row["revision"]
            job_status = "unchanged"
        elif outcome["resumed_bytes"]:
            message = "published revision %s after resuming at byte %d" % (
                row["revision"], outcome["resumed_bytes"])
            job_status = "published"
        else:
            message = "published revision %s (%d rows, %s..%s)" % (
                row["revision"], row["row_count"], row["day_min"], row["day_max"])
            job_status = "published"
        try:
            the_store.update_job(
                job_id,
                status=job_status,
                message=message,
                revision=row["revision"],
                resumed_bytes=outcome["resumed_bytes"],
                source_id=row["source_id"],
            )
        except Exception:
            # The revision is already published; failing to write the audit
            # row must not turn a successful collection into an error.
            log.exception("could not record job %s", job_id)

        if json_mode:
            return jsonify(
                {
                    "status": job_status,
                    "message": message,
                    "revision": {
                        "id": row["id"],
                        "revision": row["revision"],
                        "bytes": row["byte_count"],
                        "sha256": row["sha256"],
                        "rows": row["row_count"],
                        "day_min": row["day_min"],
                        "day_max": row["day_max"],
                        "resumed_bytes": outcome["resumed_bytes"],
                    },
                    "download_url": url_for("download_revision", revision_id=row["id"],
                                            _external=False),
                    "source_url": url_for("source_detail", source_id=row["source_id"]),
                }
            )

        flash(message, "success" if job_status != "unchanged" else "info")
        return redirect(url_for("source_detail", source_id=row["source_id"]))

    @app.post("/sources/<int:source_id>/switch")
    def switch_revision(source_id):
        source = the_store.get_source(source_id)
        if source is None:
            abort(404)
        try:
            revision_id = int(request.form.get("revision_id", ""))
        except ValueError:
            raise InputError("revision_id must be an integer")
        row = the_store.switch_current(source_id, revision_id)
        flash("revision %s is now current" % row["revision"], "success")
        return redirect(url_for("source_detail", source_id=source_id))

    @app.get("/download/<int:revision_id>")
    def download_revision(revision_id):
        row = the_store.get_revision(revision_id)
        if row is None:
            abort(404)
        path = the_store.revision_path(row)
        if not os.path.exists(path):
            raise NotFound("stored file is missing", status=410, code="file_missing")
        # Integrity check before serving: never hand out a file that no longer
        # matches the recorded bytes/digest (e.g. truncated volume).
        try:
            size = os.path.getsize(path)
        except OSError:
            raise NotFound("stored file is unreadable", status=410, code="file_missing")
        if size != row["byte_count"]:
            raise NotFound(
                "stored file size no longer matches its record", status=410, code="file_corrupt"
            )
        response = send_file(
            path,
            mimetype="text/csv",
            as_attachment=True,
            download_name=_csv_download_name(row),
            conditional=True,
            etag=hashlib.sha256(
                ("%s:%s" % (row["id"], row["sha256"])).encode("utf-8")
            ).hexdigest(),
        )
        response.headers["X-Collected-Revision"] = row["revision"]
        response.headers["X-Collected-SHA256"] = row["sha256"]
        return response

    @app.get("/api/sources")
    def api_sources():
        return jsonify(
            {
                "sources": [
                    {
                        "id": s["id"],
                        "manifest_url": s["manifest_url"],
                        "revision_count": s["revision_count"],
                        "current_revision": s["current_revision"],
                    }
                    for s in the_store.list_sources()
                ]
            }
        )

    @app.get("/api/sources/<int:source_id>")
    def api_source(source_id):
        source = the_store.get_source(source_id)
        if source is None:
            abort(404)
        revisions = the_store.list_revisions(source_id)
        return jsonify(
            {
                "id": source["id"],
                "manifest_url": source["manifest_url"],
                "revisions": [
                    {
                        "id": r["id"],
                        "revision": r["revision"],
                        "bytes": r["byte_count"],
                        "sha256": r["sha256"],
                        "rows": r["row_count"],
                        "day_min": r["day_min"],
                        "day_max": r["day_max"],
                        "current": bool(r["is_current"]),
                        "published_at": r["published_at"],
                        "download_url": url_for("download_revision", revision_id=r["id"]),
                    }
                    for r in revisions
                ],
            }
        )

    @app.get("/healthz")
    def healthz():
        with the_store.session() as conn:
            conn.execute("SELECT 1").fetchone()
        return jsonify({"ok": True})

    return app


def today_iso():
    return _dt.date.today().isoformat()


def main():
    logging.basicConfig(
        level=os.environ.get("COLLECTOR_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    host = os.environ.get("COLLECTOR_HOST", "127.0.0.1")
    port = int(os.environ.get("COLLECTOR_PORT", "8000"))
    app = create_app()
    app.run(host=host, port=port, threaded=True)


if __name__ == "__main__":
    main()
