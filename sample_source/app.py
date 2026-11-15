"""Local sample upstream for the agriculture price collector demo.

It behaves like a small public data portal:

* ``/manifest.json`` advertises the current revision: revision token, file URL,
  byte count and SHA-256.
* ``/files/<name>.csv`` serves the payload with a **strong** ETag and byte
  ranges (``Accept-Ranges: bytes``, ``206`` + ``Content-Range``, ``If-Range``).
* ``/revisions`` lists available revisions; ``/switch/<revision>`` changes what
  the manifest advertises.
* ``/demo/interrupt?after=N`` arms a one-shot truncated response so the resume
  path can be demonstrated in the browser: the next file GET sends N bytes and
  drops the connection.

Run standalone::

    python sample_source/app.py --port 8001
"""

import argparse
import datetime as _dt
import hashlib
import html
import os
import re
import threading

from flask import Flask, Response, abort, jsonify, redirect, request

_RANGE = re.compile(r"^bytes=(\d*)-(\d*)$")


def _csv_bytes(rows):
    lines = ["date,market,product,price"]
    for day, market, product, price in rows:
        lines.append("%s,%s,%s,%s" % (day, market, product, price))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _price_rows(day, revision_tag):
    """Sample rows for one day; each revision is a small price correction."""
    offsets = {"a": 0.0, "b": 0.05, "c": 0.10}
    offset = offsets.get(revision_tag, 0.0)
    markets = [
        ("Beijing Xinfadi", "cabbage", "2.40"),
        ("Beijing Xinfadi", "tomato", "5.10"),
        ("Shanghai Jiangqiao", "cabbage", "2.15"),
        ("Shanghai Jiangqiao", "tomato", "4.80"),
        ("Guangzhou Jiangnan", "cabbage", "2.60"),
        ("Guangzhou Jiangnan", "tomato", "5.35"),
    ]
    rows = []
    for market, product, price in markets:
        adjusted = "%.2f" % (float(price) + offset)
        rows.append((day, market, product, adjusted))
    return rows


def build_sample_dataset(anchor_day):
    """Return ``{revision_name: payload/metadata}`` with three revisions.

    All revisions cover ``anchor_day`` and the day before, so the collector's
    row-date check is satisfied whichever of the two days the operator enters.
    Three revisions let a demo publish one, then switch to a never-seen one so
    an interrupted download actually has bytes to resume.
    """
    previous = anchor_day - _dt.timedelta(days=1)
    revisions = {}
    for tag, label in (("a", "initial"), ("b", "corrected"), ("c", "late correction")):
        rows = _price_rows(previous, tag) + _price_rows(anchor_day, tag)
        payload = _csv_bytes(rows)
        name = "%s-%s" % (anchor_day.isoformat(), tag)
        revisions[name] = {
            "bytes": payload,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "label": label,
        }
    return revisions


class SampleSource:
    def __init__(self, anchor_day=None, public_base=None):
        self.anchor_day = anchor_day or _dt.date.today()
        self.revisions = build_sample_dataset(self.anchor_day)
        self.order = sorted(self.revisions)
        self.current = self.order[-1]
        self.public_base = public_base  # optional override for the file URL
        self._lock = threading.Lock()
        self._interrupt_after = None

    # -- helpers ---------------------------------------------------------
    def manifest_for(self, name):
        rev = self.revisions[name]
        file_name = name + ".csv"
        if self.public_base:
            file_url = self.public_base.rstrip("/") + "/files/" + file_name
        else:
            file_url = request.url_root.rstrip("/") + "/files/" + file_name
        return {
            "revision": name,
            "file": file_url,
            "bytes": len(rev["bytes"]),
            "sha256": rev["sha256"],
            "label": rev["label"],
            "published_at": self.anchor_day.isoformat(),
        }

    def etag_for(self, name):
        return '"%s"' % self.revisions[name]["sha256"]

    def arm_interrupt(self, after):
        with self._lock:
            self._interrupt_after = after

    def take_interrupt(self):
        with self._lock:
            value = self._interrupt_after
            self._interrupt_after = None
            return value

    def switch(self, name):
        if name not in self.revisions:
            return False
        self.current = name
        return True


def create_app(anchor_day=None, public_base=None):
    app = Flask(__name__)
    state = SampleSource(anchor_day=anchor_day, public_base=public_base)
    app.config["SAMPLE"] = state

    @app.get("/")
    def index():
        s = app.config["SAMPLE"]
        items = []
        for name in s.order:
            mark = " (current)" if name == s.current else ""
            items.append(
                "<li>%s%s — %d bytes — sha256 %s</li>"
                % (html.escape(name), mark, len(s.revisions[name]["bytes"]),
                   s.revisions[name]["sha256"])
            )
        return (
            "<!doctype html><html><body><h1>Sample agriculture price source</h1>"
            "<p>Manifest: <a href='/manifest.json'>/manifest.json</a></p>"
            "<ul>%s</ul>"
            "<form method='post' action='/switch'><select name='revision'>%s</select>"
            "<button>Switch current revision</button></form>"
            "<p>Armed interruption: %s</p>"
            "<form method='post' action='/demo/interrupt'>"
            "<label>Drop after N bytes: <input name='after' value='120' size='8'></label>"
            "<button>Arm one-shot interruption</button></form>"
            "<p>Use the collector with date %s.</p>"
            "</body></html>"
            % (
                "".join(items),
                "".join(
                    "<option value='%s'>%s</option>" % (html.escape(n), html.escape(n))
                    for n in s.order
                ),
                html.escape(str(s._interrupt_after)),
                s.anchor_day.isoformat(),
            )
        )

    @app.get("/manifest.json")
    def manifest():
        s = app.config["SAMPLE"]
        resp = jsonify(s.manifest_for(s.current))
        resp.headers["Cache-Control"] = "no-cache"
        return resp

    @app.get("/revisions")
    def revisions():
        s = app.config["SAMPLE"]
        return jsonify(
            {
                "current": s.current,
                "revisions": [
                    {
                        "revision": name,
                        "bytes": len(s.revisions[name]["bytes"]),
                        "sha256": s.revisions[name]["sha256"],
                        "label": s.revisions[name]["label"],
                    }
                    for name in s.order
                ],
            }
        )

    @app.post("/switch")
    def switch_form():
        s = app.config["SAMPLE"]
        name = request.form.get("revision", "")
        if not s.switch(name):
            abort(404)
        return redirect("/")

    @app.post("/switch/<name>")
    def switch_api(name):
        s = app.config["SAMPLE"]
        if not s.switch(name):
            abort(404)
        return jsonify({"current": s.current})

    @app.post("/demo/interrupt")
    def arm_interrupt():
        s = app.config["SAMPLE"]
        raw = request.form.get("after") or (request.get_json(silent=True) or {}).get("after")
        try:
            after = int(raw)
        except (TypeError, ValueError):
            return jsonify({"error": "after must be an integer"}), 400
        if after <= 0:
            return jsonify({"error": "after must be positive"}), 400
        s.arm_interrupt(after)
        if request.form:
            return redirect("/")
        return jsonify({"armed": after})

    @app.post("/demo/interrupt/clear")
    def clear_interrupt():
        app.config["SAMPLE"].arm_interrupt(None)
        return jsonify({"armed": None})

    @app.get("/files/<name>")
    def serve_file(name):
        s = app.config["SAMPLE"]
        if not name.endswith(".csv"):
            abort(404)
        revision_name = name[: -len(".csv")]
        if revision_name not in s.revisions:
            abort(404)
        payload = s.revisions[revision_name]["bytes"]
        etag = s.etag_for(revision_name)
        total = len(payload)

        if request.if_none_match and etag in request.if_none_match:
            return Response(status=304, headers={"ETag": etag})

        range_header = request.headers.get("Range")
        if_range = request.headers.get("If-Range")
        honour_range = bool(range_header) and (if_range is None or if_range.strip() == etag)

        start, end = 0, total - 1
        status = 200
        if honour_range:
            match = _RANGE.match(range_header.strip())
            if not match:
                return Response(
                    status=416,
                    headers={"Content-Range": "bytes */%d" % total, "ETag": etag},
                )
            first, last = match.group(1), match.group(2)
            if first == "" and last == "":
                return Response(
                    status=416,
                    headers={"Content-Range": "bytes */%d" % total, "ETag": etag},
                )
            if first == "":
                # suffix range: last N bytes
                length = int(last)
                if length <= 0:
                    return Response(
                        status=416,
                        headers={"Content-Range": "bytes */%d" % total, "ETag": etag},
                    )
                start = max(0, total - length)
                end = total - 1
            else:
                start = int(first)
                end = int(last) if last else total - 1
            if start >= total or start > end:
                return Response(
                    status=416,
                    headers={"Content-Range": "bytes */%d" % total, "ETag": etag},
                )
            end = min(end, total - 1)
            status = 206

        body = payload[start : end + 1]
        headers = {
            "ETag": etag,
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
            "Content-Disposition": 'inline; filename="%s"' % name,
        }
        if status == 206:
            headers["Content-Range"] = "bytes %d-%d/%d" % (start, end, total)

        interrupt_after = s.take_interrupt() if status == 200 and request.method == "GET" else None
        if interrupt_after is not None:
            interrupt_after = min(interrupt_after, len(body))

            def truncated():
                yield body[:interrupt_after]
                # Simulate the source connection dropping mid-transfer.  The
                # declared Content-Length is the full size, so the client sees
                # a short read rather than a clean end of file.
                raise ConnectionError("simulated upstream interruption")

            resp = Response(truncated(), status=200, headers=headers, mimetype="text/csv")
            resp.headers["Content-Length"] = str(len(body))
            return resp

        return Response(body, status=status, headers=headers, mimetype="text/csv")

    @app.get("/healthz")
    def healthz():
        return jsonify({"ok": True, "anchor_day": app.config["SAMPLE"].anchor_day.isoformat()})

    return app


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("SAMPLE_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SAMPLE_PORT", "8001")))
    parser.add_argument(
        "--anchor-day",
        default=os.environ.get("SAMPLE_DAY"),
        help="day the sample CSV rows are generated for (default: today)",
    )
    parser.add_argument("--public-base", default=os.environ.get("SAMPLE_PUBLIC_BASE"))
    args = parser.parse_args(argv)
    anchor = _dt.date.fromisoformat(args.anchor_day) if args.anchor_day else None
    app = create_app(anchor_day=anchor, public_base=args.public_base)
    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
