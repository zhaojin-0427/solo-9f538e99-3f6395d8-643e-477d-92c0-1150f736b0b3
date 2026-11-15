# Agricultural wholesale price collector

A small Flask + SQLite service that a market-data operator uses to pull the
**current revision** of a daily wholesale-price CSV from a public source, verify
it, and publish it for a warehouse to load by date.

The warehouse always downloads only fully verified, committed revisions. An
interrupted or rejected collection never becomes visible, and the previously
published revision stays downloadable throughout.

## Quick start (Docker Compose)

```sh
docker compose up --build
```

* Collector UI/API: <http://127.0.0.1:8000>
* Sample upstream: <http://127.0.0.1:8001> (also serves an interactive page)

On the collector page enter

* Manifest URL: `http://sample-source:8001/manifest.json`
* Business date: `2026-11-10`

and press **Collect revision**. The source page then shows the published
revision with a download link, a **make current** button for earlier revisions,
and an audit trail of collection attempts.

`scripts/demo.sh` drives the same flow over the JSON API, including an
interrupted download and its resume:

```sh
./scripts/demo.sh
```

### Running without Docker

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python scripts/run_sample_source.py --port 8001 &          # sample upstream
COLLECTOR_DATA_DIR=./data python scripts/run_collector.py   # UI on :8000
```

In this mode the manifest URL is `http://127.0.0.1:8001/manifest.json`.

### Tests

```sh
pip install -r requirements-dev.txt
python -m pytest -q
# or, in Docker:
docker build -f docker/Dockerfile.test -t collector-tests . && docker run --rm collector-tests
```

## Behaviour

### Collection flow

1. Fetch the manifest (JSON: `revision`, `file`, `bytes`, `sha256`). Only
   `http`/`https` URLs are accepted; redirects to other schemes and URLs with
   embedded credentials are refused. Relative `file` entries resolve against the
   final manifest URL.
2. If that exact revision is already published with matching byte count and
   digest, nothing is downloaded again — the result is reported as *unchanged*.
3. Probe the data file (`HEAD`) for a strong ETag and Range support, then
   download or resume.
4. Verify the downloaded bytes: exact byte count, SHA-256, then parse the CSV
   and require at least one row for the requested date. Every row must have a
   valid `YYYY-MM-DD` date, a non-empty market and product, and a finite
   non-negative price.
5. Publish atomically; only after that does the revision appear in the UI and
   become current.

### Resume rules

A partial download is continued **only** when all of these hold:

* the sidecar state is for the same revision *and* the same manifest byte
  count and SHA-256;
* the server advertises a **strong** ETag (quoted, not `W/`), and the current
  `HEAD` ETag equals the recorded one;
* the server supports Range and answers with `206` whose `Content-Range`
  begins at exactly the number of stored bytes.

If the upstream revision changes, the ETag changes, the server does not support
Range, the stored size does not match the partial, or a `416` is returned, the
partial is discarded and the file is downloaded from scratch. Bytes from two
revisions are never concatenated. A short EOF (dropped connection) keeps the
partial for a later resumable retry; a payload that is complete but fails
digest/length checks is discarded.

### Failure and restart behaviour

* A partial file lives under `partials/` and is never listed or downloadable.
* Published files move into `files/` under a fresh name before the database
  transaction commits; a crash between the two leaves an unreferenced file, not
  a row pointing at a missing file. A failed commit moves the file back into
  `partials/` (or unlinks it) and leaves the previous current revision
  untouched, so a retry reuses the verified bytes.
* `is_current` is enforced by a partial unique index, so at most one revision
  per source can be current.
* Jobs left `running` by a process crash are marked `interrupted` on start-up;
  retrying the same revision resumes from the saved bytes.
* A per-source advisory file lock makes a second concurrent collection return
  HTTP 409 instead of interleaving writes.

## HTTP API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/collect` | `{"manifest_url": "...", "day": "YYYY-MM-DD"}` → publish and return the revision |
| `GET` | `/api/sources` | list sources and current revisions |
| `GET` | `/api/sources/<id>` | revisions of a source, with download URLs |
| `GET` | `/download/<revision_id>` | download a published CSV (attachment, digest in headers) |
| `GET` | `/healthz` | health check used by Compose |

Errors are JSON for `/api/*` paths with a stable `code`
(`invalid_input`, `bad_manifest`, `incomplete_download`, `checksum_mismatch`,
`validation_failed`, `ingest_busy`, …).

## Sample upstream

`sample_source/app.py` provides three revisions (`2026-11-10-a`,
`2026-11-10-b`, `2026-11-10-c`) covering `2026-11-09` and `2026-11-10`, with
strong ETags, `Accept-Ranges`, `206`/`Content-Range`, `If-Range` and
conditional `304`s.

* `/manifest.json` — current revision descriptor
* `/switch/<revision>` — change the advertised revision (`POST`)
* `/demo/interrupt` — arm a one-shot truncated response (`{"after": 120}`)
* `/` — small browser page for both controls

`SAMPLE_DAY` (Compose sets `2026-11-10`) controls the day the rows are
generated for; the day before is included as well.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `COLLECTOR_DATA_DIR` | `./data` | base for database, published files and partials |
| `COLLECTOR_DB` | `$DATA_DIR/collector.sqlite3` | SQLite file |
| `COLLECTOR_FILES` | `$DATA_DIR/files` | published CSV files |
| `COLLECTOR_PARTIALS` | `$DATA_DIR/partials` | in-flight downloads |
| `COLLECTOR_HOST` / `COLLECTOR_PORT` | `127.0.0.1` / `8000` | dev server bind (Compose uses gunicorn on `0.0.0.0:8000`) |
| `COLLECTOR_SECRET` | random | Flask session key |
| `COLLECTOR_PROXY` | — | optional proxy for upstream requests |

## Limitations and assumptions

* The manifest is a single JSON object describing exactly one revision. If your
  source lists several files, point the manifest at the revision to load.
* Validation requires the columns `date, market, product, price`
  (case-insensitive); extra columns are allowed and ignored.
* Collection runs synchronously in the request. Large files therefore need a
  longer proxy timeout; the Compose gunicorn timeout is 120 s.
* No authentication is built in — run it behind your network boundary.
