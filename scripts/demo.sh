#!/bin/sh
# End-to-end demo against the compose stack.  Requires curl and python3.
set -eu

COLLECTOR="${COLLECTOR_URL:-http://127.0.0.1:8000}"
SOURCE="${SOURCE_URL:-http://127.0.0.1:8001}"
DAY="${DAY:-2026-11-10}"
A="$DAY-a"; B="$DAY-b"; C="$DAY-c"

say() { printf '\n== %s\n' "$*"; }

say "source health"
curl -fsS "$SOURCE/healthz"; echo

say "publish revision a"
curl -fsS -X POST "$SOURCE/switch/$A" >/dev/null
curl -fsS -X POST "$COLLECTOR/api/collect" \
  -H 'Content-Type: application/json' \
  -d "{\"manifest_url\":\"$SOURCE/manifest.json\",\"day\":\"$DAY\"}"; echo

say "publish revision b; a stays downloadable"
curl -fsS -X POST "$SOURCE/switch/$B" >/dev/null
curl -fsS -X POST "$COLLECTOR/api/collect" \
  -H 'Content-Type: application/json' \
  -d "{\"manifest_url\":\"$SOURCE/manifest.json\",\"day\":\"$DAY\"}"; echo

say "arm a one-shot interruption on the next fresh revision, then collect"
curl -fsS -X POST "$SOURCE/switch/$C" >/dev/null
curl -fsS -X POST "$SOURCE/demo/interrupt" \
  -H 'Content-Type: application/json' -d '{"after":120}' >/dev/null
curl -sS -X POST "$COLLECTOR/api/collect" \
  -H 'Content-Type: application/json' \
  -d "{\"manifest_url\":\"$SOURCE/manifest.json\",\"day\":\"$DAY\"}" || true; echo

say "retry the same revision: it resumes from the saved bytes"
curl -fsS -X POST "$COLLECTOR/api/collect" \
  -H 'Content-Type: application/json' \
  -d "{\"manifest_url\":\"$SOURCE/manifest.json\",\"day\":\"$DAY\"}"; echo

say "switch the source back to revision a and collect it (third published revision)"
curl -fsS -X POST "$SOURCE/switch/$A" >/dev/null
curl -fsS -X POST "$COLLECTOR/api/collect" \
  -H 'Content-Type: application/json' \
  -d "{\"manifest_url\":\"$SOURCE/manifest.json\",\"day\":\"$DAY\"}"; echo

say "open $COLLECTOR in a browser to switch the current revision and download"
