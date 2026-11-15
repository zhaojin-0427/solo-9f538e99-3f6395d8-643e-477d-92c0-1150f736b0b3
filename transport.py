"""HTTP transport helpers built on the standard library only.

The collector needs two kinds of upstream access:

* ``fetch_manifest`` -- a small JSON document.  Always downloaded whole and
  never persisted; the final (post-redirect) URL is returned so relative
  ``file`` entries resolve against the location that actually served it.
* raw GET/HEAD requests for the data file, issued by :mod:`downloader` because
  they must interact with the on-disk partial file.

Only ``http`` and ``https`` are ever requested.  URLs carrying userinfo are
rejected, and redirects to any other scheme are refused.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

from errors import ManifestError, UpstreamError

USER_AGENT = "agri-price-collector/1.0"
DEFAULT_TIMEOUT = 20
MAX_MANIFEST_BYTES = 2 * 1024 * 1024


class _SafeRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urllib.parse.urlsplit(newurl)
        if target.scheme not in ("http", "https") or not target.netloc:
            raise UpstreamError("refusing redirect to a non-http URL", code="bad_redirect")
        if target.username is not None or target.password is not None:
            raise UpstreamError("refusing redirect with embedded credentials", code="bad_redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def opener():
    handlers = [_SafeRedirects()]
    proxy = os.environ.get("COLLECTOR_PROXY")
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    return urllib.request.build_opener(*handlers)


def validate_source_url(url):
    """Validate and normalise a user-supplied source URL."""
    if not isinstance(url, str) or not url.strip():
        raise ManifestError("source URL is empty", status=400, code="bad_url")
    url = url.strip()
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise ManifestError("source URL must be http or https", status=400, code="bad_url")
    if not parts.netloc:
        raise ManifestError("source URL has no host", status=400, code="bad_url")
    if parts.username is not None or parts.password is not None:
        raise ManifestError("source URL must not contain credentials", status=400, code="bad_url")
    return urllib.parse.urlunsplit(parts)


def fetch_manifest(url, timeout=DEFAULT_TIMEOUT):
    """Fetch and parse a JSON manifest.  Returns ``(document, final_url)``."""
    url = validate_source_url(url)
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/json;q=0.9, */*;q=0.1",
            "Cache-Control": "no-cache",
        },
    )
    try:
        with opener().open(req, timeout=timeout) as resp:
            if resp.status != 200:
                raise ManifestError(
                    "manifest request returned HTTP %s" % resp.status, code="bad_manifest"
                )
            declared = resp.headers.get("Content-Length")
            if declared is not None:
                try:
                    if int(declared) > MAX_MANIFEST_BYTES:
                        raise ManifestError("manifest is too large", code="bad_manifest")
                except ValueError:
                    pass
            raw = resp.read(MAX_MANIFEST_BYTES + 1)
            final_url = resp.geturl()
    except ManifestError:
        raise
    except urllib.error.HTTPError as exc:
        raise ManifestError(
            "manifest request failed with HTTP %s" % exc.code, code="bad_manifest"
        ) from exc
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise ManifestError(
            "cannot reach manifest: %s" % exc, code="manifest_unreachable"
        ) from exc

    if len(raw) > MAX_MANIFEST_BYTES:
        raise ManifestError("manifest is too large", code="bad_manifest")
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ManifestError("manifest is not valid JSON", code="bad_manifest") from exc
    return doc, final_url
