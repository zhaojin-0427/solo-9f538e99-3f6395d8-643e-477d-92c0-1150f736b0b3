"""Manifest parsing and CSV validation.

The upstream manifest is treated as untrusted input.  Every field is checked
for type, range and consistency before it is used to drive a download, and the
CSV payload is validated with the standard library only.
"""
import csv
import datetime as _dt
import re
import urllib.parse

from errors import InputError, ManifestError, ValidationError

_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_REVISION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

MANIFEST_KEYS = ("revision", "file", "bytes", "sha256")
MAX_REVISION_BYTES = 2 * 1024 * 1024 * 1024  # 2 GiB safety cap


def parse_day(value):
    """Parse a ``YYYY-MM-DD`` day.

    ``datetime.date.fromisoformat`` is too permissive on newer Pythons (it
    accepts basic format like ``20240101``), so the shape is checked first.
    """
    if not isinstance(value, str):
        raise InputError("date is required (YYYY-MM-DD)")
    value = value.strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise InputError("date must be in YYYY-MM-DD form")
    try:
        day = _dt.date.fromisoformat(value)
    except ValueError as exc:
        raise InputError("date is not a valid calendar day: %s" % value) from exc
    return day


def parse_manifest(doc, manifest_url):
    """Validate and normalise a manifest document.

    Returns a dict with ``revision``, ``file_url``, ``bytes`` and ``sha256``.
    """
    if not isinstance(doc, dict):
        raise ManifestError("manifest must be a JSON object", code="bad_manifest")

    missing = [k for k in MANIFEST_KEYS if k not in doc]
    if missing:
        raise ManifestError(
            "manifest is missing required field(s): %s" % ", ".join(missing),
            code="bad_manifest",
        )

    revision = doc["revision"]
    if isinstance(revision, int) and not isinstance(revision, bool):
        revision = str(revision)
    if not isinstance(revision, str) or not _REVISION.match(revision.strip()):
        raise ManifestError("manifest revision is not a safe token", code="bad_manifest")
    revision = revision.strip()

    file_ref = doc["file"]
    if not isinstance(file_ref, str) or not file_ref.strip():
        raise ManifestError("manifest file must be a non-empty string", code="bad_manifest")
    file_url = urllib.parse.urljoin(manifest_url, file_ref.strip())
    parts = urllib.parse.urlsplit(file_url)
    # Refuse a downgrade to file:// or a data: URL supplied by upstream.
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ManifestError("manifest file URL must be http(s)", code="bad_manifest")
    if parts.username is not None or parts.password is not None:
        raise ManifestError("manifest file URL must not embed credentials", code="bad_manifest")

    byte_count = doc["bytes"]
    if isinstance(byte_count, bool) or not isinstance(byte_count, int):
        raise ManifestError("manifest bytes must be an integer", code="bad_manifest")
    if byte_count <= 0:
        raise ManifestError("manifest bytes must be positive", code="bad_manifest")
    if byte_count > MAX_REVISION_BYTES:
        raise ManifestError("manifest bytes exceeds the 2 GiB limit", code="bad_manifest")

    sha256 = doc["sha256"]
    if not isinstance(sha256, str) or not _HEX64.match(sha256.strip()):
        raise ManifestError("manifest sha256 must be 64 hex characters", code="bad_manifest")

    return {
        "revision": revision,
        "file_url": file_url,
        "bytes": byte_count,
        "sha256": sha256.strip().lower(),
    }


def is_strong_etag(etag):
    """A strong ETag is quoted and does not start with ``W/``."""
    if not isinstance(etag, str):
        return False
    etag = etag.strip()
    return len(etag) >= 2 and etag.startswith('"') and etag.endswith('"')


class CsvSummary:
    __slots__ = ("row_count", "day_min", "day_max")

    def __init__(self, row_count, day_min, day_max):
        self.row_count = row_count
        self.day_min = day_min
        self.day_max = day_max


def _clean_cell(value):
    # csv.reader already removes RFC 4180 quoting; only padding is trimmed.
    if value is None:
        return None
    return value.strip()


def _parse_price(text):
    try:
        price = float(text)
    except (TypeError, ValueError):
        return None
    # Reject NaN/inf and negative prices; NaN fails the comparison below.
    if price != price or price < 0 or price in (float("inf"), float("-inf")):
        return None
    return price


def validate_csv(path, expected_day=None):
    """Validate a downloaded CSV and return a :class:`CsvSummary`.

    Rules
    -----
    * UTF-8 (a BOM is tolerated), comma separated, header row required.
    * At least the columns ``date,market,product,price`` (case-insensitive,
      surrounding whitespace ignored).  Extra columns are allowed.
    * Every row must have a parseable ``YYYY-MM-DD`` date, non-empty market and
      product, and a finite non-negative price.
    * If ``expected_day`` is given, at least one row must be for that day.  A
      file whose rows are all for other days belongs to the wrong batch.
    """
    try:
        fh = open(path, "r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise ValidationError("cannot read downloaded file: %s" % exc) from exc

    with fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            raise ValidationError("CSV is empty", code="empty_csv")
        except csv.Error as exc:
            raise ValidationError("CSV header is malformed: %s" % exc) from exc

        columns = {}
        for index, name in enumerate(header):
            key = _clean_cell(name).lower()
            if key and key not in columns:
                columns[key] = index
        required = ("date", "market", "product", "price")
        missing = [k for k in required if k not in columns]
        if missing:
            raise ValidationError(
                "CSV is missing required column(s): %s" % ", ".join(missing),
                code="bad_csv_header",
            )
        idx_date = columns["date"]
        idx_market = columns["market"]
        idx_product = columns["product"]
        idx_price = columns["price"]
        width = len(header)

        row_count = 0
        day_min = None
        day_max = None
        found_expected = expected_day is None
        for line_no, row in enumerate(reader, start=2):
            if not row or all(not (_clean_cell(cell) or "") for cell in row):
                continue  # tolerate blank lines, common at EOF
            if len(row) != width:
                raise ValidationError(
                    "CSV row %d has %d fields, header has %d" % (line_no, len(row), width),
                    code="bad_csv_row",
                )
            day_text = _clean_cell(row[idx_date])
            try:
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_text or ""):
                    raise ValueError
                day = _dt.date.fromisoformat(day_text)
            except ValueError:
                raise ValidationError(
                    "CSV row %d has an invalid date %r" % (line_no, day_text),
                    code="bad_csv_row",
                ) from None
            market = _clean_cell(row[idx_market])
            product = _clean_cell(row[idx_product])
            if not market or not product:
                raise ValidationError(
                    "CSV row %d has an empty market or product" % line_no,
                    code="bad_csv_row",
                )
            price = _parse_price(_clean_cell(row[idx_price]))
            if price is None:
                raise ValidationError(
                    "CSV row %d has an invalid price %r" % (line_no, row[idx_price]),
                    code="bad_csv_row",
                )
            row_count += 1
            text = day.isoformat()
            if day_min is None or text < day_min:
                day_min = text
            if day_max is None or text > day_max:
                day_max = text
            if not found_expected and day == expected_day:
                found_expected = True

    if row_count == 0:
        raise ValidationError("CSV contains no data rows", code="empty_csv")
    if not found_expected:
        raise ValidationError(
            "CSV contains no rows for requested date %s (rows cover %s..%s)"
            % (expected_day.isoformat(), day_min, day_max),
            code="day_mismatch",
        )
    return CsvSummary(row_count, day_min, day_max)

