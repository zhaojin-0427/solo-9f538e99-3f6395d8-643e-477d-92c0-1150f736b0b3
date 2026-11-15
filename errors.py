"""Shared error types for the collector web application.

Every error that is expected during a collection carries an HTTP status and a
stable machine-readable ``code`` so the web UI and the JSON API can present it
without leaking a traceback.
"""


class CollectError(Exception):
    """An error that can be rendered to an API/UI caller."""

    status = 400
    code = "collect_error"

    def __init__(self, message, *, status=None, code=None):
        super().__init__(message)
        if status is not None:
            self.status = status
        if code is not None:
            self.code = code


class InputError(CollectError):
    status = 400
    code = "invalid_input"


class BusyError(CollectError):
    status = 409
    code = "ingest_busy"


class ManifestError(CollectError):
    status = 502
    code = "bad_manifest"


class UpstreamError(CollectError):
    status = 502
    code = "upstream_error"


class SourceChanged(UpstreamError):
    status = 409
    code = "source_changed"


class IncompleteDownload(UpstreamError):
    status = 502
    code = "incomplete_download"


class ValidationError(CollectError):
    status = 422
    code = "validation_failed"


class StorageError(CollectError):
    status = 500
    code = "storage_error"


class NotFound(CollectError):
    status = 404
    code = "not_found"
