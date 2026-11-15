"""Shared pytest fixtures: a live sample source on an ephemeral port."""

import datetime as _dt
import os
import socket
import sys
import threading

import pytest
from werkzeug.serving import make_server

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sample_source.app import create_app as create_sample_app  # noqa: E402


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LiveServer:
    def __init__(self, app):
        self.port = _free_port()
        self.server = make_server("127.0.0.1", self.port, app, threaded=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()
        return self

    @property
    def base(self):
        return "http://127.0.0.1:%d" % self.port

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=5)


@pytest.fixture
def anchor_day():
    return _dt.date(2026, 11, 10)


@pytest.fixture
def sample():
    app = create_sample_app(anchor_day=_dt.date(2026, 11, 10))
    server = LiveServer(app).start()
    server.app = app
    try:
        yield server
    finally:
        server.stop()
