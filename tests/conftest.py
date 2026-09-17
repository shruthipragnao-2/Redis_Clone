"""Shared fixtures for the test suite.

LiveServer runs a real Server (real gevent StreamServer, real Pool) in a
background daemon thread on an ephemeral port, backed by a throwaway
dump-file directory -- tests never read or write the project's real
dump.rdb. Each test that requests the `live_server` fixture gets its own
freshly constructed Server with an empty store, so tests can't leak state
into each other.
"""
import os
import shutil
import socket
import tempfile
import threading
import time

import pytest

from client import Client
from server import ProtocolHandler, Server

CONNECT_TIMEOUT = 5
RECV_TIMEOUT = 5


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


class LiveServer:
    """Runs a real Server in a background thread for the life of a test.

    The Server instance (and everything gevent-ish it owns: its Pool, its
    StreamServer, its internal stop Event) is constructed *inside* the
    background thread rather than passed in from the main thread. gevent
    binds these primitives to whichever thread's hub is active when
    they're first touched; constructing them on the main thread and then
    calling serve_forever() from a different thread leaves the stop
    event's hub and the accept loop's hub mismatched, which surfaces as
    gevent raising LoopExit ("this operation would block forever") the
    moment serve_forever() waits on it.
    """

    def __init__(self, **server_kwargs):
        self.port = _free_port()
        self.tmpdir = tempfile.mkdtemp(prefix='resp-test-')
        self.dump_path = server_kwargs.pop('dump_path', None) or os.path.join(
            self.tmpdir, 'test-dump.rdb')
        self._server_kwargs = server_kwargs
        self._ready = threading.Event()
        self._init_error = None
        self.server = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            self.server = Server(
                host='127.0.0.1', port=self.port, dump_path=self.dump_path,
                **self._server_kwargs)
        except Exception as exc:  # surfaced to the main thread in start()
            self._init_error = exc
            self._ready.set()
            return
        self._ready.set()
        # Server.run() (not just the bare StreamServer) so that passing
        # autosave_interval=... exercises the real autosave code path too.
        self.server.run()

    def start(self):
        self.thread.start()
        if not self._ready.wait(timeout=CONNECT_TIMEOUT):
            raise RuntimeError('server thread did not initialize in time')
        if self._init_error is not None:
            raise self._init_error
        self._wait_until_accepting()
        return self

    def stop(self):
        try:
            self.server._server.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _wait_until_accepting(self, timeout=CONNECT_TIMEOUT):
        deadline = time.time() + timeout
        last_exc = None
        while time.time() < deadline:
            try:
                s = socket.create_connection(('127.0.0.1', self.port), timeout=0.2)
                s.close()
                return
            except OSError as exc:
                last_exc = exc
                time.sleep(0.02)
        raise RuntimeError('server never started accepting connections: %r' % last_exc)

    def raw_connect(self):
        """A bare socket, for tests that need to control bytes on the wire."""
        return socket.create_connection(('127.0.0.1', self.port), timeout=RECV_TIMEOUT)

    def client(self):
        """A Client using the project's own bundled client library --
        exercises the server the same way a real caller would."""
        return Client(host='127.0.0.1', port=self.port)


class StructuredClient:
    """Pairs a raw socket with ProtocolHandler so a test can send
    hand-built requests and decode replies (including Error/SimpleString
    values that the high-level Client swallows or raises on) without
    duplicating RESP-framing logic."""

    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(RECV_TIMEOUT)
        self.fh = sock.makefile('rwb')
        self.protocol = ProtocolHandler()

    def send(self, *args):
        self.protocol.write_response(self.fh, list(args))

    def recv(self):
        return self.protocol.handle_request(self.fh)

    def call(self, *args):
        self.send(*args)
        return self.recv()

    def close(self):
        try:
            self.fh.close()
        finally:
            self.sock.close()


def recv_until(sock, min_bytes, timeout=RECV_TIMEOUT):
    """Accumulate raw bytes from a socket until at least min_bytes have
    arrived or the connection times out/closes."""
    sock.settimeout(timeout)
    buf = b''
    while len(buf) < min_bytes:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    return buf


@pytest.fixture
def live_server():
    """A fresh Server, with an empty in-memory store and its own throwaway
    dump-file directory, for the duration of one test."""
    server = LiveServer()
    server.start()
    yield server
    server.stop()


@pytest.fixture
def make_live_server():
    """Factory fixture for tests that need non-default Server kwargs
    (e.g. autosave_interval) or a specific dump_path (e.g. persistence
    tests that pre-seed a dump file). Every server created is torn down
    at the end of the test."""
    created = []

    def _make(**kwargs):
        server = LiveServer(**kwargs)
        server.start()
        created.append(server)
        return server

    yield _make
    for server in created:
        server.stop()


@pytest.fixture
def client(live_server):
    """A connected high-level Client (the project's own client.py) against
    a fresh server."""
    c = live_server.client()
    yield c
    c._socket.close()


@pytest.fixture
def structured_client(live_server):
    sc = StructuredClient(live_server.raw_connect())
    yield sc
    sc.close()


def run_concurrently(worker, n):
    """Run worker(i) for i in range(n), each in its own thread, released
    together via a threading.Barrier so they actually contend rather than
    running one after another because of incidental thread-start latency
    -- deterministic contention, not a timing guess.

    Any exception (including a failed assert) raised inside a worker is
    captured and re-raised on the calling thread once every worker has
    finished, since exceptions inside a bare Thread.run() are otherwise
    swallowed.
    """
    barrier = threading.Barrier(n)
    errors = [None] * n

    def wrapped(i):
        barrier.wait()
        try:
            worker(i)
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            errors[i] = exc

    threads = [threading.Thread(target=wrapped, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for exc in errors:
        if exc is not None:
            raise exc


@pytest.fixture
def make_structured_client(live_server):
    """Factory for tests that need several independent connections to the
    same server (e.g. transaction isolation, multi-client tests)."""
    created = []

    def _make():
        sc = StructuredClient(live_server.raw_connect())
        created.append(sc)
        return sc

    yield _make
    for sc in created:
        sc.close()
