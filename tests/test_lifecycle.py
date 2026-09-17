"""Server startup and shutdown behavior.

Implementation: Server.__init__ (binds via StreamServer, then calls
self._load()) and the StreamServer.close()/serve_forever() pair
(server.py). MULTI/transaction and command-table behavior are covered
elsewhere; this file is only about the process lifecycle: does it come up
correctly, does it load what's on disk at that moment, does shutting it
down actually stop it from accepting new connections.
"""
import os
import pickle
import socket

import pytest

from conftest import LiveServer


def test_server_starts_and_accepts_connections(live_server):
    sock = live_server.raw_connect()
    sock.close()


def test_server_with_no_existing_dump_file_starts_with_empty_store(make_live_server, tmp_path):
    server = make_live_server(dump_path=str(tmp_path / 'fresh.rdb'))
    client = server.client()
    try:
        assert client.flush() == 0
    finally:
        client._socket.close()


def test_server_loads_dump_file_present_at_startup_time(make_live_server, tmp_path):
    # This is startup-time loading specifically (Server.__init__ ->
    # self._load()), as distinct from a SAVE/reload round trip during a
    # running process (covered in test_persistence.py).
    dump_path = str(tmp_path / 'startup.rdb')
    with open(dump_path, 'wb') as fh:
        pickle.dump({b'k': b'v'}, fh)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        assert client.get('k') == b'v'
    finally:
        client._socket.close()


def test_stopping_the_server_closes_the_listening_socket(tmp_path):
    server = LiveServer(dump_path=str(tmp_path / 'dump.rdb'))
    server.start()
    try:
        sock = server.raw_connect()
        sock.close()
    finally:
        server.stop()

    with pytest.raises(OSError):
        socket.create_connection(('127.0.0.1', server.port), timeout=1)


def test_each_test_server_gets_an_independent_store(make_live_server, tmp_path):
    # Guards against a shared/global store accidentally leaking between
    # what are supposed to be independent Server instances.
    server_a = make_live_server(dump_path=str(tmp_path / 'a.rdb'))
    server_b = make_live_server(dump_path=str(tmp_path / 'b.rdb'))
    client_a = server_a.client()
    client_b = server_b.client()
    try:
        client_a.set('only-in-a', '1')
        assert client_b.get('only-in-a') is None
    finally:
        client_a._socket.close()
        client_b._socket.close()
