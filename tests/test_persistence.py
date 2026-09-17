"""SAVE / startup-load persistence behavior.

Implementation: Server.save() (atomic write: tempfile + os.replace) and
Server._load() (server.py), called once from Server.__init__.

Every test here uses a throwaway directory (via tmp_path or LiveServer's
own tmpdir) -- none of them ever touch the project's real dump.rdb.
"""
import os
import pickle
import time

import pytest

from server import Server


# --- save() / _load() round trip, driven through the live server ---

def test_save_with_empty_store_then_reload(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'empty.rdb')
    server = make_live_server(dump_path=dump_path)
    client = server.client()
    assert client.save() == b'OK'
    client._socket.close()

    assert os.path.exists(dump_path)
    with open(dump_path, 'rb') as fh:
        assert pickle.load(fh) == {}

    reloaded = make_live_server(dump_path=dump_path)
    reloaded_client = reloaded.client()
    assert reloaded_client.flush() == 0  # nothing was loaded
    reloaded_client._socket.close()


def test_save_after_multiple_mutations_then_reload(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'data.rdb')
    server = make_live_server(dump_path=dump_path)
    client = server.client()
    client.set('a', '1')
    client.set('b', '2')
    client.mset('c', '3', 'd', '4')
    client.delete('a')  # a should NOT reappear after reload
    assert client.save() == b'OK'
    client._socket.close()

    with open(dump_path, 'rb') as fh:
        on_disk = pickle.load(fh)
    assert on_disk == {b'b': b'2', b'c': b'3', b'd': b'4'}

    reloaded = make_live_server(dump_path=dump_path)
    reloaded_client = reloaded.client()
    try:
        assert reloaded_client.get('a') is None
        assert reloaded_client.get('b') == b'2'
        assert reloaded_client.mget('c', 'd') == [b'3', b'4']
    finally:
        reloaded_client._socket.close()


def test_save_overwrites_previous_snapshot(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'data.rdb')
    server = make_live_server(dump_path=dump_path)
    client = server.client()
    client.set('k', 'first')
    client.save()
    client.set('k', 'second')
    client.save()
    client._socket.close()

    with open(dump_path, 'rb') as fh:
        assert pickle.load(fh) == {b'k': b'second'}


def test_startup_with_no_dump_file_starts_with_empty_store(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'does-not-exist.rdb')
    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        assert client.flush() == 0
    finally:
        client._socket.close()


def test_startup_loads_preexisting_dump_file(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'preseeded.rdb')
    with open(dump_path, 'wb') as fh:
        pickle.dump({b'preloaded': b'yes'}, fh)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        assert client.get('preloaded') == b'yes'
    finally:
        client._socket.close()


# --- Autosave (Server.run() / Server._autosave_loop) ---

def test_autosave_periodically_writes_to_disk(make_live_server, tmp_path):
    """Server.run() spawns _autosave_loop() when autosave_interval is set,
    which calls save() on a timer. There's no completion signal exposed
    for "a background autosave just ran", so this polls for the dump file
    to appear rather than sleeping a single fixed guess -- bounded by a
    generous deadline well above the configured interval, so it fails
    fast if autosave is broken instead of only if it's slow.
    """
    dump_path = str(tmp_path / 'autosave.rdb')
    server = make_live_server(dump_path=dump_path, autosave_interval=0.1)
    client = server.client()
    try:
        client.set('k', 'v')

        # Poll for the mutation to land on disk rather than asserting
        # after a fixed sleep. The atomic write (tempfile + os.replace)
        # means a reader only ever sees a fully-written file, never a
        # torn one, so it's safe to just retry the read until it matches
        # (an autosave that fired before our set() above, or a
        # WindowsError while os.replace is mid-swap, both just mean "not
        # yet" here).
        deadline = time.time() + 5
        on_disk = None
        while time.time() < deadline:
            try:
                with open(dump_path, 'rb') as fh:
                    on_disk = pickle.load(fh)
            except (FileNotFoundError, EOFError, OSError):
                on_disk = None
            if on_disk == {b'k': b'v'}:
                break
            time.sleep(0.05)

        assert on_disk == {b'k': b'v'}, 'autosave never persisted the mutation'
    finally:
        client._socket.close()


# --- Corrupt dump file handling (Server._load's existing except clause) ---

def test_startup_with_truncated_dump_file_falls_back_to_empty_store(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'truncated.rdb')
    with open(dump_path, 'wb') as fh:
        fh.write(b'')  # zero-byte file -> pickle.load raises EOFError

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        assert client.flush() == 0
    finally:
        client._socket.close()


def test_startup_with_garbage_dump_file_falls_back_to_empty_store(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'garbage.rdb')
    with open(dump_path, 'wb') as fh:
        fh.write(b'this is not a pickle stream at all, just noise 12345')

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        assert client.flush() == 0
    finally:
        client._socket.close()


# --- Failure injection: a write failure during SAVE ---

def test_save_failure_leaves_no_temp_file_behind(monkeypatch, make_live_server, tmp_path):
    """Server.save() writes to a temp file and os.replace()s it into place;
    on failure it explicitly removes the temp file. Inject a failure in
    pickle.dump (simulating e.g. a disk error) and confirm that cleanup
    actually happens, without touching any real file on disk.
    """
    import pickle as pickle_module

    dump_path = str(tmp_path / 'data.rdb')
    server = make_live_server(dump_path=dump_path)

    def failing_dump(*args, **kwargs):
        raise OSError('simulated disk failure')

    monkeypatch.setattr(pickle_module, 'dump', failing_dump)

    client = server.client()
    client.set('k', 'v')
    with pytest.raises(Exception):
        client.save()
    client._socket.close()

    leftover = [f for f in os.listdir(tmp_path) if f.startswith('.dump-')]
    assert leftover == [], 'temp file was not cleaned up after a failed save'
    assert not os.path.exists(dump_path), 'a failed save must not create the final dump file'


def test_save_failure_does_not_corrupt_a_previous_successful_snapshot(
        monkeypatch, make_live_server, tmp_path):
    """A SAVE that fails must not clobber a prior successful snapshot --
    the atomic-write pattern (temp file + os.replace) means the rename
    only ever happens after a fully successful write, so an earlier good
    dump file must survive a later failed SAVE untouched.
    """
    import pickle as pickle_module

    dump_path = str(tmp_path / 'data.rdb')
    server = make_live_server(dump_path=dump_path)
    client = server.client()
    client.set('k', 'good-value')
    assert client.save() == b'OK'

    with open(dump_path, 'rb') as fh:
        good_snapshot_bytes = fh.read()

    orig_dump = pickle_module.dump

    def failing_dump(*args, **kwargs):
        raise OSError('simulated disk failure')

    monkeypatch.setattr(pickle_module, 'dump', failing_dump)
    client.set('k', 'this-should-never-reach-disk')
    with pytest.raises(Exception):
        client.save()
    monkeypatch.setattr(pickle_module, 'dump', orig_dump)

    client._socket.close()

    with open(dump_path, 'rb') as fh:
        assert fh.read() == good_snapshot_bytes


def test_save_failure_crashes_only_that_connection_known_bug(
        monkeypatch, make_live_server, tmp_path):
    """Documents a genuine defect, not desired behavior.

    Server.save() re-raises on failure; that's a plain OSError, which
    connection_handler does not catch (it only catches CommandError), so
    the client gets no reply and the connection is dropped instead of a
    clean `-ERR` -- the same class of bug as the wrong-arity case in
    test_commands.py, reached via a different path. The server process
    itself is unaffected: other connections keep working.
    """
    import pickle as pickle_module

    dump_path = str(tmp_path / 'data.rdb')
    server = make_live_server(dump_path=dump_path)

    def failing_dump(*args, **kwargs):
        raise OSError('simulated disk failure')

    monkeypatch.setattr(pickle_module, 'dump', failing_dump)

    broken = server.client()
    with pytest.raises(Exception):
        broken.save()
    broken._socket.close()

    monkeypatch.undo()

    healthy = server.client()
    try:
        assert healthy.set('still', 'alive') == b'OK'
    finally:
        healthy._socket.close()
