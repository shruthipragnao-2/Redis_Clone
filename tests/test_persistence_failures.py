"""Focused failure-injection tests for Server.save()/Server._load().

Implementation under test (server.py, unchanged mechanism):

    def save(self):
        snapshot = dict(self._kv)
        directory = os.path.dirname(os.path.abspath(self._dump_path))
        fd, tmp_path = tempfile.mkstemp(dir=directory, prefix='.dump-', suffix='.tmp')
        try:
            with os.fdopen(fd, 'wb') as fh:
                pickle.dump(snapshot, fh, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp_path, self._dump_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
        return SimpleString('OK')

    def _load(self):
        if not os.path.exists(self._dump_path):
            return
        with open(self._dump_path, 'rb') as fh:
            try:
                self._kv = pickle.load(fh)
            except (EOFError, pickle.UnpicklingError):
                self._kv = {}

This file exists to pin down exactly what that snapshot -> temp file ->
os.replace() pattern actually guarantees under failure, not to redesign
it. Every test uses a throwaway tmp_path directory; none ever touch the
project's real dump.rdb.

What this mechanism does NOT provide, and no test here claims otherwise:
  - No fsync() of the temp file or the containing directory, so there is
    no guarantee data survives a real power loss / OS crash (only
    exceptions raised within the same process are covered).
  - No cleanup of an orphaned temp file left behind by a process that
    died between finishing the write and os.replace() -- see
    test_crash_exactly_before_replace_leaves_canonical_file_untouched.
"""
import os
import pickle

import pytest

from server import SimpleString


# --- 1. Serialization fails ---

def test_serialization_failure_leaves_canonical_file_and_disk_state_intact(
        make_live_server, tmp_path):
    """A value that pickle genuinely cannot serialize (not a mock -- an
    object whose __reduce__ raises) exercises the real failure path in
    pickle.dump(), not a simulated one. Note the public protocol can
    never actually put a non-bytes value into the store (every SET value
    arrives as RESP bytes), so this reaches into the store directly to
    force the scenario -- it demonstrates the save()-side failure
    handling itself, independent of how a serialization failure would
    ever come to occur in practice.
    """
    class Unpicklable:
        def __reduce__(self):
            raise TypeError('refuses to be pickled')

    dump_path = str(tmp_path / 'data.rdb')
    server = make_live_server(dump_path=dump_path)
    client = server.client()
    client.set('good-key', 'good-value')
    assert client.save() == b'OK'  # establish a known-good prior snapshot
    with open(dump_path, 'rb') as fh:
        good_snapshot_bytes = fh.read()

    server.server._kv[b'poison'] = Unpicklable()  # reach into internal state
    with pytest.raises(Exception):
        client.save()
    client._socket.close()

    # The prior successful snapshot must be completely untouched.
    with open(dump_path, 'rb') as fh:
        assert fh.read() == good_snapshot_bytes
    # And no orphaned temp file was left behind.
    leftover = [f for f in os.listdir(tmp_path) if f.startswith('.dump-')]
    assert leftover == []


# --- 2. Temporary-file writing fails ---

def test_temp_file_creation_failure_leaves_canonical_file_untouched(
        monkeypatch, make_live_server, tmp_path):
    """tempfile.mkstemp() itself is called *before* save()'s try/except,
    so a failure there propagates immediately with nothing to clean up
    (there's no temp file yet). This pins that specific control-flow
    detail: the try/except only wraps the write+replace, not the temp
    file's creation.
    """
    import tempfile as tempfile_module

    dump_path = str(tmp_path / 'data.rdb')
    with open(dump_path, 'wb') as fh:
        pickle.dump({b'existing': b'value'}, fh)

    server = make_live_server(dump_path=dump_path)
    client = server.client()

    def failing_mkstemp(*args, **kwargs):
        raise OSError('simulated: cannot create temp file')

    monkeypatch.setattr(tempfile_module, 'mkstemp', failing_mkstemp)
    with pytest.raises(Exception):
        client.save()
    monkeypatch.undo()
    client._socket.close()

    with open(dump_path, 'rb') as fh:
        assert pickle.load(fh) == {b'existing': b'value'}
    assert [f for f in os.listdir(tmp_path) if f.startswith('.dump-')] == []


def test_write_failure_after_temp_file_created_cleans_up_and_leaves_canonical_untouched(
        monkeypatch, make_live_server, tmp_path):
    """Simulates an I/O error partway through writing the temp file (e.g.
    disk full) -- as distinct from #1's TypeError-from-bad-data, this is
    an OSError from the write path itself, injected via pickle.dump since
    that's where save() actually writes bytes to the open temp file.
    """
    import pickle as pickle_module

    dump_path = str(tmp_path / 'data.rdb')
    with open(dump_path, 'wb') as fh:
        pickle.dump({b'existing': b'value'}, fh)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    client.set('k', 'v')

    def failing_dump(*args, **kwargs):
        raise OSError('simulated: disk full mid-write')

    monkeypatch.setattr(pickle_module, 'dump', failing_dump)
    with pytest.raises(Exception):
        client.save()
    monkeypatch.undo()
    client._socket.close()

    with open(dump_path, 'rb') as fh:
        assert pickle.load(fh) == {b'existing': b'value'}
    assert [f for f in os.listdir(tmp_path) if f.startswith('.dump-')] == []


# --- 3. os.replace() fails ---

def test_replace_failure_cleans_up_temp_file_and_never_touches_canonical(
        monkeypatch, make_live_server, tmp_path):
    """By the time os.replace() runs, the temp file is fully and validly
    written (pickle.dump already succeeded). If the rename itself then
    fails (e.g. cross-device link, permission error), the valid temp
    file's content must never become visible at the canonical path, and
    any prior canonical content must survive untouched.
    """
    import os as os_module

    dump_path = str(tmp_path / 'data.rdb')
    with open(dump_path, 'wb') as fh:
        pickle.dump({b'old': b'value'}, fh)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    client.set('new', 'value')  # would appear in the snapshot if replace succeeded

    orig_replace = os_module.replace

    def failing_replace(src, dst):
        raise OSError('simulated: cross-device rename failure')

    monkeypatch.setattr(os_module, 'replace', failing_replace)
    with pytest.raises(Exception):
        client.save()
    monkeypatch.setattr(os_module, 'replace', orig_replace)
    client._socket.close()

    # The old canonical content survives, unmodified.
    with open(dump_path, 'rb') as fh:
        assert pickle.load(fh) == {b'old': b'value'}
    # The valid-but-unpromoted temp file was removed, not left behind.
    assert [f for f in os.listdir(tmp_path) if f.startswith('.dump-')] == []


# --- 4. Interrupted before replacement (simulated) ---

def test_crash_exactly_before_replace_leaves_canonical_file_untouched(
        make_live_server, tmp_path):
    """A real process kill between the temp file being fully written and
    os.replace() running can't be triggered portably from inside a test
    process, so this reproduces its *aftermath* directly: manually leave
    a fully-written, validly-pickled temp file in place (mimicking
    exactly what disk state a kill at that exact point would leave)
    without ever calling os.replace().

    This is the core guarantee the temp-file+rename pattern is meant to
    provide: a save that never reaches os.replace() -- for any reason,
    including a crash -- has zero effect on what a (re)started Server
    loads. The leftover temp file is never cleaned up by anything other
    than save()'s own except-block, which never got to run here, so it
    is expected to still be sitting on disk afterwards -- that's a
    disk-hygiene limitation this test documents, not a correctness
    violation (nothing ever reads it back as live data).
    """
    dump_path = str(tmp_path / 'data.rdb')
    with open(dump_path, 'wb') as fh:
        pickle.dump({b'before-crash': b'value'}, fh)

    # Recreate exactly what Server.save() would have left behind: a
    # fully-written temp file with the same naming convention, one step
    # away from being renamed into place.
    orphan_path = str(tmp_path / '.dump-simulated-crash.tmp')
    with open(orphan_path, 'wb') as fh:
        pickle.dump({b'from-the-crashed-save': b'never-should-appear'}, fh)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        # The server must load the last successfully *replaced* state --
        # the orphaned temp file must be completely invisible to it.
        assert client.get('before-crash') == b'value'
        assert client.get('from-the-crashed-save') is None
    finally:
        client._socket.close()

    # Document (not assert as desirable) that the orphan is never swept
    # up -- this implementation has no startup-time temp-file cleanup.
    assert os.path.exists(orphan_path), (
        'orphaned temp file was removed by something -- if a cleanup step '
        'was added, this assertion (and the limitation it documents) '
        'should be revisited')


# --- 5. Canonical file already exists ---

def test_save_replaces_an_existing_canonical_file_completely(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'data.rdb')
    with open(dump_path, 'wb') as fh:
        pickle.dump({b'old-key': b'old-value', b'stays-if-merged': b'x'}, fh)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    client.flush()  # the in-memory store starts fresh regardless of what's on disk
    client.set('new-key', 'new-value')
    assert client.save() == b'OK'
    client._socket.close()

    with open(dump_path, 'rb') as fh:
        on_disk = pickle.load(fh)
    # A full replace, not a merge: the old file's keys are gone.
    assert on_disk == {b'new-key': b'new-value'}


# --- 6. Canonical file missing ---

def test_save_creates_canonical_file_when_none_existed(make_live_server, tmp_path):
    dump_path = str(tmp_path / 'brand-new.rdb')
    assert not os.path.exists(dump_path)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    client.set('k', 'v')
    assert client.save() == b'OK'
    client._socket.close()

    assert os.path.exists(dump_path)
    with open(dump_path, 'rb') as fh:
        assert pickle.load(fh) == {b'k': b'v'}


# --- 7. Corrupt/invalid data in the persistence file ---

def test_empty_file_is_treated_as_corrupt_and_falls_back_to_empty_store(
        make_live_server, tmp_path):
    dump_path = str(tmp_path / 'empty.rdb')
    open(dump_path, 'wb').close()  # 0 bytes -> pickle.load raises EOFError

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        assert client.flush() == 0
    finally:
        client._socket.close()


def test_garbage_bytes_are_treated_as_corrupt_and_fall_back_to_empty_store(
        make_live_server, tmp_path):
    dump_path = str(tmp_path / 'garbage.rdb')
    with open(dump_path, 'wb') as fh:
        fh.write(b'\x00\x01not a pickle stream at all\xff\xfe' * 10)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        assert client.flush() == 0
    finally:
        client._socket.close()


def test_valid_pickle_of_the_wrong_type_falls_back_to_empty_store(make_live_server, tmp_path):
    """Found via failure injection, and fixed (see server.py _load()):
    a syntactically valid pickle stream that decodes to something other
    than a dict (e.g. a list) does not raise EOFError or
    UnpicklingError -- pickle.load() succeeds completely -- so the old
    except clause let it through untouched. self._kv would silently
    become a list, and every command (GET/SET/DELETE/MGET/MSET/FLUSH all
    assume dict semantics) would then raise AttributeError for the
    entire lifetime of the process. _load() now checks the loaded type
    and falls back to {} for anything that isn't a dict, matching the
    graceful-degradation intent the surrounding except clause already
    had for outright unparseable files.
    """
    dump_path = str(tmp_path / 'wrong-type.rdb')
    with open(dump_path, 'wb') as fh:
        pickle.dump([1, 2, 3], fh)

    server = make_live_server(dump_path=dump_path)
    client = server.client()
    try:
        assert client.flush() == 0
        assert client.set('k', 'v') == b'OK'
        assert client.get('k') == b'v'
    finally:
        client._socket.close()


def test_pickle_referencing_an_unimportable_class_falls_back_to_empty_store(
        make_live_server, tmp_path):
    """Found via failure injection, and fixed (see server.py _load()):
    a corrupt/foreign pickle stream that references a class pickle can't
    resolve raises AttributeError (or ImportError/ModuleNotFoundError,
    depending on exactly what's missing) while trying to unpickle it --
    none of which is EOFError or UnpicklingError. The old except clause
    didn't catch it, so it propagated out of _load() and crashed
    Server.__init__() entirely: the server never came up, for any client,
    rather than degrading to an empty store the way the (already
    partially-handled) corrupt-file case was clearly intended to.
    """
    dump_path = str(tmp_path / 'unresolvable-class.rdb')
    with open(dump_path, 'wb') as fh:
        # References a class name pickle cannot find on unpickling --
        # provokes AttributeError deep inside pickle.load(), not a
        # framing-level EOFError/UnpicklingError.
        fh.write(b'c__main__\nNoSuchClassAtAll\nq\x00.\n')

    server = make_live_server(dump_path=dump_path)  # must not raise
    client = server.client()
    try:
        assert client.flush() == 0
    finally:
        client._socket.close()
