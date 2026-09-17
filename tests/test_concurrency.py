"""Concurrency/stress tests for the actual implementation.

This server's concurrency model (unchanged by this file) is: one OS
thread running a gevent event loop, with one greenlet per connection
(server.py Server.__init__: StreamServer + Pool). A command handler
(get/set/delete/mget/mset/flush) is pure in-memory dict manipulation with
no I/O, so it never yields to the event loop mid-command -- which is what
would make it interleave with another connection's greenlet. The same
holds for a whole MULTI/EXEC block (server.py connection_handler's EXEC
branch loops over queued commands calling get_response(), none of which
does I/O either).

The tests below don't assert *why* that's true (that's an implementation
detail) -- they hammer the *observable* guarantees it implies, using real
concurrent OS-thread clients against a real running server, and would be
expected to fail intermittently if the atomicity assumption above were
ever violated (e.g. by a future command that does I/O mid-handler).

Synchronization is via threading.Barrier (aligned start) and
threading.Event (deterministic stop signal) -- no sleep-based pacing or
timing assumptions. Iteration counts are fixed, not time-boxed, so a run
does the same amount of work every time.
"""
import pickle
import re

import pytest

from conftest import StructuredClient, run_concurrently

N_CLIENTS = 20
N_ITERATIONS = 150


def tag(idx, i):
    """A fixed-width, self-describing value: a corrupted (torn/truncated/
    concatenated) write is very unlikely to still match TAG_RE below."""
    return 'c%02d-i%04d' % (idx, i)


TAG_RE = re.compile(rb'^c\d{2}-i\d{4}$')


# --- Multiple simultaneous clients ---

def test_many_simultaneous_clients_all_get_served(live_server):
    """N clients connect and issue a request at (as close to) the same
    instant as threading.Barrier can arrange, and every one of them must
    get correctly served -- not merely "not crash", but the right value
    back for the right key.
    """
    def worker(i):
        c = live_server.client()
        try:
            key = 'client:%d' % i
            value = 'value:%d' % i
            assert c.set(key, value) == b'OK'
            assert c.get(key) == value.encode()
        finally:
            c._socket.close()

    run_concurrently(worker, N_CLIENTS)


# --- Concurrent reads ---

def test_concurrent_reads_of_a_stable_key_are_all_consistent(live_server):
    """Many readers hammering a key nobody is mutating must all see the
    same, correct, uncorrupted value -- every single time."""
    setup = live_server.client()
    setup.set('stable', 'unchanging-value')
    setup._socket.close()

    def worker(i):
        c = live_server.client()
        try:
            for _ in range(N_ITERATIONS):
                assert c.get('stable') == b'unchanging-value'
        finally:
            c._socket.close()

    run_concurrently(worker, N_CLIENTS)


# --- Concurrent writes: distinct keys (no lost updates / no cross-wiring) ---

def test_concurrent_writes_to_distinct_keys_have_no_lost_updates(live_server):
    """Each thread owns one key and repeatedly overwrites it. Nothing
    else ever touches that key, so after all threads finish, every key
    must hold exactly that thread's last write -- proving concurrent
    writes to different keys never clobber each other or land on the
    wrong key.
    """
    def worker(i):
        c = live_server.client()
        try:
            key = 'owned:%d' % i
            for iteration in range(N_ITERATIONS):
                c.set(key, tag(i, iteration))
        finally:
            c._socket.close()

    run_concurrently(worker, N_CLIENTS)

    checker = live_server.client()
    try:
        for i in range(N_CLIENTS):
            expected = tag(i, N_ITERATIONS - 1).encode()
            assert checker.get('owned:%d' % i) == expected
    finally:
        checker._socket.close()


# --- Concurrent writes: same key (no torn values) ---

def test_concurrent_writes_to_the_same_key_never_produce_a_torn_value(live_server):
    """All N_CLIENTS threads hammer the *same* key. We can't predict
    which write is "last" (that's a genuine race by design), but every
    single value that ever lands in the store -- including the final one
    -- must be a complete, unmodified value some thread actually wrote,
    never a mix of two writes. A dict assignment that could be
    interrupted mid-update would eventually produce a string that fails
    TAG_RE under enough contention; this repeats the write/verify cycle
    many times to give that a real chance to show up.
    """
    def worker(i):
        c = live_server.client()
        try:
            for iteration in range(N_ITERATIONS):
                c.set('contended', tag(i, iteration))
        finally:
            c._socket.close()

    for _round in range(5):
        run_concurrently(worker, N_CLIENTS)
        checker = live_server.client()
        try:
            final_value = checker.get('contended')
        finally:
            checker._socket.close()
        assert final_value is not None
        assert TAG_RE.match(final_value), 'torn/corrupted value: %r' % final_value


# --- Mixed concurrent reads and writes (no torn reads) ---

def test_mixed_concurrent_reads_and_writes_never_observe_a_torn_value(live_server):
    """One writer continuously overwrites a key while several readers
    continuously read it. Every read must be either None (before the
    first write ever lands) or a complete, valid tag -- never a partial
    or mixed value. The readers stop via an Event set by the writer when
    it's done, not via a timer.
    """
    import threading

    stop_event = threading.Event()
    read_counts = [0] * (N_CLIENTS - 1)
    observed_bad = []

    def writer(_i):
        c = live_server.client()
        try:
            for iteration in range(N_ITERATIONS * 3):
                c.set('shared', tag(0, iteration))
        finally:
            stop_event.set()
            c._socket.close()

    def reader(i):
        c = live_server.client()
        try:
            count = 0
            while not stop_event.is_set():
                value = c.get('shared')
                count += 1
                if value is not None and not TAG_RE.match(value):
                    observed_bad.append(value)
            read_counts[i - 1] = count
        finally:
            c._socket.close()

    workers = [writer] + [reader] * (N_CLIENTS - 1)

    def dispatch(i):
        workers[i](i)

    run_concurrently(dispatch, N_CLIENTS)

    assert observed_bad == []
    # Sanity check the test actually exercised concurrent reads, so a
    # trivial "readers never got scheduled" scenario can't pass silently.
    assert sum(read_counts) > 0


# --- Independent client / transaction state under concurrency ---

def test_independent_connections_maintain_separate_transaction_state_under_load(live_server):
    """N connections each run their own MULTI/SET/EXEC loop concurrently.
    Server.connection_handler keeps `in_transaction`/`transaction_queue`
    as connection-local variables (not shared/global state) -- if that
    were ever broken, one connection's queued command could leak into
    another's EXEC, or MULTI state could bleed across connections under
    contention. Verified by having each connection write only to its own
    key and checking every connection ends on exactly its own last value.
    """
    def worker(i):
        sc = StructuredClient(live_server.raw_connect())
        try:
            key = 'txn-owned:%d' % i
            for iteration in range(N_ITERATIONS):
                assert sc.call('MULTI') == b'OK'
                assert sc.call('SET', key, tag(i, iteration)) == b'QUEUED'
                result = sc.call('EXEC')
                assert result == [b'OK']
        finally:
            sc.close()

    run_concurrently(worker, N_CLIENTS)

    checker = live_server.client()
    try:
        for i in range(N_CLIENTS):
            expected = tag(i, N_ITERATIONS - 1).encode()
            assert checker.get('txn-owned:%d' % i) == expected
    finally:
        checker._socket.close()


def test_exec_is_atomic_against_concurrent_transactional_readers(live_server):
    """The strongest atomicity claim in this file: one writer repeatedly
    executes `MULTI; SET x N; SET y N; EXEC`, updating two keys together.
    Several reader connections concurrently execute their own
    `MULTI; GET x; GET y; EXEC` -- an atomic *paired* read, from the
    reader's own perspective, of both keys at once.

    If (and only if) the server ever let another connection's request run
    in between the writer's two SETs inside one EXEC, a reader could
    observe x and y out of sync (e.g. x updated, y not yet). Because each
    reader's own EXEC is itself atomic w.r.t. other connections (by the
    same invariant being tested), a plain two-round-trip GET/GET pair
    would give false-positive mismatches from the *reader's* round-trip
    gap rather than a real server bug -- wrapping the reader's read in its
    own MULTI/EXEC removes that confound.
    """
    import threading

    stop_event = threading.Event()
    mismatches = []
    read_counts = [0] * (N_CLIENTS - 1)
    iterations = N_ITERATIONS * 5

    def writer(_i):
        sc = StructuredClient(live_server.raw_connect())
        try:
            for i in range(iterations):
                value = str(i).encode()
                sc.call('MULTI')
                sc.call('SET', b'x', value)
                sc.call('SET', b'y', value)
                result = sc.call('EXEC')
                assert result == [b'OK', b'OK']
        finally:
            stop_event.set()
            sc.close()

    def reader(idx):
        sc = StructuredClient(live_server.raw_connect())
        try:
            count = 0
            while not stop_event.is_set():
                sc.call('MULTI')
                sc.call('GET', b'x')
                sc.call('GET', b'y')
                x_val, y_val = sc.call('EXEC')
                count += 1
                if x_val != y_val:
                    mismatches.append((x_val, y_val))
            read_counts[idx - 1] = count
        finally:
            sc.close()

    workers = [writer] + [reader] * (N_CLIENTS - 1)

    def dispatch(i):
        workers[i](i)

    run_concurrently(dispatch, N_CLIENTS)

    assert mismatches == [], (
        'observed x/y out of sync -- EXEC was not atomic w.r.t. another '
        'connection: %r' % (mismatches[:5],))
    assert sum(read_counts) > 0


# --- Connection creation and cleanup ---

def test_connection_pool_serves_more_clients_than_max_clients_without_rejecting(make_live_server):
    """max_clients bounds concurrent *service*, not the number of clients
    that can ever connect: excess connections should queue for a free
    slot (gevent Pool backpressure) rather than being refused. Bounded by
    each socket's own connect/recv timeout, not a sleep.
    """
    server = make_live_server(max_clients=2)
    n_connections = 8

    def worker(i):
        c = server.client()
        try:
            key = 'pool:%d' % i
            assert c.set(key, 'v') == b'OK'
            assert c.get(key) == b'v'
        finally:
            c._socket.close()

    run_concurrently(worker, n_connections)


def test_connection_churn_does_not_exhaust_the_pool(make_live_server):
    """Repeatedly open, use, and close a connection against a
    deliberately tiny pool. If closing a connection didn't free its Pool
    slot, this would deadlock (or time out) well before reaching
    n_churns, since n_churns > max_clients.
    """
    server = make_live_server(max_clients=2)
    n_churns = 20

    for i in range(n_churns):
        c = server.client()
        try:
            assert c.set('churn', str(i)) == b'OK'
            assert c.get('churn') == str(i).encode()
        finally:
            c._socket.close()


def test_concurrent_connection_churn_leaves_server_responsive(live_server):
    """Many clients concurrently connect, do a little work, and disconnect
    -- repeated over several concurrent rounds -- and the server must
    still be cleanly usable afterwards (no accumulated deadlock or
    resource exhaustion within this test's scope).
    """
    def worker(i):
        for iteration in range(10):
            c = live_server.client()
            try:
                key = 'churn:%d' % i
                c.set(key, str(iteration))
                assert c.get(key) == str(iteration).encode()
            finally:
                c._socket.close()

    run_concurrently(worker, N_CLIENTS)

    final = live_server.client()
    try:
        assert final.set('after-churn', 'ok') == b'OK'
    finally:
        final._socket.close()


# --- Concurrent persistence ---

def test_concurrent_save_never_produces_a_corrupt_or_torn_snapshot(make_live_server, tmp_path):
    """Several writers hammer distinct keys while a separate thread calls
    SAVE repeatedly in between writes. Server.save() snapshots via
    dict(self._kv) -- a single atomic dict-copy operation, per this
    implementation's cooperative-scheduling invariant -- before doing any
    file I/O, so every snapshot it ever writes must load back cleanly and
    contain only complete, valid values (never a value from a write that
    was only half-applied).
    """
    dump_path = str(tmp_path / 'concurrent.rdb')
    server = make_live_server(dump_path=dump_path)
    n_writers = N_CLIENTS - 1
    saves_per_thread = 10

    def writer(i):
        c = server.client()
        try:
            for iteration in range(N_ITERATIONS):
                c.set('cp:%d' % i, tag(i, iteration))
        finally:
            c._socket.close()

    def saver(_i):
        c = server.client()
        try:
            for _ in range(saves_per_thread):
                assert c.save() == b'OK'
                with open(dump_path, 'rb') as fh:
                    snapshot = pickle.load(fh)  # must never raise
                for key, value in snapshot.items():
                    if key.startswith(b'cp:'):
                        assert TAG_RE.match(value), (
                            'torn value in on-disk snapshot: %r=%r' % (key, value))
        finally:
            c._socket.close()

    workers = [saver] + [writer] * n_writers

    def dispatch(i):
        workers[i](i)

    run_concurrently(dispatch, N_CLIENTS)


def test_final_save_after_concurrent_writes_matches_final_state(make_live_server, tmp_path):
    """After all concurrent writers have finished (joined), one clean
    SAVE's on-disk snapshot must exactly match the final in-memory state
    -- concurrent writing earlier must not have left the store (or a
    subsequent clean snapshot of it) in an inconsistent condition.
    """
    dump_path = str(tmp_path / 'final.rdb')
    server = make_live_server(dump_path=dump_path)

    def worker(i):
        c = server.client()
        try:
            for iteration in range(N_ITERATIONS):
                c.set('fp:%d' % i, tag(i, iteration))
        finally:
            c._socket.close()

    run_concurrently(worker, N_CLIENTS)

    finisher = server.client()
    try:
        assert finisher.save() == b'OK'
    finally:
        finisher._socket.close()

    with open(dump_path, 'rb') as fh:
        snapshot = pickle.load(fh)

    for i in range(N_CLIENTS):
        key = ('fp:%d' % i).encode()
        expected = tag(i, N_ITERATIONS - 1).encode()
        assert snapshot.get(key) == expected
