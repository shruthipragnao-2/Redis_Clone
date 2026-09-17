# Redis Clone

A small Redis-protocol server in Python, originally built by following
[Charles Leifer's tutorial](https://charlesleifer.com/blog/building-a-simple-redis-server-with-python/)
and since extended with protocol hardening, transactions, snapshot
persistence, a test suite, and a benchmark harness. Everything the server
does lives in `server.py` (~410 lines); `client.py` is a minimal client
built on the same protocol code; `benchmark.py` drives load against a
running server.

## 1. What this implements

A single-process, in-memory key/value store speaking a subset of RESP
(REdis Serialization Protocol) over TCP: 9 commands (§7) on a
`dict[bytes, bytes]` store, `MULTI`/`EXEC`/`DISCARD` transactions isolated
per connection, and manual (`SAVE`) or interval-based (`--autosave`)
snapshotting via `pickle`. Not an implementation of Redis's full command
set, data model, or replication/clustering — see §15.

## 2. Architecture

```
   Clients            TCP :31337        gevent StreamServer + Pool
 (client.py,      ──────────────────►   one greenlet per connection
  redis-cli,                                       │
  benchmark.py)                                     ▼
                                     ProtocolHandler: RESP decode/encode
                                                     │
                                                     ▼
                                Server.connection_handler
                          (per-connection MULTI/EXEC queue state)
                                                     │
                                                     ▼
                          command dispatch → self._kv
                        (one in-memory dict, all connections)
                                                     │
                                          SAVE → pickle snapshot
                                                     ▼
                                dump.rdb (temp file + os.replace)
```

Everything above `self._kv` runs in **one OS thread** — see §6.

## 3. Request lifecycle

Per connection, `connection_handler` loops: read one RESP frame
(`ProtocolHandler.handle_request`) → parse the command name → if inside
`MULTI`, queue it and reply `QUEUED`; otherwise dispatch it
(`Server.get_response`) against `self._kv` → write the response
(`write_response`) → repeat until disconnect. A malformed frame is a
special case — see §4.

## 4. RESP implementation

Supports simple strings, errors, integers, bulk strings (incl. null,
`$-1`), arrays (incl. nested, null `*-1`), and a dict/map type (`%`) no
command actually returns. Declared bulk-string lengths are capped at
512 MiB and array/dict element counts at 1,048,576 (Redis's own
defaults), and any single line is capped at 64 KiB — all rejected
immediately from the header, before reading a body that size. Reads loop
until satisfied rather than assuming one `recv()` returns everything, so
partial reads and multiple pipelined commands sharing one TCP read both
work by construction.

A **framing-level** error (bad length, bad terminator, unknown type byte)
desyncs the byte stream, so the connection gets one error reply and
closes. A **command-level** error (unknown command, bad UTF-8 command
name) leaves framing intact, so the connection stays open.

## 5. TCP/networking model

`gevent.server.StreamServer` + `gevent.pool.Pool` (`max_clients=64`
default) — connections beyond that queue for a free slot rather than
being rejected. Each connection gets its own buffered socket stream and
its own greenlet. No read/write timeouts anywhere, and no cap on how many
connections can be *accepted* — see §15.

## 6. Concurrency model

Single-threaded: one `gevent` hub, one OS thread, many greenlets. A
command handler is pure in-memory dict manipulation with no I/O, so it
never yields mid-command — which is what would let another connection
interleave with it. The same holds for a whole `MULTI`/`EXEC` block. That
gives every command, and every transaction, atomicity across connections
with zero locking code — stress-tested directly (not just inferred) in
`tests/test_concurrency.py`. The trade-off: one OS thread means no
parallel *execution* across connections, ever — see the scaling data in
§12.

## 7. Supported commands

| Command | Behavior |
|---|---|
| `GET key` | Value, or null bulk string if absent |
| `SET key value` | Always returns `OK` |
| `DELETE`/`DEL key` | `1` if it existed, `0` otherwise |
| `FLUSH`/`FLUSHDB` | Clears the store, returns count removed |
| `MGET key [key ...]` | Array of values (nulls for absent keys) |
| `MSET key value [...]` | Returns number of pairs set |
| `SAVE` | Synchronous snapshot to disk, returns `OK` |
| `MULTI`/`EXEC`/`DISCARD` | Transaction control — §8 |

No `EXPIRE`/TTL, no data types beyond strings, no `AUTH`, no `WATCH`, no
pub/sub, no `HELLO`/RESP3 negotiation.

## 8. Transaction semantics

`MULTI` puts the connection in a queuing state — every command gets
`QUEUED` instead of running. `EXEC` runs the queue in order and returns
an array of results (a failed command's error sits in that array without
aborting the rest, matching Redis's own no-rollback behavior). `DISCARD`
drops the queue. Nested `MULTI`, and `EXEC`/`DISCARD` without one, error.

Queue state is a local variable inside `connection_handler`, scoped to
one connection's greenlet, so transactions can't see or interfere with
each other — verified under real concurrent load in
`tests/test_transactions.py` and `tests/test_concurrency.py`. One
deviation from Redis: commands are queued with no arity/syntax check, so
a bad command only surfaces as an error inside the `EXEC` array — there's
no `EXECABORT`.

## 9. Persistence mechanism

`SAVE` copies the store (`dict(self._kv)`, one atomic operation), pickles
it to a temp file in the same directory, then `os.replace()`s it into
place (`dump.rdb` by default). Startup loads that file if present.
`--autosave N` runs this on a timer; without it, only `SAVE` persists.

Verified by failure injection (`tests/test_persistence_failures.py`), not
just review: a `SAVE` that fails at any point leaves the on-disk file
exactly as it was, cleans up its temp file, and a dump file that can't be
unpickled (or unpickles to the wrong type) makes the server start empty
instead of crashing.

**Not** guaranteed: no `fsync()`, so no durability claim across a real
power loss or OS crash — only against exceptions within the same
process. A crash between the write and the rename leaves an orphaned temp
file nothing cleans up. `SAVE` is fully synchronous (like Redis's `SAVE`,
not `BGSAVE`) — on a large store it blocks that connection, and per §6,
every other connection too, for the duration of the write.

## 10. Testing strategy

144 tests pass, 3 skip (`redis-cli` interop, when that binary isn't on
`PATH`) — see §18.

| File | Covers |
|---|---|
| `test_protocol.py` | RESP decode/encode in isolation |
| `test_commands.py` | Every command, unknown commands, arity edge cases |
| `test_transactions.py` | `MULTI`/`EXEC`/`DISCARD`, per-connection isolation |
| `test_connections.py` | Pipelining, fragmented reads, multiple clients |
| `test_persistence.py` | Save/load round trips, corrupt-file fallback |
| `test_persistence_failures.py` | Failure injection: write/rename failures |
| `test_concurrency.py` | Real concurrent-client stress tests |
| `test_lifecycle.py` | Startup load behavior, shutdown |
| `test_redis_cli_interop.py` | Best-effort checks against real `redis-cli` |

Tests run a real `Server` on an ephemeral port with a throwaway dump
directory — never the project's real `dump.rdb`. CI
(`.github/workflows/tests.yml`) runs the same suite on every push and PR.

## 11. Benchmark methodology

Four workloads: sequential `SET`, sequential `GET`, sequential interleaved
`SET`/`GET` (one connection), concurrent interleaved `SET`/`GET`
(`--clients` OS threads). One discarded warm-up pass, then `--trials`
timed passes (default 5), each timed as one `perf_counter()` interval
around the whole batch — this gives throughput and mean latency, not a
real per-call p50/p99 distribution. Reports min/mean/median/max ops/sec
plus stdev, so noise is visible rather than hidden. Full detail (payload
sizes, why concurrent throughput is aggregate ops/wall-clock rather than
summed per-thread rates) is in `benchmark.py`'s docstring.

## 12. Actual benchmark results

Windows 11, Python 3.13.14, gevent 26.7.0, 24 logical CPUs, `n=5000`, 5
trials (3 for the scaling sweep). One machine's numbers, not a portable
performance claim.

| Workload | Ops/sec (median) | Mean latency |
|---|---|---|
| Sequential SET (1 conn.) | ~21,800 | ~46 µs/op |
| Sequential GET (1 conn.) | ~21,900 | ~45 µs/op |
| Sequential mixed SET/GET (1 conn.) | ~22,500 | ~45 µs/op |
| Concurrent mixed SET/GET (8 conns.) | ~27,900 | ~36 µs/op |

Scaling (`--per-client 500 --trials 3`), median ops/sec:

| Clients | 1 | 2 | 4 | 8 | 16 | 32 | 64 |
|---|---|---|---|---|---|---|---|
| Ops/sec | ~22,100 | ~24,500 | ~27,200 | ~27,900 | ~27,000 | ~27,100 | ~26,600 |

Rises through ~8 connections, then flattens and drifts down by 64 — one
OS thread (§6) means no parallel execution to scale into.

## 13. Example `redis-cli` usage

```
redis-cli -p 31337
127.0.0.1:31337> SET foo bar
127.0.0.1:31337> GET foo
127.0.0.1:31337> MSET a 1 b 2
127.0.0.1:31337> MGET a b
127.0.0.1:31337> DEL foo
127.0.0.1:31337> FLUSHDB
127.0.0.1:31337> SAVE
127.0.0.1:31337> MULTI
127.0.0.1:31337> SET x 1
127.0.0.1:31337> SET y 2
127.0.0.1:31337> EXEC
```
`tests/test_redis_cli_interop.py` runs equivalent checks against a real
`redis-cli` when available (skipped otherwise, including in this repo's
own CI) — the commands above are documented usage, not a compatibility
claim beyond what those tests check.

## 14. Design tradeoffs

- **One thread, one event loop** (§6): free atomicity for every command
  and transaction, no locking code — at the cost of zero parallel
  execution across cores, regardless of client count.
- **`pickle` for persistence** (§9): trivial to implement correctly, but
  ties the file format to Python object pickling (not portable) and
  makes loading an untrusted dump file unsafe (`pickle.load` can execute
  arbitrary code).
- **Synchronous `SAVE`, no `BGSAVE`**: simpler than a background writer,
  at the cost of blocking every connection for the write's duration.
- **A deliberately small command set**: the whole implementation stays
  readable in one sitting, at the cost of not covering anything beyond
  the 9 commands in §7.

## 15. Known limitations

- **Wrong-arity commands drop the connection.** No argument-count check
  before dispatch; a `TypeError` isn't caught, so the client gets no
  reply. Pinned by `test_commands.py::test_wrong_arity_crashes_the_connection_known_bug`.
- **A failed `SAVE` closes the connection** the same way (same root
  cause). Pinned in `test_persistence_failures.py`.
- **`MSET` with an odd argument count silently drops the trailing key**
  instead of erroring (`zip()` truncates).
- **No auth, no TLS, no per-connection timeouts.** Any TCP-reachable
  client can read/write/`SAVE` everything; a slow or idle connection can
  hold a pool slot indefinitely. Not for an untrusted network.
- **No cleanup of an orphaned save temp file** left by a process killed
  mid-save (§9) — harmless to correctness, not to long-run disk usage.
- **RESP subset, no RESP3 negotiation** (no `HELLO`/`COMMAND`/`PING`) —
  a client library that probes for these first may not work here even
  where basic framing is compatible.
- Benchmarks (§12) reflect one Windows machine on 2026-09-17, not a
  cross-platform or cross-version performance claim.

## 16. Setup

```
pip install -r requirements.txt
```

## 17. Running the server

```
python server.py
python server.py --host 127.0.0.1 --port 31337 --dump-path dump.rdb --autosave 30
```
Listens on `127.0.0.1:31337` by default (non-standard, won't clash with a
real Redis on 6379), loads `dump.rdb` on startup if present, persists
only on `SAVE` unless `--autosave N` is given.

## 18. Running tests

```
pip install -r requirements.txt -r requirements-test.txt
pytest
```
Same command CI runs (§10) on every push and pull request.

## 19. Running benchmarks

With the server running:
```
python benchmark.py
python benchmark.py -n 20000 --clients 16 --per-client 2000
```
See §11-§12 for methodology and results.
