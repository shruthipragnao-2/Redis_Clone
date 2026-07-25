# Redis_Clone

A miniature Redis-protocol-compatible server in Python, originally built by
following [Charles Leifer's tutorial](https://charlesleifer.com/blog/building-a-simple-redis-server-with-python/)
and extended with persistence, transactions, and a benchmark harness. It
implements RESP (REdis Serialization Protocol) over a `gevent`-based TCP
server, with an in-memory key/value store.

Commands: `GET`, `SET`, `DELETE`, `FLUSH`, `MGET`, `MSET`, `SAVE`, `MULTI`,
`EXEC`, `DISCARD`.

## Running

```
pip install -r requirements.txt
python server.py
```

By default the server listens on `127.0.0.1:31337` (a non-standard port so it
won't clash with a real Redis instance on 6379). Options:

```
python server.py --host 127.0.0.1 --port 31337 --dump-path dump.rdb --autosave 30
```

`--autosave N` snapshots the dataset to disk every `N` seconds in the
background; without it, persistence only happens when a client sends `SAVE`.
On startup, the server automatically reloads `dump.rdb` (or whatever
`--dump-path` points to) if it exists.

## Talking to it

With the real `redis-cli`:

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

Or with the bundled Python client:

```
python client.py
```

## Transactions

`MULTI` puts the connection into a queuing state: every subsequent command
gets a `QUEUED` reply instead of being run immediately. `EXEC` runs the
queued commands in order and returns an array of their results; `DISCARD`
throws the queue away instead. This is handled per-connection in
`Server.connection_handler`, so concurrent clients don't interfere with each
other's transactions.

## Persistence

`SAVE` pickles the entire in-memory dict to `dump.rdb` (or `--dump-path`).
The server reloads that file automatically on startup. This is a full
synchronous snapshot (like Redis's `SAVE`, not `BGSAVE`) — fine for this
project's scale, but it will block other clients briefly on a large dataset.

## Benchmarking

With the server running:

```
python benchmark.py
python benchmark.py -n 20000 --clients 16 --per-client 2000
```

Reports sequential (single-connection) SET/GET throughput and concurrent
(multi-threaded, multi-connection) throughput in ops/sec.

