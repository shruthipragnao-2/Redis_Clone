# Redis_Clone

A miniature Redis-protocol-compatible server in Python, built by following
[Charles Leifer's tutorial](https://charlesleifer.com/blog/building-a-simple-redis-server-with-python/).
It implements RESP (REdis Serialization Protocol) over a `gevent`-based TCP
server, with an in-memory key/value store and six commands: `GET`, `SET`,
`DELETE`, `FLUSH`, `MGET`, `MSET`.

## Running

```
pip install -r requirements.txt
python server.py
```

By default the server listens on `127.0.0.1:31337` (a non-standard port so it
won't clash with a real Redis instance on 6379).

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
```

Or with the bundled Python client:

```
python client.py
```

