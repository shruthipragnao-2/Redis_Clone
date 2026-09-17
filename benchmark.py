"""Benchmark harness for the redis clone server.

Methodology, documented explicitly since none of this is obvious from the
numbers alone:

  Environment
      Printed at the start of every run: OS/platform, Python version,
      gevent version, and CPU count. Throughput on this kind of
      single-threaded, event-loop server is sensitive to all of these
      (see the "known bottlenecks" note at the bottom of this file), so a
      number without them attached is not reproducible or comparable.

  Workload / operation
      Four workloads, chosen to isolate different costs:
        - Sequential SET: one connection, n SETs back to back.
        - Sequential GET: same connection, n GETs of the keys SET above.
        - Sequential mixed SET/GET: one connection, n (SET, GET) pairs on
          the *same* key, interleaved -- unlike the two phases above,
          this never lets the OS/network pipeline a run of same-typed
          requests, so it exercises command dispatch switching on every
          round trip.
        - Concurrent mixed SET/GET: N independent connections (real OS
          threads, one per connection), each running the same interleaved
          (SET, GET) loop against its *own* keys.

  Payload / key characteristics
      Keys: 'key:<i>' (sequential/mixed) or 'client:<i>:<j>' (concurrent),
      ASCII, single digits growing with i/j -- printed exactly for the n
      used in a given run, since size is not fixed across --n values.
      Values: 'value:<i>' / 'value:<j>' -- same shape as the key, chosen
      so every workload's payload has the same structure and the same
      order of magnitude in size, rather than the previous version's
      inconsistency (sequential embedded the index in the value;
      concurrent used a constant 'value' string for every write). This is
      a small-payload, protocol/dispatch-overhead benchmark: it says
      nothing about performance with large values, which is a different,
      deliberately out-of-scope workload.

  Concurrency
      The concurrent workload uses one OS thread per client connection
      (stdlib `threading` + a plain blocking `socket`-based Client, not
      gevent). Each thread's own socket calls release the GIL while
      blocked on network I/O, so this does produce genuinely overlapping
      I/O across threads -- but the *server* being benchmarked is a
      single gevent hub on one OS thread, so no amount of client-side
      concurrency causes parallel command execution on the server side
      (see the bottleneck note below). Each concurrent worker also only
      ever touches its own keys, so this workload measures concurrent
      *connection/dispatch* throughput, not lock or hot-key contention --
      there is nothing resembling a lock in this implementation to
      contend over.

  Trials / warm-up / measurement interval
      Every workload runs one full discarded warm-up pass before any
      timed trial, then `--trials` timed repeats (default 5). Because the
      warm-up pass already SETs the full key range once, the timed trials
      measure steady-state overwrite/read of an already-sized dict, not
      dict growth from empty -- deliberate, since "growing from empty" is
      a one-time cost that would otherwise dominate a low `--trials`
      count. Each trial is timed as a single time.perf_counter() interval
      around the *entire* batch of operations (not per-operation), which
      amortizes Python-level timer overhead across the batch; the
      trade-off is that this reports throughput and mean latency, not a
      real per-operation latency distribution (no p50/p99) -- doing that
      properly would need per-call timing, which was deliberately left
      out here as a distinct, heavier-weight benchmark this file doesn't
      attempt.

  Throughput calculation
      ops / elapsed_seconds, where ops counts every command sent
      (a SET+GET pair counts as 2). For the concurrent workload, ops is
      the sum across all threads and elapsed is the wall-clock time for
      every thread to finish -- i.e. aggregate cluster throughput bounded
      by the slowest thread, not a sum of each thread's own throughput
      (summing per-thread rates would double-count the same wall-clock
      window and overstate the result).

  Statistics reported
      min / mean / median / max ops/sec across trials, plus the derived
      mean per-operation latency in microseconds (1e6 / mean ops/sec).
      min/max are included specifically to surface trial-to-trial noise;
      a wide min-max spread on a quiet machine usually means something
      else (another process, thermal throttling, antivirus, a debugger)
      was interfering during that run, and the fix is to rerun in a
      quieter environment, not to raise --trials until the noise averages
      out.
"""
import argparse
import os
import platform
import statistics
import sys
import threading
import time

from client import Client

try:
    import gevent
    _GEVENT_VERSION = gevent.__version__
except Exception:
    _GEVENT_VERSION = 'unknown'


def _print_environment():
    print('Environment:')
    print('  platform     : %s' % platform.platform())
    print('  python       : %s (%s)' % (platform.python_version(), sys.implementation.name))
    print('  gevent       : %s' % _GEVENT_VERSION)
    print('  cpu_count    : %s' % (os.cpu_count(),))
    print()


# --- Sequential: SET phase, then GET phase (isolates per-op-type cost) ---

def _timed_set_get(client, n):
    start = time.perf_counter()
    for i in range(n):
        client.set('key:%d' % i, 'value:%d' % i)
    set_elapsed = time.perf_counter() - start

    start = time.perf_counter()
    for i in range(n):
        client.get('key:%d' % i)
    get_elapsed = time.perf_counter() - start

    return set_elapsed, get_elapsed


def bench_sequential(n, trials, host, port):
    client = Client(host, port)
    client.flush()

    _timed_set_get(client, n)  # Warm-up pass, discarded.

    set_ops, get_ops = [], []
    for _ in range(trials):
        set_elapsed, get_elapsed = _timed_set_get(client, n)
        set_ops.append(n / set_elapsed)
        get_ops.append(n / get_elapsed)

    return set_ops, get_ops


# --- Sequential mixed: SET then GET on the same key, interleaved every round trip ---

def _timed_mixed(client, n):
    start = time.perf_counter()
    for i in range(n):
        client.set('key:%d' % i, 'value:%d' % i)
        client.get('key:%d' % i)
    elapsed = time.perf_counter() - start
    return elapsed


def bench_mixed(n, trials, host, port):
    client = Client(host, port)
    client.flush()

    _timed_mixed(client, n)  # Warm-up pass, discarded.

    ops = []
    for _ in range(trials):
        elapsed = _timed_mixed(client, n)
        ops.append((n * 2) / elapsed)

    return ops


# --- Concurrent: N connections, each running its own interleaved SET/GET loop ---

def _worker(n, host, port, idx, results):
    client = Client(host, port)
    start = time.perf_counter()
    for i in range(n):
        client.set('client:%d:%d' % (idx, i), 'value:%d' % i)
        client.get('client:%d:%d' % (idx, i))
    results[idx] = time.perf_counter() - start


def _timed_concurrent_round(n_per_client, n_clients, host, port):
    results = [None] * n_clients
    threads = [
        threading.Thread(target=_worker, args=(n_per_client, host, port, i, results))
        for i in range(n_clients)
    ]

    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_elapsed = time.perf_counter() - start

    total_ops = n_per_client * n_clients * 2  # one SET + one GET per iteration
    return total_ops / total_elapsed


def bench_concurrent(n_per_client, n_clients, trials, host, port):
    _timed_concurrent_round(n_per_client, n_clients, host, port)  # Warm-up round, discarded.

    return [
        _timed_concurrent_round(n_per_client, n_clients, host, port)
        for _ in range(trials)
    ]


def _report(label, samples_ops_per_sec):
    mean_ops = statistics.mean(samples_ops_per_sec)
    stdev_ops = statistics.stdev(samples_ops_per_sec) if len(samples_ops_per_sec) > 1 else 0.0
    mean_latency_us = 1e6 / mean_ops
    print('  %-24s min %10.2f  mean %10.2f  median %10.2f  max %10.2f  '
          'stdev %9.2f  ops/sec   (%.2f us/op mean)' % (
              label,
              min(samples_ops_per_sec),
              mean_ops,
              statistics.median(samples_ops_per_sec),
              max(samples_ops_per_sec),
              stdev_ops,
              mean_latency_us))


def main():
    parser = argparse.ArgumentParser(description='Benchmark the redis clone server.')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=31337)
    parser.add_argument('-n', type=int, default=5000,
                         help='ops per trial for the sequential/mixed benchmarks')
    parser.add_argument('--clients', type=int, default=8,
                         help='concurrent client threads for the concurrent benchmark')
    parser.add_argument('--per-client', type=int, default=1000,
                         help='ops per client thread for the concurrent benchmark')
    parser.add_argument('--trials', type=int, default=5,
                         help='timed trials per measurement, after one discarded warm-up pass')
    args = parser.parse_args()

    _print_environment()

    last_key = 'key:%d' % (args.n - 1)
    last_value = 'value:%d' % (args.n - 1)
    print('Payload: keys like %r (%d-%d bytes), values like %r (%d-%d bytes)' % (
        'key:0', len('key:0'), len(last_key),
        'value:0', len('value:0'), len(last_value)))
    print()

    print('Sequential SET/GET: %d SET then %d GET per trial, %d trials (+1 warm-up)' %
          (args.n, args.n, args.trials))
    set_ops, get_ops = bench_sequential(args.n, args.trials, args.host, args.port)
    _report('SET', set_ops)
    _report('GET', get_ops)
    print()

    print('Sequential mixed SET/GET: %d interleaved (SET,GET) pairs per trial, '
          '%d trials (+1 warm-up)' % (args.n, args.trials))
    mixed_ops = bench_mixed(args.n, args.trials, args.host, args.port)
    _report('Mixed SET/GET', mixed_ops)
    print()

    print('Concurrent mixed SET/GET: %d threads x %d (SET,GET) pairs each, '
          '%d trials (+1 warm-up)' % (args.clients, args.per_client, args.trials))
    concurrent_ops = bench_concurrent(
        args.per_client, args.clients, args.trials, args.host, args.port)
    _report('Concurrent SET/GET', concurrent_ops)


if __name__ == '__main__':
    main()
