import argparse
import statistics
import threading
import time

from client import Client


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


def _worker(n, host, port, idx, results):
    client = Client(host, port)
    start = time.perf_counter()
    for i in range(n):
        client.set('worker:%d:%d' % (idx, i), 'value')
        client.get('worker:%d:%d' % (idx, i))
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
    print('  %-10s min %10.2f  median %10.2f  max %10.2f  ops/sec' % (
        label,
        min(samples_ops_per_sec),
        statistics.median(samples_ops_per_sec),
        max(samples_ops_per_sec)))


def main():
    parser = argparse.ArgumentParser(description='Benchmark the redis clone server.')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=31337)
    parser.add_argument('-n', type=int, default=5000,
                         help='ops per trial for the sequential benchmark')
    parser.add_argument('--clients', type=int, default=8,
                         help='concurrent client threads for the concurrent benchmark')
    parser.add_argument('--per-client', type=int, default=1000,
                         help='ops per client thread for the concurrent benchmark')
    parser.add_argument('--trials', type=int, default=5,
                         help='timed trials per measurement, after one discarded warm-up pass')
    args = parser.parse_args()

    print('Sequential: %d SET + %d GET per trial, %d trials (+1 warm-up)' %
          (args.n, args.n, args.trials))
    set_ops, get_ops = bench_sequential(args.n, args.trials, args.host, args.port)
    _report('SET', set_ops)
    _report('GET', get_ops)

    print()
    print('Concurrent: %d threads x %d ops (SET+GET) each, %d trials (+1 warm-up)' %
          (args.clients, args.per_client, args.trials))
    concurrent_ops = bench_concurrent(
        args.per_client, args.clients, args.trials, args.host, args.port)
    _report('SET+GET', concurrent_ops)


if __name__ == '__main__':
    main()
