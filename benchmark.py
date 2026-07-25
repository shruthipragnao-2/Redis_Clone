import argparse
import threading
import time

from client import Client


def bench_sequential(n, host, port):
    client = Client(host, port)
    client.flush()

    start = time.perf_counter()
    for i in range(n):
        client.set('key:%d' % i, 'value:%d' % i)
    set_elapsed = time.perf_counter() - start

    start = time.perf_counter()
    for i in range(n):
        client.get('key:%d' % i)
    get_elapsed = time.perf_counter() - start

    return set_elapsed, get_elapsed


def _worker(n, host, port, idx, results):
    client = Client(host, port)
    start = time.perf_counter()
    for i in range(n):
        client.set('worker:%d:%d' % (idx, i), 'value')
        client.get('worker:%d:%d' % (idx, i))
    results[idx] = time.perf_counter() - start


def bench_concurrent(n_per_client, n_clients, host, port):
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
    return total_elapsed, total_ops


def main():
    parser = argparse.ArgumentParser(description='Benchmark the redis clone server.')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=31337)
    parser.add_argument('-n', type=int, default=5000,
                         help='number of ops for the sequential benchmark')
    parser.add_argument('--clients', type=int, default=8,
                         help='concurrent client threads for the concurrent benchmark')
    parser.add_argument('--per-client', type=int, default=1000,
                         help='ops per client thread for the concurrent benchmark')
    args = parser.parse_args()

    print('Sequential: %d SET + %d GET on a single connection' % (args.n, args.n))
    set_elapsed, get_elapsed = bench_sequential(args.n, args.host, args.port)
    print('  SET: %10.2f ops/sec (%.3fs total)' % (args.n / set_elapsed, set_elapsed))
    print('  GET: %10.2f ops/sec (%.3fs total)' % (args.n / get_elapsed, get_elapsed))

    print()
    print('Concurrent: %d threads x %d ops (SET+GET) each' % (args.clients, args.per_client))
    total_elapsed, total_ops = bench_concurrent(
        args.per_client, args.clients, args.host, args.port)
    print('  %10.2f ops/sec (%.3fs total, %d ops)' %
          (total_ops / total_elapsed, total_elapsed, total_ops))


if __name__ == '__main__':
    main()
