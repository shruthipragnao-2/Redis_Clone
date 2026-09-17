"""Best-effort interoperability check against the real redis-cli binary.

Skipped (not failed) when redis-cli isn't on PATH, so the suite stays
deterministic and green in environments without it (this repository's own
dev environment included, at the time of writing).
"""
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which('redis-cli') is None,
    reason='redis-cli not installed; skipping interop test',
)

RECV_TIMEOUT = 5


def _redis_cli(port, *args):
    return subprocess.run(
        ['redis-cli', '-p', str(port)] + list(args),
        capture_output=True, text=True, timeout=RECV_TIMEOUT,
    )


def test_set_and_get(live_server):
    result = _redis_cli(live_server.port, 'SET', 'foo', 'bar')
    assert result.stdout.strip() == 'OK'
    result = _redis_cli(live_server.port, 'GET', 'foo')
    assert result.stdout.strip() == 'bar'


def test_mset_mget(live_server):
    _redis_cli(live_server.port, 'MSET', 'a', '1', 'b', '2')
    result = _redis_cli(live_server.port, 'MGET', 'a', 'b')
    assert result.stdout.strip().splitlines() == ['1', '2']


def test_del(live_server):
    _redis_cli(live_server.port, 'SET', 'k', 'v')
    result = _redis_cli(live_server.port, 'DEL', 'k')
    assert result.stdout.strip() == '1'
    result = _redis_cli(live_server.port, 'GET', 'k')
    assert result.stdout.strip() == ''
