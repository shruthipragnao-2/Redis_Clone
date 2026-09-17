"""Multiple commands per connection, multiple concurrent connections, TCP
framing edge cases (pipelining, fragmentation), and connection cleanup.

Implementation: Server.connection_handler's request loop (server.py), the
gevent Pool/StreamServer that accepts connections (Server.__init__), and
ProtocolHandler's use of a buffered socket.makefile() stream, which is what
makes pipelining/fragmentation transparent to the parser (see
test_protocol.py's module docstring for why that's the parser's job, and
here we confirm it holds over a real socket).
"""
import time

import pytest

from conftest import StructuredClient, recv_until


# --- Multiple commands over one connection ---

def test_multiple_sequential_commands_on_one_connection(client):
    assert client.set('a', '1') == b'OK'
    assert client.get('a') == b'1'
    assert client.set('b', '2') == b'OK'
    assert client.mget('a', 'b') == [b'1', b'2']
    assert client.delete('a') == 1
    assert client.get('a') is None


def test_two_pipelined_commands_in_a_single_send(live_server):
    sock = live_server.raw_connect()
    try:
        payload = (
            b'*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n'
            b'*2\r\n$3\r\nGET\r\n$1\r\nk\r\n'
        )
        sock.sendall(payload)
        raw = recv_until(sock, len(b'+OK\r\n') + len(b'$1\r\n1\r\n'))
        assert raw == b'+OK\r\n$1\r\n1\r\n'
    finally:
        sock.close()


def test_many_pipelined_commands_in_a_single_send(live_server):
    sock = live_server.raw_connect()
    try:
        one_set = b'*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n'
        sock.sendall(one_set * 20)
        raw = recv_until(sock, len(b'+OK\r\n') * 20)
        assert raw == b'+OK\r\n' * 20
    finally:
        sock.close()


# --- Commands split across multiple TCP reads ---

def test_command_split_across_many_small_sends(live_server):
    sock = live_server.raw_connect()
    try:
        payload = b'*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n'
        for i in range(0, len(payload), 3):
            sock.sendall(payload[i:i + 3])
            time.sleep(0.01)  # encourage separate TCP segments/recv() calls
        raw = recv_until(sock, len(b'+OK\r\n'))
        assert raw == b'+OK\r\n'

        client = StructuredClient(sock)
        assert client.call('GET', 'hello') == b'world'
    finally:
        sock.close()


def test_split_mid_length_header(live_server):
    sock = live_server.raw_connect()
    try:
        part1 = b'*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$'
        part2 = b'5\r\nhello\r\n'
        sock.sendall(part1)
        time.sleep(0.05)
        sock.sendall(part2)
        assert recv_until(sock, len(b'+OK\r\n')) == b'+OK\r\n'
    finally:
        sock.close()


def test_split_mid_bulk_body(live_server):
    sock = live_server.raw_connect()
    try:
        full = b'*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n'
        cut = full.index(b'hello') + 2  # inside "hello"
        sock.sendall(full[:cut])
        time.sleep(0.05)
        sock.sendall(full[cut:])
        assert recv_until(sock, len(b'+OK\r\n')) == b'+OK\r\n'
    finally:
        sock.close()


# --- Multiple concurrent client connections share server state ---

def test_two_client_connections_share_the_same_store(live_server):
    writer = live_server.client()
    reader = live_server.client()
    try:
        writer.set('shared', 'value')
        assert reader.get('shared') == b'value'
    finally:
        writer._socket.close()
        reader._socket.close()


def test_many_client_connections_can_connect_simultaneously(live_server):
    clients = [live_server.client() for _ in range(10)]
    try:
        for i, c in enumerate(clients):
            c.set('key:%d' % i, 'value:%d' % i)
        for i, c in enumerate(clients):
            assert c.get('key:%d' % i) == ('value:%d' % i).encode()
    finally:
        for c in clients:
            c._socket.close()


# --- Malformed frames close the connection but not the server ---

def test_malformed_frame_closes_connection_but_server_stays_up(live_server):
    bad = live_server.raw_connect()
    bad.sendall(b'*abc\r\n')
    bad.settimeout(5)
    reply = bad.recv(4096)
    assert reply.startswith(b'-')
    bad.close()

    good = live_server.client()
    try:
        assert good.set('still', 'alive') == b'OK'
        assert good.get('still') == b'alive'
    finally:
        good._socket.close()


# --- Clean and mid-frame disconnects don't hang or crash the server ---

def test_clean_disconnect_with_no_data_does_not_affect_server(live_server):
    sock = live_server.raw_connect()
    sock.close()
    client = live_server.client()
    try:
        assert client.set('a', '1') == b'OK'
    finally:
        client._socket.close()


def test_disconnect_mid_frame_does_not_affect_server(live_server):
    sock = live_server.raw_connect()
    sock.sendall(b'*2\r\n$3\r\nGET\r\n$3\r\nfo')  # cut off mid bulk-string body
    sock.close()
    client = live_server.client()
    try:
        assert client.set('b', '2') == b'OK'
    finally:
        client._socket.close()


def test_connection_can_be_reused_for_many_requests_without_leaking(live_server):
    """Not a resource-usage assertion (nothing in this test suite can
    observe fd counts portably) -- just confirms the connection stays
    healthy over many requests, which would surface any accumulating
    per-request breakage.
    """
    client = live_server.client()
    try:
        for i in range(200):
            client.set('k', str(i))
            assert client.get('k') == str(i).encode()
    finally:
        client._socket.close()
