"""RESP encoding/decoding, tested against ProtocolHandler directly.

No sockets, no server, no threads -- pure in-memory byte streams, so these
are fast and fully deterministic. Framing behavior that only a real
transport can exercise (pipelined commands sharing one TCP read, a command
split across multiple reads) lives in test_connections.py instead.
"""
import io

import pytest

from server import CommandError, Disconnect, Error, ProtocolHandler, SimpleString


def reader(data):
    return io.BytesIO(data)


class ChunkedReader:
    """A file-like object that only ever hands back a few bytes at a time,
    regardless of how much is asked for -- stands in for a transport where
    a single read() can return less than requested (e.g. the connection
    dropping mid-frame). Used to prove the parser doesn't assume a request
    arrives in one shot.
    """

    def __init__(self, data, chunk_size=1):
        self._data = data
        self._pos = 0
        self._chunk_size = chunk_size

    def read(self, n=-1):
        if n is None or n < 0:
            n = len(self._data) - self._pos
        end = min(self._pos + min(n, self._chunk_size), len(self._data))
        chunk = self._data[self._pos:end]
        self._pos = end
        return chunk

    def readline(self, size=-1):
        limit = len(self._data) if size is None or size < 0 else min(len(self._data), self._pos + size)
        window = self._data[self._pos:limit]
        nl = window.find(b'\n')
        end = self._pos + nl + 1 if nl != -1 else min(self._pos + self._chunk_size, limit)
        chunk = self._data[self._pos:end]
        self._pos = end
        return chunk


@pytest.fixture
def protocol():
    return ProtocolHandler()


# --- Decoding: every wire format the implementation claims to support ---

@pytest.mark.parametrize('frame, expected', [
    (b'+OK\r\n', b'OK'),
    (b':1000\r\n', 1000),
    (b':-42\r\n', -42),
    (b':0\r\n', 0),
    (b'$3\r\nfoo\r\n', b'foo'),
    (b'$0\r\n\r\n', b''),
    (b'$-1\r\n', None),
    (b'*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n', [b'foo', b'bar']),
    (b'*0\r\n', []),
    (b'*-1\r\n', None),
    (b'*3\r\n:1\r\n+OK\r\n$-1\r\n', [1, b'OK', None]),
    (b'*2\r\n*2\r\n:1\r\n:2\r\n$3\r\nfoo\r\n', [[1, 2], b'foo']),
    (b'*1\r\n*1\r\n*1\r\n:7\r\n', [[[7]]]),
    (b'%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n:2\r\n', {b'a': 1, b'b': 2}),
    (b'%0\r\n', {}),
])
def test_decode_supported_frame(protocol, frame, expected):
    assert protocol.handle_request(reader(frame)) == expected


def test_decode_error(protocol):
    assert protocol.handle_request(reader(b'-ERR bad thing\r\n')) == Error(b'ERR bad thing')


def test_decode_bulk_string_binary_safe(protocol):
    # RESP bulk strings may contain arbitrary bytes, including \r\n, as
    # long as the declared length is honored.
    payload = b'a\r\nb'
    frame = b'$%d\r\n%s\r\n' % (len(payload), payload)
    assert protocol.handle_request(reader(frame)) == payload


# --- Malformed input: must raise CommandError, never hang or crash oddly ---

@pytest.mark.parametrize('frame', [
    b'!nope\r\n',                  # unknown type byte
    b'$abc\r\nfoo\r\n',            # non-integer bulk length
    b'*abc\r\n',                   # non-integer array length
    b':notanumber\r\n',            # non-integer value
    b'$-5\r\nfoo\r\n',             # negative bulk length other than -1
    b'*-5\r\n:1\r\n',              # negative array length other than -1
    b'$3\r\nfooXX',                # bad bulk terminator
])
def test_malformed_frame_raises_command_error(protocol, frame):
    with pytest.raises(CommandError):
        protocol.handle_request(reader(frame))


def test_oversized_bulk_length_rejected_without_reading_body(protocol):
    # No body is ever supplied; if the implementation tried to honor the
    # declared length it would hang waiting for data that doesn't exist.
    huge = ProtocolHandler.MAX_BULK_LEN + 1
    with pytest.raises(CommandError):
        protocol.handle_request(reader(b'$%d\r\n' % huge))


def test_oversized_array_length_rejected_without_reading_elements(protocol):
    huge = ProtocolHandler.MAX_ARRAY_ELEMENTS + 1
    with pytest.raises(CommandError):
        protocol.handle_request(reader(b'*%d\r\n' % huge))


def test_oversized_dict_length_rejected(protocol):
    huge = ProtocolHandler.MAX_ARRAY_ELEMENTS
    with pytest.raises(CommandError):
        protocol.handle_request(reader(b'%%%d\r\n' % huge))


def test_negative_dict_length_rejected(protocol):
    with pytest.raises(CommandError):
        protocol.handle_request(reader(b'%-1\r\n'))


def test_line_too_long_without_terminator_raises_command_error():
    p = ProtocolHandler()
    p.MAX_LINE_LEN = 16  # shrink the cap so the test stays tiny/fast
    junk = b'+' + (b'x' * 100)  # no \r\n anywhere
    with pytest.raises(CommandError):
        p.handle_request(reader(junk))


# --- Disconnects: a dropped connection is distinct from a malformed frame ---

@pytest.mark.parametrize('frame', [
    b'',                             # clean EOF between frames
    b'$5\r\nfoo',                    # truncated bulk body
    b'+OK',                          # truncated line, no CRLF
    b'*2\r\n$3\r\nfoo\r\n',          # array header promises 2, stream has 1
    b'$',                            # type byte only, stream ends before the length line
])
def test_dropped_connection_raises_disconnect(protocol, frame):
    with pytest.raises(Disconnect):
        protocol.handle_request(reader(frame))


def test_disconnect_via_incremental_short_reads(protocol):
    # Same truncated-body case, delivered one byte at a time, to rule out
    # any dependence on read() returning everything in one call.
    with pytest.raises(Disconnect):
        protocol.handle_request(ChunkedReader(b'$5\r\nfoo', chunk_size=1))


# --- Bytes trickling in must parse identically to bytes delivered at once ---

@pytest.mark.parametrize('frame, chunk_size, expected', [
    (b'$3\r\nfoo\r\n', 1, b'foo'),
    (b'*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n', 1, [b'foo', b'bar']),
    (b'*2\r\n*2\r\n:1\r\n:2\r\n$3\r\nfoo\r\n', 3, [[1, 2], b'foo']),
])
def test_incremental_delivery(protocol, frame, chunk_size, expected):
    assert protocol.handle_request(ChunkedReader(frame, chunk_size=chunk_size)) == expected


# --- Multiple frames back to back in one buffer (in-process pipelining) ---

def test_two_frames_in_one_buffer(protocol):
    buf = reader(b'*1\r\n$4\r\nPING\r\n+OK\r\n')
    assert protocol.handle_request(buf) == [b'PING']
    assert protocol.handle_request(buf) == b'OK'
    with pytest.raises(Disconnect):
        protocol.handle_request(buf)


def test_many_frames_in_one_buffer(protocol):
    one = b'$3\r\nfoo\r\n'
    buf = reader(one * 50)
    for _ in range(50):
        assert protocol.handle_request(buf) == b'foo'
    with pytest.raises(Disconnect):
        protocol.handle_request(buf)


# --- Encode/decode round trip: server and client sides must agree ---

def _roundtrip(protocol, value):
    buf = io.BytesIO()
    protocol.write_response(buf, value)
    buf.seek(0)
    return protocol.handle_request(buf)


@pytest.mark.parametrize('value, expected', [
    (b'hello', b'hello'),
    ('hello', b'hello'),
    (b'', b''),
    (None, None),
    (12345, 12345),
    (-7, -7),
    ([b'a', 1, None], [b'a', 1, None]),
    ([], []),
    ([[1, 2], [3, [4, 5]]], [[1, 2], [3, [4, 5]]]),
    ({b'a': 1}, {b'a': 1}),
])
def test_roundtrip(protocol, value, expected):
    assert _roundtrip(protocol, value) == expected


def test_bool_encodes_as_integer(protocol):
    # Intentional deviation from a full RESP3 implementation: this codec
    # has no boolean wire type, so booleans are sent as RESP integers
    # (0/1), matching how real Redis represents booleans in replies.
    assert _roundtrip(protocol, True) == 1
    assert _roundtrip(protocol, False) == 0


def test_simple_string_and_error_encode(protocol):
    assert _roundtrip(protocol, SimpleString('OK')) == b'OK'
    assert _roundtrip(protocol, Error('ERR broken')) == Error(b'ERR broken')


def test_unencodable_type_raises_command_error(protocol):
    with pytest.raises(CommandError):
        _roundtrip(protocol, object())
