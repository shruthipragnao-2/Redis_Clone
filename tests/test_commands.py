"""Every command in Server.get_commands(), driven through the project's
own client.py (public behavior, not internals), plus command-parsing edge
cases (unknown commands, inline string commands, empty requests) that need
raw socket control.

Command table under test (server.py Server.get_commands):
    GET, SET, DELETE, DEL, FLUSH, FLUSHDB, MGET, MSET, SAVE
SAVE's actual persistence behavior is covered in test_persistence.py; here
it's only exercised as "a command that exists and replies OK".
"""
import pytest

from server import Error


# --- GET / SET ---

def test_get_missing_key_returns_none(client):
    assert client.get('nope') is None


def test_set_then_get(client):
    assert client.set('foo', 'bar') == b'OK'
    assert client.get('foo') == b'bar'


def test_set_overwrites_existing_value(client):
    client.set('foo', 'first')
    client.set('foo', 'second')
    assert client.get('foo') == b'second'


def test_set_empty_value_round_trips(client):
    client.set('empty', '')
    assert client.get('empty') == b''


# --- DELETE / DEL (aliases) ---

@pytest.mark.parametrize('delete_command', ['DELETE', 'DEL'])
def test_delete_existing_key_returns_1_and_removes_it(client, delete_command):
    client.set('k', 'v')
    assert client.execute(delete_command, 'k') == 1
    assert client.get('k') is None


@pytest.mark.parametrize('delete_command', ['DELETE', 'DEL'])
def test_delete_missing_key_returns_0(client, delete_command):
    assert client.execute(delete_command, 'nope') == 0


# --- FLUSH / FLUSHDB (aliases) ---

@pytest.mark.parametrize('flush_command', ['FLUSH', 'FLUSHDB'])
def test_flush_clears_store_and_returns_count(client, flush_command):
    client.set('a', '1')
    client.set('b', '2')
    assert client.execute(flush_command) == 2
    assert client.get('a') is None
    assert client.get('b') is None


def test_flush_on_empty_store_returns_zero(client):
    assert client.flush() == 0


# --- MGET ---

def test_mget_mix_of_present_and_missing_keys(client):
    client.set('a', '1')
    assert client.mget('a', 'missing') == [b'1', None]


def test_mget_empty_key_list(client):
    assert client.mget() == []


def test_mget_all_missing_returns_all_none(client):
    assert client.mget('x', 'y', 'z') == [None, None, None]


# --- MSET ---

def test_mset_multiple_pairs(client):
    assert client.mset('a', '1', 'b', '2', 'c', '3') == 3
    assert client.mget('a', 'b', 'c') == [b'1', b'2', b'3']


def test_mset_zero_args_sets_nothing(client):
    assert client.mset() == 0


def test_mset_odd_number_of_args_silently_drops_trailing_key(client):
    # Documents an existing quirk, not a desired behavior: mset() zips
    # items[::2] with items[1::2], so a trailing key with no value is
    # silently dropped rather than raising an arity error the way real
    # Redis's MSET would. Not fixed here -- this suite characterizes
    # existing behavior, it doesn't correct it.
    result = client.execute('MSET', 'a', '1', 'b')
    assert result == 1
    assert client.get('a') == b'1'
    assert client.get('b') is None


# --- SAVE (existence/wiring only; see test_persistence.py for behavior) ---

def test_save_command_returns_ok(client):
    assert client.save() == b'OK'


# --- Unknown commands / command parsing edge cases ---

def test_unknown_command_returns_error(client):
    with pytest.raises(Exception):
        client.execute('NOTACOMMAND')


def test_unknown_command_error_message_names_the_command(structured_client):
    result = structured_client.call('NOTACOMMAND')
    assert isinstance(result, Error)
    assert b'NOTACOMMAND' in result.message


def test_command_name_is_case_insensitive(client):
    # get_command_name() upper-cases the command before dispatch.
    assert client.execute('get', 'missing') is None
    assert client.execute('SeT', 'k', 'v') == b'OK'


def test_unknown_command_is_recoverable_connection_stays_open(structured_client):
    result = structured_client.call('NOTACOMMAND')
    assert isinstance(result, Error)
    # A bad command name is a command-level error (framing was fine), so
    # the connection must still be usable afterwards.
    assert structured_client.call('SET', 'x', '1') == b'OK'


def test_inline_string_command_is_parsed_via_split(structured_client):
    # normalize_request() accepts a plain (non-array) request and falls
    # back to str.split()/bytes.split() -- this is reachable whenever a
    # client sends a RESP simple string instead of an array of bulk
    # strings. Exercise that path directly over the wire.
    structured_client.sock.sendall(b'+SET inline value\r\n')
    assert structured_client.recv() == b'OK'
    assert structured_client.call('GET', 'inline') == b'value'


def test_empty_array_request_is_missing_command_error(structured_client):
    structured_client.sock.sendall(b'*0\r\n')
    result = structured_client.recv()
    assert isinstance(result, Error)
    assert b'Missing command' in result.message


def test_non_list_non_string_request_is_a_command_error(structured_client):
    # normalize_request() accepts a list, or falls back to str.split()/
    # bytes.split() for anything with a .split() method. A bare RESP
    # integer has neither, so it must be rejected as a CommandError
    # rather than raising an uncaught AttributeError.
    structured_client.sock.sendall(b':5\r\n')
    result = structured_client.recv()
    assert isinstance(result, Error)
    assert b'must be list or simple string' in result.message
    # This is a command-level error (framing was intact), so the
    # connection stays open.
    assert structured_client.call('SET', 'x', '1') == b'OK'


def test_invalid_utf8_command_name_is_command_error_not_crash(structured_client):
    result = structured_client.call(b'\xff\xfe', 'x')
    assert isinstance(result, Error)
    assert b'utf-8' in result.message
    # Framing was fine (a well-formed array of bulk strings); only the
    # command name was bad, so the connection stays usable.
    assert structured_client.call('SET', 'y', '1') == b'OK'


@pytest.mark.parametrize('bad_request', [
    ('GET',),         # missing required key
    ('SET', 'k'),     # missing required value
])
def test_wrong_arity_crashes_the_connection_known_bug(structured_client, bad_request):
    """Documents a genuine defect, not desired behavior.

    Server.get_response() calls self._commands[command](*data[1:]) with no
    arity check. A wrong number of arguments raises a plain TypeError,
    which connection_handler does not catch (it only catches CommandError)
    -- so the client gets no reply at all and the connection is dropped,
    instead of a `-ERR wrong number of arguments` reply. This is flagged
    as a known bug in the audit; fixing it is out of scope for this test
    suite (which characterizes existing behavior), so this test pins the
    current (broken) behavior rather than the desired one.
    """
    structured_client.send(*bad_request)
    with pytest.raises(Exception):  # Disconnect (EOF) or a socket timeout
        structured_client.recv()
