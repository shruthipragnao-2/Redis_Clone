"""MULTI/EXEC/DISCARD and per-connection transaction isolation.

Implementation: Server.connection_handler (server.py), which keeps
`in_transaction`/`transaction_queue` as connection-local variables -- that
locality is exactly what should make two connections' transactions
independent, and is what this file's isolation tests verify.
"""
from server import Error


def test_multi_returns_ok(structured_client):
    assert structured_client.call('MULTI') == b'OK'


def test_nested_multi_is_an_error(structured_client):
    structured_client.call('MULTI')
    result = structured_client.call('MULTI')
    assert isinstance(result, Error)
    assert b'nested' in result.message.lower()


def test_discard_without_multi_is_an_error(structured_client):
    result = structured_client.call('DISCARD')
    assert isinstance(result, Error)


def test_exec_without_multi_is_an_error(structured_client):
    result = structured_client.call('EXEC')
    assert isinstance(result, Error)


def test_discard_clears_queue_and_ends_transaction(structured_client):
    structured_client.call('MULTI')
    structured_client.call('SET', 'k', 'v')
    assert structured_client.call('DISCARD') == b'OK'
    # Queued SET must not have been applied.
    assert structured_client.call('GET', 'k') is None
    # And the transaction state is really over: EXEC now errors again.
    result = structured_client.call('EXEC')
    assert isinstance(result, Error)


def test_commands_inside_multi_are_queued_not_executed(structured_client):
    structured_client.call('MULTI')
    assert structured_client.call('SET', 'k', 'v') == b'QUEUED'
    # Not executed yet -- a plain GET outside the queue (this same
    # connection is still "inside" MULTI, but GET issued now would also
    # just be queued; to observe pre-EXEC state we check via a second
    # connection instead, see test_transaction_isolation below).
    assert structured_client.call('EXEC') == [b'OK']
    assert structured_client.call('GET', 'k') == b'v'


def test_exec_returns_array_of_results_in_order(structured_client):
    structured_client.call('MULTI')
    structured_client.call('SET', 'a', '1')
    structured_client.call('SET', 'b', '2')
    structured_client.call('MGET', 'a', 'b')
    result = structured_client.call('EXEC')
    assert result == [b'OK', b'OK', [b'1', b'2']]


def test_exec_with_unknown_queued_command_reports_error_for_that_slot_only(structured_client):
    structured_client.call('MULTI')
    structured_client.call('SET', 'a', '1')
    structured_client.call('NOTACOMMAND')
    structured_client.call('SET', 'b', '2')
    result = structured_client.call('EXEC')
    assert result[0] == b'OK'
    assert isinstance(result[1], Error)
    assert result[2] == b'OK'
    # Commands before and after the bad one still took effect -- this
    # implementation does not roll back a transaction on a per-command
    # error (matching real Redis, which also does not roll back EXEC).
    assert structured_client.call('GET', 'a') == b'1'
    assert structured_client.call('GET', 'b') == b'2'


def test_exec_with_empty_queue_returns_empty_array(structured_client):
    structured_client.call('MULTI')
    assert structured_client.call('EXEC') == []


def test_transaction_queue_and_state_reset_after_exec(structured_client):
    structured_client.call('MULTI')
    structured_client.call('SET', 'a', '1')
    structured_client.call('EXEC')
    # MULTI state must be fully cleared: EXEC again (with nothing queued)
    # is now "EXEC without MULTI", not an empty-array success.
    result = structured_client.call('EXEC')
    assert isinstance(result, Error)


def test_transaction_isolation_between_two_connections(make_structured_client):
    """Queuing a command on one connection must not be visible to, or
    interfere with, another connection -- and must not take effect until
    that connection's own EXEC runs. All ordering here is driven by
    waiting for each call's reply before issuing the next, so there is no
    reliance on timing or sleeps.
    """
    conn_a = make_structured_client()
    conn_b = make_structured_client()

    assert conn_a.call('MULTI') == b'OK'
    assert conn_a.call('SET', 'shared', 'from-a') == b'QUEUED'

    # B is a completely separate connection: it was never put in a
    # transaction, and A's queued (not yet executed) SET must not be
    # visible to it.
    assert conn_b.call('GET', 'shared') is None
    assert conn_b.call('SET', 'shared', 'from-b') == b'OK'

    # A's queued command is unaffected by B's direct write; running A's
    # EXEC now applies A's queued value, overwriting B's.
    assert conn_a.call('EXEC') == [b'OK']
    assert conn_b.call('GET', 'shared') == b'from-a'

    # B was never in a transaction, so MULTI-only commands on A had no
    # effect on B's own (non-existent) transaction state.
    result = conn_b.call('EXEC')
    assert isinstance(result, Error)


def test_second_connection_queuing_does_not_affect_first(make_structured_client):
    """The reverse direction: while A is mid-transaction, B starts and
    queues its own transaction independently, and each EXEC only ever
    plays back that connection's own queue."""
    conn_a = make_structured_client()
    conn_b = make_structured_client()

    conn_a.call('MULTI')
    conn_a.call('SET', 'a-key', 'a-value')

    conn_b.call('MULTI')
    conn_b.call('SET', 'b-key', 'b-value')

    assert conn_b.call('EXEC') == [b'OK']
    # A's queue must still hold only its own command.
    assert conn_a.call('EXEC') == [b'OK']

    assert conn_a.call('GET', 'a-key') == b'a-value'
    assert conn_a.call('GET', 'b-key') == b'b-value'
