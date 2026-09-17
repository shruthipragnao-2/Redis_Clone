import os
import pickle
import tempfile
from collections import namedtuple
from io import BytesIO

import gevent
from gevent.pool import Pool
from gevent.server import StreamServer


class CommandError(Exception):
    pass


class Disconnect(Exception):
    pass


Error = namedtuple('Error', ('message',))
SimpleString = namedtuple('SimpleString', ('value',))


class ProtocolHandler(object):
    """Encodes/decodes the RESP (REdis Serialization Protocol) wire format.

    Reads come from a buffered file-like object (typically a socket's
    ``makefile()``). Using a buffered stream instead of raw ``recv()`` calls
    means partial TCP reads and multiple pipelined commands sharing one TCP
    read are already handled correctly by the stream's own buffering — a
    ``readline()``/``read(n)`` call transparently blocks and accumulates
    across as many underlying ``recv()`` calls as it takes. The hardening
    below is about *malformed* input: bad lengths, truncated frames, and
    frames that declare implausibly large sizes.
    """

    # Matches Redis's own default (proto-max-bulk-len). Declaring a length
    # above this is almost certainly a malformed or hostile request, not a
    # legitimate large value.
    MAX_BULK_LEN = 512 * 1024 * 1024

    # Matches Redis's hardcoded multibulk element-count ceiling.
    MAX_ARRAY_ELEMENTS = 1024 * 1024

    # Applies to any single line (simple strings, errors, integers, and
    # length headers). Modeled on Redis's inline-command size limit; without
    # a cap, a line with no "\r\n" would make readline() buffer unbounded
    # amounts of memory waiting for a terminator that may never arrive.
    MAX_LINE_LEN = 64 * 1024

    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict,
        }

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            handler = self.handlers[first_byte]
        except KeyError:
            raise CommandError('bad request: unknown type byte %r' % first_byte)
        return handler(socket_file)

    def _read_line(self, socket_file):
        """Read one CRLF-terminated line, capped at MAX_LINE_LEN.

        Raises Disconnect if the stream ended before a full line arrived
        (a clean or unclean mid-frame close), and CommandError if a
        terminator never showed up within the size cap (malformed/hostile
        input rather than a dropped connection).
        """
        line = socket_file.readline(self.MAX_LINE_LEN + 1)
        if not line:
            raise Disconnect()
        if not line.endswith(b'\r\n'):
            if len(line) > self.MAX_LINE_LEN:
                raise CommandError('line too long (max %d bytes)' % self.MAX_LINE_LEN)
            raise Disconnect()
        return line[:-2]

    def _read_length(self, socket_file, label):
        line = self._read_line(socket_file)
        try:
            return int(line)
        except ValueError:
            raise CommandError('invalid %s: not an integer' % label)

    def _read_exact(self, socket_file, n):
        """Read exactly n bytes, or raise Disconnect if the stream ended first.

        A single read(n) call is enough for a real socket.makefile() (its
        BufferedReader loops internally until n bytes or EOF), but looping
        here too means correctness doesn't depend on that guarantee holding
        for whatever file-like object is passed in.
        """
        chunks = []
        remaining = n
        while remaining > 0:
            chunk = socket_file.read(remaining)
            if not chunk:
                raise Disconnect()
            chunks.append(chunk)
            remaining -= len(chunk)
        return b''.join(chunks)

    def handle_simple_string(self, socket_file):
        return self._read_line(socket_file)

    def handle_error(self, socket_file):
        return Error(self._read_line(socket_file))

    def handle_integer(self, socket_file):
        return self._read_length(socket_file, 'integer')

    def handle_string(self, socket_file):
        length = self._read_length(socket_file, 'bulk length')
        if length == -1:
            return None  # Null bulk string ($-1\r\n).
        if length < -1:
            raise CommandError('invalid bulk length: %d' % length)
        if length > self.MAX_BULK_LEN:
            raise CommandError(
                'bulk length too large: %d bytes (max %d)' % (length, self.MAX_BULK_LEN))
        payload = self._read_exact(socket_file, length + 2)  # + trailing \r\n
        if payload[-2:] != b'\r\n':
            raise CommandError('bad bulk string terminator')
        return payload[:-2]

    def handle_array(self, socket_file):
        num_elements = self._read_length(socket_file, 'array length')
        if num_elements == -1:
            return None  # Null array (*-1\r\n).
        if num_elements < -1:
            raise CommandError('invalid array length: %d' % num_elements)
        if num_elements > self.MAX_ARRAY_ELEMENTS:
            raise CommandError(
                'array length too large: %d (max %d)' % (num_elements, self.MAX_ARRAY_ELEMENTS))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = self._read_length(socket_file, 'dict length')
        if num_items < 0:
            raise CommandError('invalid dict length: %d' % num_items)
        if num_items * 2 > self.MAX_ARRAY_ELEMENTS:
            raise CommandError(
                'dict length too large: %d (max %d entries)' % (num_items, self.MAX_ARRAY_ELEMENTS // 2))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%b\r\n' % (len(data), data))
        elif isinstance(data, bool):
            buf.write(b':%d\r\n' % (1 if data else 0))
        elif isinstance(data, int):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, SimpleString):
            buf.write(b'+%b\r\n' % data.value.encode('utf-8'))
        elif isinstance(data, Error):
            buf.write(b'-%b\r\n' % data.message.encode('utf-8'))
        elif isinstance(data, (list, tuple)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b'$-1\r\n')
        else:
            raise CommandError('unrecognized type: %s' % type(data))


class Server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64,
                 dump_path='dump.rdb', autosave_interval=None):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port), self.connection_handler, spawn=self._pool)
        self._protocol = ProtocolHandler()
        self._dump_path = dump_path
        self._autosave_interval = autosave_interval
        self._kv = {}
        self._load()
        self._commands = self.get_commands()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'DEL': self.delete,
            'FLUSH': self.flush,
            'FLUSHDB': self.flush,
            'MGET': self.mget,
            'MSET': self.mset,
            'SAVE': self.save,
        }

    def connection_handler(self, conn, address):
        socket_file = conn.makefile('rwb')
        in_transaction = False
        transaction_queue = []

        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break
            except CommandError as exc:
                # A malformed frame desynchronizes the stream: we can no
                # longer tell where the next valid frame starts. Unlike a
                # command-level error (bad arity, unknown command), which
                # leaves framing intact and the connection reusable, this
                # is unrecoverable — report it and close, matching how real
                # Redis responds to protocol errors it can't resync from.
                try:
                    self._protocol.write_response(socket_file, Error(exc.args[0]))
                except Exception:
                    pass
                break

            try:
                data = self.normalize_request(data)
                command = self.get_command_name(data)
            except CommandError as exc:
                self._protocol.write_response(socket_file, Error(exc.args[0]))
                continue

            if command == 'MULTI':
                if in_transaction:
                    resp = Error('MULTI calls can not be nested')
                else:
                    in_transaction = True
                    transaction_queue = []
                    resp = SimpleString('OK')
            elif command == 'DISCARD':
                if not in_transaction:
                    resp = Error('DISCARD without MULTI')
                else:
                    in_transaction = False
                    transaction_queue = []
                    resp = SimpleString('OK')
            elif command == 'EXEC':
                if not in_transaction:
                    resp = Error('EXEC without MULTI')
                else:
                    in_transaction = False
                    resp = []
                    for queued in transaction_queue:
                        try:
                            resp.append(self.get_response(queued))
                        except CommandError as exc:
                            resp.append(Error(exc.args[0]))
                    transaction_queue = []
            elif in_transaction:
                transaction_queue.append(data)
                resp = SimpleString('QUEUED')
            else:
                try:
                    resp = self.get_response(data)
                except CommandError as exc:
                    resp = Error(exc.args[0])

            self._protocol.write_response(socket_file, resp)

    def normalize_request(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except AttributeError:
                raise CommandError('Request must be list or simple string.')

        if not data:
            raise CommandError('Missing command')

        return data

    def get_command_name(self, data):
        command = data[0]
        if isinstance(command, bytes):
            try:
                command = command.decode('utf-8')
            except UnicodeDecodeError:
                raise CommandError('command name must be valid utf-8')
        return command.upper()

    def get_response(self, data):
        data = self.normalize_request(data)
        command = self.get_command_name(data)

        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)

        return self._commands[command](*data[1:])

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return SimpleString('OK')

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self._kv[key] = value
        return len(items) // 2

    def save(self):
        # Snapshot the dict before writing so the on-disk data reflects one
        # consistent point in time even if something about the write path
        # changes later (e.g. a thread-backed file writer) and stops being
        # implicitly atomic with respect to other greenlets.
        snapshot = dict(self._kv)

        directory = os.path.dirname(os.path.abspath(self._dump_path))
        fd, tmp_path = tempfile.mkstemp(dir=directory, prefix='.dump-', suffix='.tmp')
        try:
            with os.fdopen(fd, 'wb') as fh:
                pickle.dump(snapshot, fh, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp_path, self._dump_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
        return SimpleString('OK')

    def _load(self):
        if not os.path.exists(self._dump_path):
            return
        with open(self._dump_path, 'rb') as fh:
            try:
                data = pickle.load(fh)
            except Exception:
                # Any failure to unpickle the dump file (truncated,
                # corrupted, or referencing a class that can no longer be
                # resolved) degrades to an empty store rather than
                # crashing startup entirely -- narrowly catching only
                # EOFError/UnpicklingError missed other exception types
                # (e.g. AttributeError from an unresolvable class) that
                # are just as much "this file isn't usable".
                data = {}
        # A syntactically valid pickle of the wrong type (e.g. a list)
        # would otherwise silently become self._kv, breaking every
        # command for the life of the process.
        self._kv = data if isinstance(data, dict) else {}

    def _autosave_loop(self):
        while True:
            gevent.sleep(self._autosave_interval)
            self.save()

    def run(self):
        if self._autosave_interval:
            gevent.spawn(self._autosave_loop)
        self._server.serve_forever()


if __name__ == '__main__':
    import argparse

    from gevent import monkey
    monkey.patch_all()

    parser = argparse.ArgumentParser(description='Run the redis clone server.')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=31337)
    parser.add_argument('--dump-path', default='dump.rdb',
                         help='path to load/save the snapshot file')
    parser.add_argument('--autosave', type=int, default=None,
                         help='autosave interval in seconds (disabled by default)')
    args = parser.parse_args()

    Server(host=args.host, port=args.port, dump_path=args.dump_path,
           autosave_interval=args.autosave).run()
