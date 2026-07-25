import os
import pickle
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
    """Encodes/decodes the RESP (REdis Serialization Protocol) wire format."""

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
            raise CommandError('bad request')
        return handler(socket_file)

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))

    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip(b'\r\n'))

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2  # Include the trailing \r\n.
        return socket_file.read(length)[:-2]

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
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
            'FLUSH': self.flush,
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
            command = command.decode('utf-8')
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
        return 1

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
        with open(self._dump_path, 'wb') as fh:
            pickle.dump(self._kv, fh, protocol=pickle.HIGHEST_PROTOCOL)
        return SimpleString('OK')

    def _load(self):
        if not os.path.exists(self._dump_path):
            return
        with open(self._dump_path, 'rb') as fh:
            try:
                self._kv = pickle.load(fh)
            except (EOFError, pickle.UnpicklingError):
                self._kv = {}

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
