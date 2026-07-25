import socket

from server import Error, ProtocolHandler, SimpleString


class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        self._protocol.write_response(self._fh, list(args))
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise Exception(resp.message)
        if isinstance(resp, SimpleString):
            return resp.value
        return resp

    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET', *keys)

    def mset(self, *items):
        return self.execute('MSET', *items)

    def save(self):
        return self.execute('SAVE')

    def multi(self):
        return self.execute('MULTI')

    def discard(self):
        return self.execute('DISCARD')

    def exec_(self):
        return self.execute('EXEC')


if __name__ == '__main__':
    client = Client()
    client.flush()
    client.set('foo', 'bar')
    print('GET foo ->', client.get('foo'))
    client.mset('a', '1', 'b', '2')
    print('MGET a b ->', client.mget('a', 'b'))
    print('DELETE foo ->', client.delete('foo'))
    print('GET foo (after delete) ->', client.get('foo'))

    print('SAVE ->', client.save())

    print('MULTI ->', client.multi())
    print('  SET x 1 ->', client.set('x', '1'))
    print('  SET y 2 ->', client.set('y', '2'))
    print('  GET x ->', client.get('x'))
    print('EXEC ->', client.exec_())
