"""One bounded duplex frame per direction; no queues or pickle application data."""
import select
import socket

from .errors import IntegrityError, ResourceError
from .worker_protocol import HEADER_BYTES, inspect_header, encode_message, decode_message


class FramedSocket:
    def __init__(self, sock):
        self.socket = sock
        sock.setblocking(False)
        self._out = None
        self._sent = 0
        self._header = bytearray()
        self._body = None
        self._length = 0
        self._message = None
        self.closed = False

    @property
    def sending(self):
        return self._out is not None

    def send(self, message):
        if self.closed or self._out is not None:
            raise ResourceError('worker transport already has an admitted outgoing frame')
        self._out = encode_message(message)
        self._sent = 0

    def receive(self):
        result, self._message = self._message, None
        return result

    def pump(self, timeout=0.0):
        if self.closed:
            raise ResourceError('worker transport is closed')
        readable = [] if self._message is not None else [self.socket]
        writable = [self.socket] if self.sending else []
        reads, writes, _ = select.select(readable, writable, [], timeout)
        if writes:
            try:
                count = self.socket.send(memoryview(self._out)[self._sent:])
            except BlockingIOError:
                count = None
            if count == 0:
                raise ResourceError('worker channel closed during send')
            if count:
                self._sent += count
                if self._sent == len(self._out):
                    self._out, self._sent = None, 0
        if reads:
            target = self._header if self._body is None else self._body
            remaining = (HEADER_BYTES if self._body is None else self._length) - len(target)
            try:
                data = self.socket.recv(min(65536, remaining))
            except BlockingIOError:
                return
            if not data:
                raise ResourceError('worker channel closed during receive')
            target.extend(data)
            if self._body is None and len(self._header) == HEADER_BYTES:
                _, self._length = inspect_header(bytes(self._header))
                self._body = bytearray()  # length checked before any body allocation
            if self._body is not None and len(self._body) == self._length:
                self._message = decode_message(bytes(self._header) + bytes(self._body))
                self._header, self._body, self._length = bytearray(), None, 0

    def close(self):
        if not self.closed:
            self.socket.close()
            self._out = self._body = self._message = None
            self._header.clear()
            self.closed = True


def socket_pair():
    left, right = socket.socketpair()
    try:
        for sock in (left, right):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
            sock.set_inheritable(False)
        # Four observed kernel buffers, plus framing/copy buffers, must fit the
        # channel reservation. Fail before launching if the host expands them.
        observed = sum(s.getsockopt(socket.SOL_SOCKET, option)
                       for s in (left, right) for option in (socket.SO_SNDBUF, socket.SO_RCVBUF))
        if observed > 2 << 20:
            raise ResourceError('observed worker socket buffers exceed their envelope')
        return left, right
    except BaseException:
        left.close()
        right.close()
        raise
