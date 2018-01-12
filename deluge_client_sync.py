import collections
import concurrent.futures
import contextlib
import errno
import logging
import os
import Queue
import socket
import ssl
import struct
import threading
import time
import weakref
import zlib

import rencode


RPC_RESPONSE = 1
RPC_ERROR = 2
RPC_EVENT = 3


def log():
    return logging.getLogger(__name__)


def auth_from_file(path, username=None):
    if not username:
        username = "localclient"
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            line = line.strip()
            lsplit = line.split(":")

            if len(lsplit) in (2, 3):
                u, password = lsplit[:2]

            if u == username:
                return (u, password)
    return (None, None)

def auth_from_config_dir(path, username=None):
    path = os.path.join(path, "auth")
    if not os.path.exists(path):
        return (None, None)
    return auth_from_file(path, username=username)


def get_localhost_auth():
    try:
        from xdg.BaseDirectory import save_config_path
    except ImportError:
        return (None, None)
    return auth_from_config_dir(save_config_path("deluge"))


class Error(Exception):

    pass


class EOF(Error):

    def __init__(self):
        super(EOF, self).__init__("EOF")


class EncodingError(Error):

    def __init__(self, *args, **kwargs):
        super(EncodingError, self).__init__(*args, **kwargs)


class RPCError(Error):

    def __init__(self, request, exc_type, exc_message, exc_traceback):
        super(RPCError, self).__init__(exc_type, exc_message)
        self.message = exc_message
        self.request = request
        self.type = exc_type
        self.traceback = exc_traceback


class Request(object):

    def __init__(self, ci, request_id, method, args, kwargs):
        # This is mainly for the strong reference.
        self.ci = ci
        self.request_id = request_id
        self.method = method
        self.args = args
        self.kwargs = kwargs

        self.future = concurrent.futures.Future()

    def cancel(self):
        return self.future.cancel()

    def cancelled(self):
        return self.future.cancelled()

    def running(self):
        return self.future.running()

    def done(self):
        return self.future.done()

    def result(self, timeout=None):
        if timeout is None:
            timeout = self.ci.timeout
        return self.future.result(timeout=timeout)

    def exception(self, timeout=None):
        if timeout is None:
            timeout = self.ci.timeout
        return self.future.exception(timeout=timeout)

    def add_done_callback(self, fn):
        return self.future.add_done_callback(fn)


class ClientInstance(object):

    RECV_SIZE = 256 * 1024
    TIMEOUT = 5

    REQUEST_LOGIN = 0
    REQUEST_SET_EVENT_INTEREST = 1
    REQUEST_NORMAL = 2

    def __init__(self, client):
        self.host = client.host
        self.port = client.port
        self.username = client.username
        self.password = client.password
        self.timeout = client.timeout

        # We do need to set sockets to be non-blocking, because
        # SSLSocket.recv() will never return on a non-blocking socket that has
        # been closed.
        self.socket = client.ssl_factory().wrap_socket(client.socket_factory())
        self.socket.settimeout(self.TIMEOUT)
        self.event_pool = client._event_pool

        self.cv = threading.Condition(lock=client._lock)
        self._event_to_handlers = client._event_to_handlers
        self._event_to_registration = {}
        self._next_request_id = 0
        self._id_to_request = weakref.WeakValueDictionary()
        self._request_queue = Queue.PriorityQueue()
        self._running = True
        self._login_stage = self.REQUEST_LOGIN
        self._protocol_version = 1

        connector = threading.Thread(
            target=self.connector, name="connector-%x" % hash(self))
        connector.daemon = True
        connector.start()

    def loginner(self):
        with self.exceptions_are_fatal(Exception):
            self.loginner_inner()

    def loginner_inner(self):
        for version in (2, 1):
            kwargs = {}
            if version != 1:
                kwargs["protocol_version"] = version
            r = self._request(
                self.REQUEST_LOGIN, "daemon.login", self.username,
                self.password, **kwargs)
            try:
                r.result()
            except RPCError as e:
                if e.type in (b"TypeError", b"InvalidProtocolVersionError"):
                    continue
                else:
                    raise
            else:
                break
        with self.cv:
            self._protocol_version = version
            self._login_stage = self.REQUEST_SET_EVENT_INTEREST
            event_names = list(self._event_to_handlers.keys())
            r = None
            if event_names:
                r = self._request(
                    self.REQUEST_SET_EVENT_INTEREST,
                    "daemon.set_event_interest", event_names)
                self._event_to_registration.update(
                    {n: r for n in event_names})
            self.cv.notify_all()
        if r:
            r.result()
        with self.cv:
            self._login_stage = self.REQUEST_NORMAL
            self.cv.notify_all()

    def running(self):
        with self.cv:
            return self._running

    def _terminate_locked(self, e):
        if not self._running:
            return

        if e:
            log().exception("terminating due to exception")
        else:
            log().debug("terminating due to client death")

        if self.socket:
            self.socket.close()

        for request in list(self._id_to_request.values()):
            if not request.future.done():
                if e is None:
                    request.future.cancel()
                else:
                    request.future.set_exception(e)

        self._running = False
        self.cv.notify_all()

    def terminate(self, e):
        with self.cv:
            self._terminate_locked(e)

    @contextlib.contextmanager
    def exceptions_are_fatal(self, exception_types):
        try:
            yield
        except exception_types as e:
            self.terminate(e)
            raise

    def receiver_inner(self):
        log().debug("starting")
        buf = b""
        first_bad_encoding_time = None
        while True:
            # Defend against stream corruption bugs I haven't fixed yet.
            if (first_bad_encoding_time is not None and
                    time.time() - first_bad_encoding_time > self.timeout):
                raise EncodingError(
                    "%ss without good encoding, assuming stream corruption" %
                    self.timeout)
            # recv() on a blocking SSLSocket won't return when the socket is
            # closed, so we need to set a timeout to avoid receiver lasting
            # forever.
            try:
                part = self.socket.recv(self.RECV_SIZE)
            except socket.timeout:
                continue
            except ssl.SSLError as e:
                # This seems to happen in python 2.7.
                if e.args == (b"The read operation timed out",):
                    continue
                raise
            except OSError as e:
                # This happens on a graceful shutdown from our side.
                if e.errno == errno.EBADF:
                    break
                # Not sure why this happens? Some race condition in ssl?
                if e.errno == errno.EAGAIN:
                    continue
                raise
            except ssl.SSLWantReadError:
                # Uh, for some reason this except block doesn't actually work.
                continue
            if len(part) == 0:
                raise EOF()
            buf += part
            while buf:
                with self.cv:
                    protocol_version = self._protocol_version
                if protocol_version < 2:
                    # I found examples of messages for which
                    # zlib.decompress(buf) == zlib.decompress(buf[:-1])
                    # That is, you can truncate one byte and still get the same
                    # message. If we receive the first n - 1 bytes in one frame,
                    # we'll successfully decompress it, but the leftover byte will
                    # corrupt the stream on the next pass.
                    for offset in range(4):
                        d = zlib.decompressobj()
                        try:
                            message = rencode.loads(d.decompress(buf[offset:]))
                            if offset:
                                log().warning("offset stream by %s...", offset)
                            break
                        except:
                            pass
                    else:
                        log().debug("bad encoding. short read?")
                        if first_bad_encoding_time is None:
                            first_bad_encoding_time = time.time()
                        break
                    first_bad_encoding_time = None
                    buf = d.unused_data
                else:
                    if len(buf) < 4:
                        break
                    length = struct.unpack("<L", buf[:4])[0]
                    if len(buf) < length + 4:
                        break
                    message = buf[4:length + 4]
                    buf = buf[length + 4:]
                    d = zlib.decompressobj()
                    try:
                        message = rencode.loads(d.decompress(message))
                    except:
                        log().warning("bad message of length %s", length)
                        continue
                if log().isEnabledFor(logging.DEBUG):
                    log().debug("received: %s", str(message)[:100])
                try:
                    self.got_message(message)
                except Exception:
                    log().exception("while processing: %s", message)

    def receiver(self):
        try:
            with self.exceptions_are_fatal(Exception):
                self.receiver_inner()
        finally:
            log().debug("shutting down")

    def get_request_locked(self):
        try:
            p, _, request = self._request_queue.get_nowait()
        except Queue.Empty:
            return None
        if p > self._login_stage:
            self._request_queue.put((p, request.request_id, request))
            return None
        return request

    def sender_inner(self):
        log().debug("starting")
        while True:
            with self.cv:
                while True:
                    if not self._running:
                        return
                    request = self.get_request_locked()
                    if request:
                        break
                    self.cv.wait()
                protocol_version = self._protocol_version
            if not request.future.set_running_or_notify_cancel():
                # Clear references
                request = None
                continue
            message = ((
                request.request_id, request.method, request.args,
                request.kwargs),)
            log().debug("sending: %s", message)
            data = zlib.compress(rencode.dumps(message))
            if protocol_version >= 2:
                data = struct.pack("<L", len(data)) + data
            # Clear references
            message = None
            request = None
            while data:
                try:
                    n = self.socket.send(data)
                except socket.timeout:
                    continue
                data = data[n:]

    def sender(self):
        try:
            with self.exceptions_are_fatal(Exception):
                self.sender_inner()
        finally:
            log().debug("shutting down")

    def got_message(self, message):
        msg_type = message[0]
        if msg_type == RPC_EVENT:
            event_name, event_data = message[1:3]
            self.got_event(event_name, *event_data)
        elif msg_type in (RPC_RESPONSE, RPC_ERROR):
            request_id, result = message[1:3]
            self.got_rpc_response_or_error(msg_type, request_id, result)
        else:
            log().error("received unknown RPC message type %s", msg_type)

    def got_event(self, event_name, *event_data):
        with self.cv:
            handlers = list(self._event_to_handlers.get(event_name, ()))
        for handler in handlers:
            self.event_pool.submit(handler, *event_data)

    def got_rpc_response_or_error(self, msg_type, request_id, result):
        try:
            with self.cv:
                request = self._id_to_request.pop(request_id)
        except KeyError:
            log().debug(
                "got response to msg %s, which doesn't exist", request_id)
            return

        if request.future.done():
            log().error("msg %s already complete?", request_id)
        elif not request.future.running():
            log().error("msg %s never started?", request_id)
        elif msg_type == RPC_RESPONSE:
            request.future.set_result(result)
        else:
            exc_type, message, exc_traceback = result[0:3]
            request.future.set_exception(RPCError(
                request, exc_type, message, exc_traceback))

    def connector_inner(self):
        log().debug("connecting to %s:%s", self.host, self.port)
        self.socket.connect((self.host, self.port))
        log().debug("connected")

        loginner = threading.Thread(
            target = self.loginner, name="loginner-%x" % hash(self))
        loginner.daemon = True
        loginner.start()
        receiver = threading.Thread(
            target=self.receiver, name="receiver-%x" % hash(self))
        receiver.daemon = True
        receiver.start()
        sender = threading.Thread(
            target=self.sender, name="sender-%x" % hash(self))
        sender.daemon = True
        sender.start()

    def connector(self):
        with self.exceptions_are_fatal(Exception):
            self.connector_inner()

    def _request(self, p, method, *args, **kwargs):
        with self.cv:
            request = Request(
                self, self._next_request_id, method, args, kwargs)
            self._next_request_id += 1
            self._id_to_request[request.request_id] = request
            self._request_queue.put((p, request.request_id, request))
            self.cv.notify_all()
            return request

    def request(self, method, *args, **kwargs):
        return self._request(self.REQUEST_NORMAL, method, *args, **kwargs)

    def request_register_event(self, event_name):
        with self.cv:
            r = self._event_to_registration.get(event_name)
            if (not r) or (r.future.done() and r.future.exception()):
                r = self._request(
                    self.REQUEST_SET_EVENT_INTEREST,
                    "daemon.set_event_interest", [event_name])
                self._event_to_registration[event_name] = r
            return r


def add_arguments(parser, create_group=False):
    if create_group:
        target = parser.add_argument_group("Deluge Client Options")
    else:
        target = parser

    target.add_argument("--deluge_config_dir")
    target.add_argument("--deluge_host")
    target.add_argument("--deluge_port", type=int, default=Client.DEFAULT_PORT)
    target.add_argument("--deluge_username")
    target.add_argument("--deluge_password")
    target.add_argument(
        "--deluge_timeout", type=int, default=Client.DEFAULT_TIMEOUT)
    target.add_argument(
        "--deluge_max_event_workers", type=int,
        default=Client.DEFAULT_EVENT_WORKERS)

    return target


class Client(object):

    DEFAULT_PORT = 58846
    DEFAULT_TIMEOUT = 30
    DEFAULT_EVENT_WORKERS = 16

    @classmethod
    def from_args(cls, parser, args, **kwargs):
        return cls(
            host=args.deluge_host, port=args.deluge_port,
            username=args.deluge_username, password=args.deluge_password,
            config_dir=args.deluge_config_dir, timeout=args.deluge_timeout,
            **kwargs)

    def __init__(self, host=None, port=None, username=None, password=None,
                 config_dir=None, timeout=None, max_event_workers=None,
                 ssl_factory=None, socket_factory=None):
        self.host = host or "localhost"
        self.port = port or self.DEFAULT_PORT
        if not password and config_dir:
            username, password = auth_from_config_dir(
                config_dir, username=username)
        if not password and host in ("127.0.0.1", "localhost"):
            username, password = get_localhost_auth()
        self.username = username or ""
        self.password = password or ""
        self.timeout = timeout if timeout is not None else self.DEFAULT_TIMEOUT
        max_event_workers = max_event_workers or self.DEFAULT_EVENT_WORKERS

        if ssl_factory is not None:
            self.ssl_factory = ssl_factory
        if socket_factory is not None:
            self.socket_factory = socket_factory

        self._lock = threading.RLock()
        self._ci = None
        self._event_to_handlers = collections.defaultdict(set)
        self._event_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_event_workers)

    def __del__(self):
        if self._ci:
            self._ci.terminate(None)

    def _get_ci(self):
        with self._lock:
            if self._ci is None or not self._ci.running():
                self._ci = ClientInstance(self)
            return self._ci

    def ssl_factory(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def socket_factory(self):
        return socket.socket()

    def request(self, method, *args, **kwargs):
        return self._get_ci().request(method, *args, **kwargs)

    def call(self, method, *args, **kwargs):
        if "timeout" in kwargs:
            timeout = kwargs.pop("timeout")
        else:
            timeout = self.timeout
        # Explicitly hold a reference in this frame
        request = self.request(method, *args, **kwargs)
        return request.future.result(timeout=timeout)

    def add_event_handler(self, event_name, handler, timeout=None):
        if timeout is None:
            timeout = self.timeout
        with self._lock:
            self._event_to_handlers[event_name].add(handler)
            request = self._get_ci().request_register_event(event_name)
        return request.future.result(timeout=timeout)

    def remove_event_handler(self, event_name, handler):
        with self._lock:
            try:
                self._event_to_handlers[event_name].remove(handler)
            finally:
                if not self._event_to_handlers[event_name]:
                    del self._event_to_handlers[event_name]
