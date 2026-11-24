"""Single-threaded, non-blocking telemetry collector."""

import logging
import os
import selectors
import socket
import threading
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Self

from py_frame_relay.protocol import FrameDecoder, FramingError, encode_frame

log = logging.getLogger(__name__)
RECV_SIZE = 4096


@dataclass(slots=True)
class Connection:
    sock: socket.socket
    addr: tuple[str, int]
    decoder: FrameDecoder


@dataclass(slots=True)
class Stats:
    frames_ok: int = 0
    frames_corrupt: int = 0
    connections_closed: int = 0


class TelemetryServer:
    """Serves all clients from one thread.

    DefaultSelector is epoll on Linux, kqueue/select elsewhere.
    Note: fsync per frame blocks the loop; a slow disk stalls every client.
    """

    def __init__(self, host: str, port: int, log_path: Path) -> None:
        self._bind = (host, port)
        self._log_path = log_path
        self._stop = threading.Event()
        self._stack = ExitStack()
        self.stats = Stats()
        self.server_address: tuple[str, int] = self._bind

    def __enter__(self) -> Self:
        with ExitStack() as stack:  # close anything already opened if setup fails
            self._log: IO[bytes] = stack.enter_context(self._log_path.open("ab"))
            self._sel = stack.enter_context(selectors.DefaultSelector())
            listener = stack.enter_context(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(self._bind)
            listener.listen()
            listener.setblocking(False)
            self.server_address = listener.getsockname()
            self._sel.register(listener, selectors.EVENT_READ)
            self._stack = stack.pop_all()
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            for key in list(self._sel.get_map().values()):
                key.fileobj.close()
        finally:
            self._stack.close()

    def stop(self) -> None:
        self._stop.set()

    def serve_forever(self) -> None:
        while not self._stop.is_set():
            for key, _ in self._sel.select(timeout=0.05):
                if key.data is None:
                    self._accept(key.fileobj)  # type: ignore[arg-type]
                else:
                    self._read(key.data)

    def _accept(self, listener: socket.socket) -> None:
        while True:  # one event can cover several pending connections
            try:
                sock, addr = listener.accept()
            except BlockingIOError:
                return
            except OSError:
                log.exception("accept failed")
                return
            sock.setblocking(False)
            self._sel.register(sock, selectors.EVENT_READ, Connection(sock, addr, FrameDecoder()))

    def _read(self, conn: Connection) -> None:
        try:
            chunk = conn.sock.recv(RECV_SIZE)
        except BlockingIOError:
            return
        except OSError as exc:
            log.warning("dropping %s: %s", conn.addr, exc)
            return self._close(conn)
        if not chunk:
            return self._close(conn)
        try:
            # Disk errors in _handle are not caught: blaming the client would hide
            # lost data, so they stop the server instead.
            for payload in conn.decoder.feed(chunk):
                self._handle(conn, payload)
        except FramingError as exc:
            log.warning("dropping %s: %s", conn.addr, exc)
            self._close(conn)

    def _handle(self, conn: Connection, payload: bytes | None) -> None:
        if payload is None:
            self.stats.frames_corrupt += 1
            log.warning("corrupt frame from %s", conn.addr)
            return
        self._log.write(encode_frame(payload))  # keep framing so the log is parseable
        self._log.flush()
        os.fsync(self._log.fileno())
        self.stats.frames_ok += 1

    def _close(self, conn: Connection) -> None:
        self._sel.unregister(conn.sock)
        conn.sock.close()
        self.stats.connections_closed += 1
