import os
import socket
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path

import pytest

from py_frame_relay import FrameDecoder, TelemetryServer, encode_frame


def wait_for(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out waiting for condition"
        time.sleep(0.005)


def read_log(path: Path) -> list[bytes]:
    frames = list(FrameDecoder().feed(path.read_bytes()))
    assert None not in frames
    return frames  # type: ignore[return-value]


@pytest.fixture
def log_path(tmp_path: Path) -> Path:
    return tmp_path / "telemetry.log"


@pytest.fixture
def server(log_path: Path) -> Iterator[TelemetryServer]:
    with TelemetryServer("127.0.0.1", 0, log_path) as srv:
        thread = threading.Thread(target=srv.serve_forever)
        thread.start()
        try:
            yield srv
        finally:
            srv.stop()
            thread.join(timeout=5)


@contextmanager
def connect(server: TelemetryServer) -> Iterator[socket.socket]:
    with socket.create_connection(server.server_address, timeout=5) as sock:
        yield sock


def test_trickle_fragmentation(server: TelemetryServer, log_path: Path) -> None:
    with connect(server) as sock:
        for byte in encode_frame(b"hello telemetry"):
            sock.sendall(bytes([byte]))
        wait_for(lambda: server.stats.frames_ok == 1)
    assert read_log(log_path) == [b"hello telemetry"]


def test_coalesced_burst_and_concurrency(server: TelemetryServer, log_path: Path) -> None:
    clients, per_client = 4, 60

    def blast(cid: int) -> None:
        burst = b"".join(encode_frame(f"c{cid}-{i}".encode()) for i in range(per_client))
        with connect(server) as sock:
            sock.sendall(burst)
            wait_for(lambda: server.stats.frames_ok >= 1)  # keep socket open while draining

    threads = [threading.Thread(target=blast, args=(c,)) for c in range(clients)]
    for t in threads:
        t.start()
    wait_for(lambda: server.stats.frames_ok == clients * per_client)
    for t in threads:
        t.join()

    expected = {f"c{c}-{i}".encode() for c in range(clients) for i in range(per_client)}
    frames = read_log(log_path)
    assert len(frames) == len(expected) and set(frames) == expected
    assert server.stats.frames_corrupt == 0


def test_payload_bit_corruption(server: TelemetryServer, log_path: Path) -> None:
    f1, f2, f3 = (encode_frame(p) for p in (b"one", b"two", b"three"))
    flipped = bytearray(f2)
    flipped[-1] ^= 0b0000_0001  # flip one payload bit; header stays valid
    with connect(server) as sock:
        sock.sendall(f1 + bytes(flipped) + f3)
        wait_for(lambda: server.stats.frames_ok == 2)
        assert server.stats.frames_corrupt == 1
        assert server.stats.connections_closed == 0
    assert read_log(log_path) == [b"one", b"three"]


def test_bad_magic_closes_bad_client_only(server: TelemetryServer, log_path: Path) -> None:
    with connect(server) as bad, connect(server) as good:
        bad.sendall(b"GARBAGE-NOT-A-FRAME!")
        wait_for(lambda: server.stats.connections_closed == 1)
        with suppress(ConnectionResetError):  # a reset is also a valid disconnect signal
            assert bad.recv(1) == b""  # server closed its end
        good.sendall(encode_frame(b"still alive"))
        wait_for(lambda: server.stats.frames_ok == 1)
    assert read_log(log_path) == [b"still alive"]


def test_durable_fsync_called(
    server: TelemetryServer, log_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    synced: list[int] = []
    real_fsync = os.fsync

    def spy(fd: int) -> None:
        synced.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", spy)
    with connect(server) as sock:
        sock.sendall(encode_frame(b"a") + encode_frame(b"b"))
        wait_for(lambda: server.stats.frames_ok == 2)
    assert len(synced) == 2  # one fsync per persisted frame
    assert read_log(log_path) == [b"a", b"b"]
