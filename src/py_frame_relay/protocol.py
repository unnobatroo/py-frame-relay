"""Length-prefixed, CRC-checked frames over TCP."""

import struct
import zlib
from collections.abc import Iterator

MAGIC = b"PYFR"
MAX_PAYLOAD_SIZE = 65_536  # bounds per-client buffer
HEADER = struct.Struct("!4sII")  # magic, length, crc32
HEADER_SIZE = HEADER.size


class FramingError(ValueError):
    """Stream is out of sync. Drop the client."""


def _crc(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def encode_frame(payload: bytes) -> bytes:
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError(f"payload of {len(payload)} bytes exceeds {MAX_PAYLOAD_SIZE}")
    return HEADER.pack(MAGIC, len(payload), _crc(payload)) + payload


class FrameDecoder:
    """Buffers partial frames between recv() calls."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, chunk: bytes) -> Iterator[bytes | None]:
        """Yield payloads, or None on bad CRC.

        Bad CRC is skippable since the length is still good. Bad magic or
        length raises FramingError after earlier frames are yielded.
        """
        self._buf += chunk
        while len(self._buf) >= HEADER_SIZE:
            magic, length, expected_crc = HEADER.unpack_from(self._buf)
            if magic != MAGIC or length > MAX_PAYLOAD_SIZE:
                raise FramingError(f"bad header: magic={bytes(magic)!r} length={length}")
            end = HEADER_SIZE + length
            if len(self._buf) < end:
                return
            payload = bytes(self._buf[HEADER_SIZE:end])
            del self._buf[:end]
            yield payload if _crc(payload) == expected_crc else None
