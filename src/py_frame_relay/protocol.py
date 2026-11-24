"""Length-prefixed, CRC-checked framing over a TCP byte stream."""

import struct
import zlib
from collections.abc import Iterator

MAGIC = b"PYFR"
MAX_PAYLOAD_SIZE = 65_536  # cap per-client buffering
HEADER = struct.Struct("!4sII")  # magic, payload length, crc32 (big-endian)
HEADER_SIZE = HEADER.size


class FramingError(ValueError):
    """Stream is misaligned or abusive; caller should drop the connection."""


def _crc(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def encode_frame(payload: bytes) -> bytes:
    if len(payload) > MAX_PAYLOAD_SIZE:
        raise ValueError(f"payload of {len(payload)} bytes exceeds {MAX_PAYLOAD_SIZE}")
    return HEADER.pack(MAGIC, len(payload), _crc(payload)) + payload


class FrameDecoder:
    """Buffers partial frames across recv() calls."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, chunk: bytes) -> Iterator[bytes | None]:
        """Yield each payload, or None if its CRC failed.

        Bad CRC is skippable (length was valid, so we stay aligned). Bad magic or
        length raises FramingError after earlier frames have been yielded.
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
