from __future__ import annotations

from collections import deque
from logging import Logger, getLogger
from typing import Any

import attr


@attr.s(slots=True, frozen=True, kw_only=True)
class LoggerWithTrace:
    logger: Logger = attr.ib()

    @classmethod
    def get(cls, name: str) -> LoggerWithTrace:
        return LoggerWithTrace(logger=getLogger(name))

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.logger.info(*args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.logger.warning(*args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self.logger.debug(*args, **kwargs)

    def critical(self, *args: Any, **kwargs: Any) -> None:
        self.logger.critical(*args, **kwargs)

    def trace(self, message: str, *args: Any, **kws: Any) -> None:
        self.logger.log(5, message, *args, **kws)


def pack_strings(*s: str, encoding: str = "ascii") -> bytes:
    """
    Packs a sequence of strings using null terminators.
    """
    return b"\x00".join(x.encode(encoding) for x in s) + b"\x00"


class Buffer:
    """
    Simple buffer that allows reading data off of a bytearray ala Java ByteBuffer.
    """

    def __init__(self, ba: bytearray | None = None) -> None:
        self.data: deque[int] = deque(ba if ba else b"")

    def __bool__(self) -> bool:
        return bool(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def read_bytes(self, count: int) -> bytearray:
        ba = bytearray()
        for _x in range(0, count):
            ba.append(self.data.popleft())

        return ba

    def read_byte(self) -> int:
        return self.data.popleft()

    def read_short(self) -> int:
        return int.from_bytes(self.read_bytes(2), byteorder="big", signed=True)

    def read_int(self) -> int:
        return int.from_bytes(self.read_bytes(4), byteorder="big", signed=True)

    def read_uint(self) -> int:
        return int.from_bytes(self.read_bytes(4), byteorder="big", signed=False)

    def read_long(self) -> int:
        return int.from_bytes(self.read_bytes(8), byteorder="big", signed=True)

    def read_cstring(self, encoding: str) -> str:
        """
        Reads a null-terminated C string.
        """
        buf = bytearray()
        while True:
            byte = self.data.popleft()
            if byte == 0x0:
                break

            buf.append(byte)

        return buf.decode(encoding=encoding)

    def read_all_cstrings(self, encoding: str, drop_empty: bool = False) -> list[str]:
        """
        Reads all null-terminated C strings from the buffer.
        """
        items = []
        while self.data:
            items.append(self.read_cstring(encoding))

        if drop_empty:
            return [i for i in items if i]

        return items

    def read_remaining(self) -> bytes:
        """
        Reads out the remainder of this buffer.
        """

        ba = bytes(bytearray(self.data))
        self.data = deque()
        return ba
