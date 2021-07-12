from __future__ import annotations

from collections import deque
from typing import List


def pack_strings(*s: str, encoding: str = "ascii") -> bytes:
    """
    Packs a sequence of strings using null terminators.
    """
    return b"\x00".join(x.encode(encoding) for x in s) + b"\x00"


class Buffer(object):
    """
    Simple buffer that allows reading data off of a bytearray ala Java ByteBuffer.
    """

    def __init__(self, ba: bytearray = None):
        self.data = deque(ba if ba else b"")

    def __bool__(self):
        return bool(self.data)

    def __len__(self):
        return len(self.data)

    def read_bytes(self, count: int):
        ba = bytearray()
        for x in range(0, count):
            ba.append(self.data.popleft())

        return ba

    def read_byte(self) -> int:
        return self.data.popleft()

    def read_short(self) -> int:
        return int.from_bytes(self.read_bytes(2), byteorder="big", signed=True)

    def read_int(self) -> int:
        return int.from_bytes(self.read_bytes(4), byteorder="big", signed=True)

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

    def read_all_cstrings(self, encoding: str, drop_empty: bool = False) -> List[str]:
        """
        Reads all null-terminated C strings from the buffer.
        """
        items = []
        while self.data:
            items.append(self.read_cstring(encoding))

        if drop_empty:
            return [i for i in items if i]
        else:
            return items

    def read_remaining(self) -> bytes:
        """
        Reads out the remainer of this buffer.
        """
        ba = bytes(bytearray(self.data))
        self.data = deque()
        return ba
