from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, override

from pg_purepy.conversion.abc import Converter

if TYPE_CHECKING:
    from pg_purepy.protocol import ConversionContext


class SimpleFunctionConverter[ConvType](Converter[ConvType]):
    """
    A simple converter that calls fn_from_pg to convert to a Python type, and fn_to_pg to convert
    from a Python type.
    """

    def __init__(
        self, oid: int, fn_from_pg: Callable[[str], ConvType], fn_to_pg: Callable[[ConvType], str]
    ) -> None:
        self.oid = oid

        self._from_pg = fn_from_pg
        self._to_pg = fn_to_pg

    @override
    def from_postgres(self, context: ConversionContext, data: str) -> ConvType:
        return self._from_pg(data)

    @override
    def to_postgres(self, context: ConversionContext, data: ConvType) -> str:
        return self._to_pg(data)


class BoolConverter(Converter[bool]):
    """
    Converter that converts for booleans.
    """

    oid = 16

    @override
    def from_postgres(self, context: ConversionContext, data: str) -> bool:
        return data == "t"

    @override
    def to_postgres(self, context: ConversionContext, data: bool) -> str:
        return "true" if data else "false"


class TextConverter(Converter[str]):
    """
    Text converter for default text types.
    """

    def __init__(self, oid: int):
        self.oid = oid

    @override
    def from_postgres(self, context: ConversionContext, data: str) -> str:
        return data

    @override
    def to_postgres(self, context: ConversionContext, data: str) -> str:
        return str(data)


class ByteaConverter(Converter[bytes]):
    """
    Converter that turns bytes objects into bytea objects.
    """

    oid = 17

    @override
    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        # bytea data is hex escaped.
        prefix, rest = data[0:2], data[2:]
        assert prefix == r"\x"

        return bytes.fromhex(rest)

    @override
    def to_postgres(self, context: ConversionContext, data: bytes) -> str:
        return r"\x" + data.hex()
