from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pg_purepy.conversion.abc import Converter

if TYPE_CHECKING:
    from pg_purepy.protocol import ConversionContext


ConvType = TypeVar("ConvType")


class SimpleFunctionConverter(Converter, Generic[ConvType]):
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

    def from_postgres(self, context: ConversionContext, data: str) -> ConvType:
        return self._from_pg(data)

    def to_postgres(self, context: ConversionContext, data: ConvType) -> str:
        return self._to_pg(data)


class BoolConverter(Converter):
    """
    Converter that converts for booleans.
    """

    oid = 16

    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        return data == "t"

    def to_postgres(self, context: ConversionContext, data: bool) -> str:
        return "true" if data else "false"


class TextConverter(Converter):
    """
    Text converter for default text types.
    """

    def __init__(self, oid: int):
        self.oid = oid

    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        return data

    def to_postgres(self, context: ConversionContext, data: Any) -> str:
        return str(data)


class ByteaConverter(Converter):
    """
    Converter that turns bytes objects into bytea objects.
    """

    oid = 17

    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        # bytea data is hex escaped.
        prefix, rest = data[0:2], data[2:]
        assert prefix == r"\x"

        return bytes.fromhex(rest)

    def to_postgres(self, context: ConversionContext, data: bytes) -> str:
        return r"\x" + data.hex()
