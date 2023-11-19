"""
Converter type for array objects.
"""

from functools import partial
from typing import Any

from pg_purepy.conversion.abc import ConversionContext, Converter

## BEGIN
from pg_purepy.conversion.array_parse import _parse_array


class ArrayConverter(Converter):
    """
    Converts arrays to Python lists. This requires a subconverter which will be called to convert
    every value in the array.
    """

    def __init__(self, oid: int, subconverter: Converter, quote_inner: bool = False) -> None:
        """
        :param oid: The OID of the array type (not the base type!)
        :param subconverter: The converter for individual elements inside the array.
        :param quote_inner: When converting to PostgreSQL, if inner elements should be quoted.
        """
        self.oid = oid
        self._subconverter = subconverter
        self._quote_inner = quote_inner

    def from_postgres(self, context: ConversionContext, data: str) -> list[Any]:
        p = partial(self._subconverter.from_postgres, context)
        return _parse_array(data, p)

    def to_postgres(self, context: ConversionContext, data: list[Any]) -> str:
        converted = [
            self._subconverter.to_postgres(context, i) if i is not None else "NULL" for i in data
        ]
        if self._quote_inner:
            converted = ['"' + i + '"' for i in converted]

        return "{" + ",".join(converted) + "}"
