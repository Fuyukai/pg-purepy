"""
Index of builtin converters.
"""
import abc
from typing import Any, Callable, Tuple, Dict, List

import attr


@attr.s(slots=True, frozen=False)
class ConversionContext:
    """
    A conversion context contains information that might be needed to convert from the PostgreSQL
    string representation to the real representation.
    """

    #: The encoding of the client.
    client_encoding: str = attr.ib()


class Converter(abc.ABC):
    """
    A converter converts between the PostgreSQL representation of data and the Python
    representation, in either direction.
    """

    #: The OID of the PostgreSQL type this converter uses.
    oid: int

    @abc.abstractmethod
    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        """
        Converts ``data`` from the PostgreSQL string representation to a Python type.
        """

    @abc.abstractmethod
    def to_postgres(self, context: ConversionContext, data: Any) -> str:
        """
        Converts ``data`` from the Python type to the PostgreSQL string representation.
        """


class SimpleFunctionConverter(Converter):
    """
    A simple converter that calls fn_from_pg to convert to a Python type, and fn_to_pg to convert
    from a Python type.
    """

    def __init__(self, oid: int, fn_from_pg, fn_to_pg):
        self.oid = oid

        self._to = fn_from_pg
        self._from = fn_to_pg

    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        return self._to(data)

    def to_postgres(self, context: ConversionContext, data: Any) -> str:
        return self._from(data)


class BoolConverter(Converter):
    """
    Converter that converts for booleans.
    """

    def __init__(self):
        self.oid = 16

    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        return data == "t"

    def to_postgres(self, context: ConversionContext, data: Any) -> str:
        return "true" if data else "false"


class TextConverter(Converter):
    """
    Pass-through converter for the various text types.
    """

    def __init__(self, oid: int):
        self.oid = oid

    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        return data

    def to_postgres(self, context: ConversionContext, data: Any) -> str:
        return data


_PASSTHROUGH = lambda it: it
DEFAULT_TEXT_CONVERTER = SimpleFunctionConverter(-1, _PASSTHROUGH, _PASSTHROUGH)

#: A list of the default type converters.
DEFAULT_CONVERTERS: List[Converter] = [
    BoolConverter(),  # bool
    SimpleFunctionConverter(20, int, str),  # int8
    SimpleFunctionConverter(21, int, str),  # int2
    SimpleFunctionConverter(23, int, str),  # int4
    SimpleFunctionConverter(26, int, str),  # oid
    SimpleFunctionConverter(18, _PASSTHROUGH, _PASSTHROUGH),  # char
    SimpleFunctionConverter(19, _PASSTHROUGH, _PASSTHROUGH),  # name
    SimpleFunctionConverter(25, _PASSTHROUGH, _PASSTHROUGH),  # text
]
