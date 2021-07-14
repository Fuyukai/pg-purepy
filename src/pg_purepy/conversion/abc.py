from __future__ import annotations

import abc
from datetime import tzinfo
from typing import Any

import attr
from dateutil.tz import UTC


class Converter(metaclass=abc.ABCMeta):
    """
    Base class for all conversion classes. Implement this to create a custom converter.
    """

    #: The OID of the PostgreSQL type this converter uses.
    oid: int

    @abc.abstractmethod
    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        """
        Converts ``data`` from the PostgreSQL string representation to a Python type.

        :param context: The conversion context this converter was invoked in.
        :param data: The raw string data.
        :return: Any Python object that resulted from the conversion.
        """

    @abc.abstractmethod
    def to_postgres(self, context: ConversionContext, data: Any) -> str:
        """
        Converts ``data`` from the Python type to the PostgreSQL string representation.

        :param context: The conversion context this converter was invoked in.
        :param data: The Python object that needs to be converted.
        :return: The string data that will be used in a query string.
        """


@attr.s(slots=True, frozen=False)
class ConversionContext:
    """
    A conversion context contains information that might be needed to convert from the PostgreSQL
    string representation to the real representation.
    """

    #: The encoding of the client.
    client_encoding: str = attr.ib()

    #: The timezone of the server.
    timezone: tzinfo = attr.ib(default=UTC)
