from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pg_purepy.protocol import ConversionContext


class Converter(metaclass=abc.ABCMeta):
    """
    Base class for all conversion classes.
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
