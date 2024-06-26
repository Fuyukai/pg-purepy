from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Self, override

if TYPE_CHECKING:
    from pg_purepy.connection import AsyncPostgresConnection


class PostgresqlError(Exception):
    """
    Base exception class all other exceptions are derived from.
    """


class ConnectionForciblyKilledError(PostgresqlError):
    """
    Raised when a connection is forcibly killed.
    """

    def __init__(self, conn: AsyncPostgresConnection):
        self._conn = conn

    @override
    def __str__(self) -> str:
        return f"Connection {self._conn!r} could not send the Terminate message"

    __repr__: Callable[[Self], str] = __str__


class ProtocolParseError(PostgresqlError):
    """
    Base exception class for all protocol parsing related errors.
    """


class MissingPasswordError(ProtocolParseError):
    """
    Raised when the server asks for a password, but we don't have one.
    """


class UnknownMessageError(ProtocolParseError):
    """
    Raised when an unknown message is returned.
    """


class IllegalStateError(ProtocolParseError):
    """
    Raised when an operation is attempted that would result in an illegal state.
    """


class ConnectionInTransactionWarning(ResourceWarning):
    """
    Raised when a connection is returned to a connection pool whilst it is still in a transaction.
    """


class MissingRowError(ValueError):
    """
    Raised when there's no row during a ``fetch_one`` operation.
    """
