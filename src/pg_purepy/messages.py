from __future__ import annotations

import abc
import enum
import logging
from io import StringIO
from typing import Optional, List, Any

import attr
from pg_purepy.exc import ProtocolParseError, PostgresqlError

logging.basicConfig(level=logging.DEBUG)


@attr.s(slots=True, frozen=True)
class PostgresMessage(object):
    """
    Base class for a PostgreSQL protocol message.
    """


class AuthenticationMethod(enum.IntEnum):
    """
    Enumeration of supported authentication methods.
    """

    CLEARTEXT = 3
    MD5 = 5
    SASL = 10


@attr.s(slots=True, frozen=True)
class AuthenticationRequest(PostgresMessage):
    """
    Returned when the PostgreSQL server requires authentication.
    """

    #: The authentication method required.
    method: AuthenticationMethod = attr.ib()

    #: When doing MD5 authentication, the salt to use.
    md5_salt: Optional[bytes] = attr.ib(default=None)

    #: When doing SASL authentication, the list of authentication methods.
    sasl_methods: List[str] = attr.ib(default=[])


@attr.s(slots=True, frozen=True)
class BackendKeyData(PostgresMessage):
    """
    Misc data used for cancellation.
    """

    #: The PID of this connection.
    pid: int = attr.ib()

    #: The secret key data of this connection.
    secret_key: int = attr.ib()


@attr.s(slots=True, frozen=True)
class AuthenticationCompleted(PostgresMessage):
    """
    Returned when authentication is completed.
    """


@attr.s(slots=True, frozen=True)
class ParameterStatus(PostgresMessage):
    """
    Returned when a configuration parameter is changed, e.g. via SET.
    """

    #: The name of the parameter.
    name: str = attr.ib()

    #: The value of the parameter.
    value: str = attr.ib()


class ReadyForQueryState(enum.Enum):
    """
    Enumeration of possible ReadyForQuery states.
    """

    IDLE = ord("I")
    IN_TRANSACTION = ord("T")
    ERRORED_TRANSACTION = ord("E")


@attr.s(slots=True, frozen=True)
class ReadyForQuery(PostgresMessage):
    """
    Returned when the server is ready for the next query cycle.
    """

    #: The sub-state that the current connection is in.
    state: ReadyForQueryState = attr.ib()


class ErrorResponseFieldType(enum.IntEnum):
    """
    Enumeration of possible error response fields.
    """

    SEVERITY_LOCALISED = ord("S")
    SEVERITY = ord("V")
    CODE = ord("C")
    MESSAGE = ord("M")
    DETAIL = ord("D")
    HINT = ord("H")
    POSITION = ord("P")
    INTERNAL_POSITION = ord("p")
    INTERNAL_QUERY = ord("q")
    WHERE = ord("W")
    SCHEMA_NAME = ord("s")
    TABLE_NAME = ord("t")
    COLUMN_NAME = ord("c")
    DATA_TYPE_NAME = ord("t")
    CONSTRAINT_NAME = ord("n")

    # 0 will never exist
    UNKNOWN = 0


@attr.s(slots=True, frozen=True)
class ColumnDescription:
    """
    A description of a column.
    """

    #: The name of this column.
    name: str = attr.ib()

    #: The table OID of this column.
    table_oid: Optional[int] = attr.ib()

    #: The column index of this column.
    column_index: Optional[int] = attr.ib()

    #: The type OID of this column.
    type_oid: int = attr.ib()

    #: The internal column length.
    column_length: int = attr.ib()

    #: The type modifier for this column.
    type_modifier: int = attr.ib()


@attr.s(slots=True, frozen=True)
class QueryResultMessage(PostgresMessage, abc.ABC):
    """
    Superclass for query results.
    """


@attr.s(slots=True, frozen=True)
class RowDescription(QueryResultMessage):
    """
    Describes the rows of a query.
    """

    columns: List[ColumnDescription] = attr.ib()


@attr.s(slots=True, frozen=True)
class DataRow(QueryResultMessage):
    """
    A singular data row. This contains a :class:`.RowDescription` and a list of converted data
    values.
    """

    #: The :class:`.RowDescription` that describes the data within this row.
    description: RowDescription = attr.ib()

    #: A list of column values, in the same order as the description, that contains the actual data
    #: incoming from the server.
    data: List[Any] = attr.ib()


@attr.s(slots=True, frozen=True)
class CommandComplete(QueryResultMessage):
    """
    Returned when a single query command is complete.
    """

    #: The command tag. Probably useless.
    tag: str = attr.ib()

    #: The row count returned. This may be None if the command does not have a row count
    #: (e.g. SHOW or SET).
    row_count: Optional[int] = attr.ib()


def _optional_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None

    return int(value)


@attr.s(slots=True, frozen=True)
class ErrorResponse(PostgresMessage):
    """
    Returned when an error is encountered by the server.
    """

    severity_localised: str = attr.ib()
    severity: str = attr.ib()
    code: str = attr.ib()
    message: str = attr.ib()

    # optional parameters
    detail: Optional[str] = attr.ib(default=None)
    position: Optional[int] = attr.ib(default=None, converter=_optional_int)
    internal_position: Optional[int] = attr.ib(default=None, converter=_optional_int)
    internal_query: Optional[str] = attr.ib(default=None)
    where: Optional[str] = attr.ib(default=None)
    schema_name: Optional[str] = attr.ib(default=None)
    table_name: Optional[str] = attr.ib(default=None)
    column_name: Optional[str] = attr.ib(default=None)
    data_type_name: Optional[str] = attr.ib(default=None)
    constraint_name: Optional[str] = attr.ib(default=None)


class DatabaseError(PostgresqlError):
    """
    An exception produceed from the database, usually from an ErrorResponse message.
    """

    def __init__(self, response: ErrorResponse, query: str = None):
        self.response = response

    def __str__(self) -> str:
        buf = StringIO()
        buf.write(self.response.severity)
        buf.write(":")
        buf.write(" [code ")
        buf.write(str(self.response.code))
        buf.write("] ")
        buf.write(self.response.message)

        # TODO: Optional fields
        return buf.getvalue()


class InvalidPasswordError(DatabaseError):
    """
    Raised when the password provided is invalid.
    """


def wrap_error(response: ErrorResponse, query: str = None) -> DatabaseError:
    """
    Wraps a :class:`.ErrorResponse` in an exception. If a query produced the error in question, then
    passing it as the ``query`` param can produce a prettier error.
    """
    # TODO: More codes
    if response.code == "28P01":
        return InvalidPasswordError(response, query)

    else:
        return DatabaseError(response, query)
