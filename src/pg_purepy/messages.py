from __future__ import annotations

import abc
import enum
import logging
from io import StringIO
from typing import Any

import attr

from pg_purepy.exc import PostgresqlError

logging.basicConfig(level=logging.DEBUG)


@attr.s(slots=True, frozen=False)
class PostgresMessage:
    """
    Base class for a PostgreSQL protocol message.
    """


class AuthenticationMethod(enum.IntEnum):
    """
    Enumeration of supported authentication methods.
    """

    #: The server wishes for us to send our password in clear text.
    CLEARTEXT = 3
    #: The server wishes for us to use MD5 hashing for our password.
    MD5 = 5
    #: The server wishes for us to use SASL authentication.
    SASL = 10


@attr.s(slots=True)
class AuthenticationRequest(PostgresMessage):
    """
    Returned when the PostgreSQL server requires authentication.
    """

    #: The authentication method required.
    method: AuthenticationMethod = attr.ib()

    #: When doing MD5 authentication, the salt to use.
    md5_salt: bytes | None = attr.ib(default=None)

    #: When doing SASL authentication, the list of authentication methods.
    sasl_methods: list[str] = attr.ib(default=[])


@attr.s(slots=True)
class SASLContinue(PostgresMessage):
    """
    Returned when the PostgreSQL server wants us to continue doing SASL authentication.
    """


@attr.s(slots=True)
class SASLComplete(PostgresMessage):
    """
    Returned when SASL authentication is complete.
    """


@attr.s(slots=True)
class BackendKeyData(PostgresMessage):
    """
    Misc data used for cancellation.
    """

    #: The :class:`int` PID of this connection.
    pid: int = attr.ib()

    #: The 64-bit :class:`int` secret key data of this connection.
    secret_key: int = attr.ib()


@attr.s(slots=True)
class AuthenticationCompleted(PostgresMessage):
    """
    Returned when authentication is completed.
    """


@attr.s(slots=True)
class ParameterStatus(PostgresMessage):
    """
    Returned when a configuration parameter is changed, e.g. via SET.
    """

    #: The :class:`str` name of the parameter.
    name: str = attr.ib()

    #: The :class:`str` value of the parameter.
    value: str = attr.ib()


class ReadyForQueryState(enum.Enum):
    """
    Enumeration of possible ReadyForQuery states.
    """

    #: The server is idle and not in any transaction.
    IDLE = ord("I")

    #: The server is currently in a transaction, and new commands can be issued.
    IN_TRANSACTION = ord("T")

    #: The server is currently in a transaction that has errored, and no new commands can be issued.
    ERRORED_TRANSACTION = ord("E")


@attr.s(slots=True)
class ReadyForQuery(PostgresMessage):
    """
    Returned when the protocol machine is ready for the next query cycle.
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

    #: The :class:`str` name of this column.
    name: str = attr.ib()

    #: The optional :class:`int` table OID of this column.
    table_oid: int | None = attr.ib()

    #: The optional :class:`int` column index of this column.
    column_index: int | None = attr.ib()

    #: The :class`int` type OID of this column.
    type_oid: int = attr.ib()

    #: The :class`int` internal column length.
    column_length: int = attr.ib()

    #: The :class:`int` type modifier for this column.
    type_modifier: int = attr.ib()


@attr.s(slots=True)
class QueryResultMessage(PostgresMessage, abc.ABC):
    """
    Superclass for query results.
    """


@attr.s(slots=True)
class RowDescription(QueryResultMessage):
    """
    Describes the rows of a query.
    """

    #: The list of :class:`.ColumnDescription` instances that wraps the decoding info for each
    #: column returned in this row.
    columns: list[ColumnDescription] = attr.ib()


@attr.s(slots=True)
class DataRow(QueryResultMessage):
    """
    A singular data row. This contains a :class:`.RowDescription` and a list of converted data
    values.
    """

    #: The :class:`.RowDescription` that describes the data within this row.
    description: RowDescription = attr.ib()

    #: A list of column values, in the same order as the description, that contains the actual
    #: converted data incoming from the server.
    data: list[Any | None] = attr.ib()

    def __getitem__(self, item: int) -> Any | None:  # pragma: no cover
        return self.data[item]

    def to_dict(self) -> dict[str, Any | None]:
        """
        Converts this data row to a dict. If multiple columns have the same name, this may not
        end up the way you expect.
        """
        d = {}
        for col, data in zip(self.description.columns, self.data, strict=True):
            d[col.name] = data

        return d


@attr.s(slots=True)
class CommandComplete(QueryResultMessage):
    """
    Returned when a single query command is complete.
    """

    #: The :class:`str` command tag. Probably useless.
    tag: str = attr.ib()

    #: The :class:`int` row count returned. This may be None if the command does not have a row
    #: count (e.g. SHOW or SET).
    row_count: int | None = attr.ib()


@attr.s(slots=True)
class ParseComplete(PostgresMessage):
    """
    Returned when parsing a prepared statement completes.
    """

    #: The :class:`str` name of the statement prepared. None means the unnamed prepared statement.
    statement_name: str | None = attr.ib()


@attr.s(slots=True)
class ParameterDescription(PostgresMessage):
    """
    Returned when parsing a ParameterDescription message.
    """

    #: The list of :class:`int` OIDs within this description.
    oids: list[int] = attr.ib()


@attr.s(slots=True)
class PreparedStatementInfo(PostgresMessage):
    """
    Contains the state of a prepared statement. Returned for a RowDescription over a prepared
    statement.
    """

    #: The :class:`str` name of the prepared statement.
    name: str | None = attr.ib()

    #: The :class:`~.ParameterDescription` for the parameters for this prepared statement.
    parameter_oids: ParameterDescription = attr.ib()

    #: The :class:`~.RowDescription` of the incoming row data of the prepared statement.
    #: This may be None if this query doesn't return any data.
    row_description: RowDescription | None = attr.ib()


@attr.s(slots=True)
class BindComplete(PostgresMessage):
    """
    Returned when a Bind message completes successfully.
    """


@attr.s(slots=True)
class PortalSuspended(PostgresMessage):
    """
    Returned when the execution portal is suspended.
    """


def _optional_int(value: str | None) -> int | None:
    if value is None:
        return None

    return int(value)


@attr.s(slots=True, frozen=False)
class ErrorOrNoticeResponse(PostgresMessage):
    """
    Returned when an error or a notice message is produced from the server.
    """

    #: If this error is a notice, rather than a real error.
    notice: bool = attr.ib()

    #: If this error is recoverable or not.
    recoverable: bool = attr.ib()

    severity_localised: str = attr.ib()
    severity: str = attr.ib()
    code: str = attr.ib()
    message: str = attr.ib()

    # optional parameters
    hint: str = attr.ib(default=None)
    detail: str | None = attr.ib(default=None)
    position: int | None = attr.ib(default=None, converter=_optional_int)
    internal_position: int | None = attr.ib(default=None, converter=_optional_int)
    internal_query: str | None = attr.ib(default=None)
    where: str | None = attr.ib(default=None)
    schema_name: str | None = attr.ib(default=None)
    table_name: str | None = attr.ib(default=None)
    column_name: str | None = attr.ib(default=None)
    data_type_name: str | None = attr.ib(default=None)
    constraint_name: str | None = attr.ib(default=None)


class BaseDatabaseError(PostgresqlError):
    """
    An exception produceed from the database, usually from an ErrorOrNoticeResponse message. This
    does NOT include things such as protocol parsing errors.
    """

    def __init__(self, response: ErrorOrNoticeResponse, query: str | None = None):
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


class RecoverableDatabaseError(BaseDatabaseError):
    """
    A subclass of :class:`.BaseDatabaseError` that the client may potentially recover from. Examples
    include query errors.
    """


class UnrecoverableDatabaseError(BaseDatabaseError):
    """
    A subclass of :class:`.BaseDatabaseError` that the client must not recover from. This usually
    implies internal errors in the server.
    """


class InvalidPasswordError(UnrecoverableDatabaseError):
    """
    Raised when the password provided is invalid.
    """


def wrap_error(response: ErrorOrNoticeResponse, query: str | None = None) -> BaseDatabaseError:
    """
    Wraps a :class:`.ErrorOrNoticeResponse` in an exception. If a query produced the error in
    question, then passing it as the ``query`` param can produce a prettier error.
    """
    # TODO: More codes
    if response.code == "28P01":
        return InvalidPasswordError(response, query)

    if response.recoverable:
        return RecoverableDatabaseError(response, query)

    return UnrecoverableDatabaseError(response, query)


__all__ = (
    "PostgresMessage",
    "AuthenticationRequest",
    "BackendKeyData",
    "AuthenticationMethod",
    "AuthenticationCompleted",
    "ParameterStatus",
    "ReadyForQueryState",
    "ReadyForQuery",
    "QueryResultMessage",
    "RowDescription",
    "DataRow",
    "CommandComplete",
    "ParseComplete",
    "ParameterDescription",
    "PreparedStatementInfo",
    "BindComplete",
    "ErrorResponseFieldType",
    "ErrorOrNoticeResponse",
    "InvalidPasswordError",
    "BaseDatabaseError",
    "RecoverableDatabaseError",
    "UnrecoverableDatabaseError",
    "ColumnDescription",
    "PortalSuspended",
    "wrap_error",
)
