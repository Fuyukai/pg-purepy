"""
The guts of the library - the protocol state machine.
"""

from __future__ import annotations

import enum
import functools
import logging
import struct
from collections.abc import Callable, Collection, Mapping
from datetime import tzinfo
from hashlib import md5
from itertools import count as it_count
from typing import Any

import dateutil.tz
from scramp import ScramClient

from pg_purepy.conversion import apply_default_converters
from pg_purepy.conversion.abc import ConversionContext, Converter
from pg_purepy.exc import (
    IllegalStateError,
    MissingPasswordError,
    ProtocolParseError,
    UnknownMessageError,
)
from pg_purepy.messages import (
    AuthenticationCompleted,
    AuthenticationMethod,
    AuthenticationRequest,
    BackendKeyData,
    BindComplete,
    ColumnDescription,
    CommandComplete,
    DataRow,
    ErrorOrNoticeResponse,
    ErrorResponseFieldType,
    ParameterDescription,
    ParameterStatus,
    ParseComplete,
    PortalSuspended,
    PostgresMessage,
    PreparedStatementInfo,
    ReadyForQuery,
    ReadyForQueryState,
    RowDescription,
    SASLComplete,
    SASLContinue,
)
from pg_purepy.util import Buffer, pack_strings

# static messages with no params
FLUSH_MESSAGE = b"H\x00\x00\x00\x04"
SYNC_MESSAGE = b"S\x00\x00\x00\x04"
TERMINATE_MESSAGE = b"X\x00\x00\x00\x04"
COPY_DONE_MESSAGE = b"c\x00\x00\x00\x04"
SSL_MESSAGE = b"\x00\x00\x00\x08\x04\xd2\x16/"


class BackendMessageCode(enum.IntEnum):
    """
    Enumeration of possible backend message codes.
    """

    NOTICE_RESPONSE = ord("N")
    AUTHENTICATION_REQUEST = ord("R")
    PARAMETER_STATUS = ord("S")
    BACKEND_KEY_DATA = ord("K")
    READY_FOR_QUERY = ord("Z")
    ROW_DESCRIPTION = ord("T")
    ERROR_RESPONSE = ord("E")
    DATA_ROW = ord("D")
    COMMAND_COMPLETE = ord("C")
    PARSE_COMPLETE = ord("1")
    BIND_COMPLETE = ord("2")
    CLOSE_COMPLETE = ord("3")
    PORTAL_SUSPENDED = ord("s")
    NO_DATA = ord("n")
    PARAMETER_DESCRIPTION = ord("t")
    NOTIFICATION_RESPONSE = ord("A")
    COPY_DONE = ord("c")
    COPY_DATA = ord("d")
    COPY_IN_RESPONSE = ord("G")
    COPY_OUT_RESPONSE = ord("H")
    EMPTY_QUERY_RESPONSE = ord("I")
    NEGOTIATE_PROTOCOL_VERSION = ord("v")


class FrontendMessageCode(enum.IntEnum):
    """
    Enumeration of possible frontend message codes.
    """

    BIND = ord("B")
    PARSE = ord("P")
    QUERY = ord("Q")
    EXECUTE = ord("E")
    FLUSH = ord("H")
    SYNC = ord("S")
    PASSWORD = ord("p")
    DESCRIBE = ord("D")
    TERMINATE = ord("X")
    CLOSE = ord("C")


class NeedData:
    """
    Special sentinel object returned to signify the state machine needs more data,
    """

    pass


#: Special singleton sentinel returned to signify the state machine needs more data.
NEED_DATA = NeedData()

#: Special singleton sentinel used to specify a state in the state machine cannot process any
#: events.
_NO_HANDLE = object()


def unrecoverable_error(
    fn: Callable[[SansIOClient, BackendMessageCode, Buffer], PostgresMessage]
) -> Callable[[SansIOClient, BackendMessageCode, Buffer], PostgresMessage]:
    """
    Decorator that will automatically set the state to an unrecoverable error if an error response
    is found.
    """

    @functools.wraps(fn)
    def wrapper(
        self: SansIOClient, code: BackendMessageCode, body: Buffer
    ) -> ErrorOrNoticeResponse | PostgresMessage:
        if code == BackendMessageCode.ERROR_RESPONSE:
            error = self._decode_error_response(body, recoverable=False, notice=False)
            if error.code == "57014":
                self.state = ProtocolState.RECOVERABLE_ERROR
                error.recoverable = True
            else:
                self.state = ProtocolState.UNRECOVERABLE_ERROR
                self._logger.fatal(f"Unrecoverable error: {error.severity}: {error.message}")

            return error

        return fn(self, code, body)

    return wrapper


class ProtocolState(enum.Enum):
    """
    Enumeration of possible states for the protocol machine.
    """

    ## Generic states.

    #: The initial state. We are waiting to send our startup message.
    STARTUP = 0

    #: We've sent our startup message. We're waiting for the server to respond to us.
    SENT_STARTUP = 1

    #: We've authenticated, receiving an Authentication Request Success, but we're waiting for
    #: a Ready For Query message.
    AUTHENTICATED_WAITING_FOR_COMPLETION = 2

    ## SASL states.

    #: During startup, we've been told to authenticate using SASL.
    SASL_STARTUP = 10

    #: During authentication, we've sent our first SASL message, and we're waiting for a response
    #: from the server.
    SASL_FIRST_SENT = 11

    #: During authentication, we've received the server's first SASL message, and we're waiting to
    #: send the final message.
    SASL_FIRST_RECEIVED = 12

    #: During authentication, we've sent our final SASL message, and we're waiting for a response
    #: from the server.
    SASL_FINAL_SENT = 13

    ## MD5 states.

    #: During startup, we've been told to authenticate using MD5.
    MD5_STARTUP = 20

    #: During authentication, we've sent our MD5-hashed password.
    MD5_SENT = 21

    ## Cleartext states.

    #: During startup, we've been told to authenticate using a cleartext password.
    CLEARTEXT_STARTUP = 30

    #: During authentication, we've sent our cleartext password.
    CLEARTEXT_SENT = 31

    ## Simple query states.

    #: During a simple query, we've sent the query to the server.
    SIMPLE_QUERY_SENT_QUERY = 100

    #: During a simple query, we've received the row description message, and we're waiting for
    #: either data rows, or command completion.
    SIMPLE_QUERY_RECEIVED_ROW_DESCRIPTION = 101

    #: During a simple query, we've received the command completion message, and we're waiting for
    #: the Ready for Query message.
    SIMPLE_QUERY_RECEIVED_COMMAND_COMPLETE = 102

    ## Extended query states.

    #: We've sent a Parse+Describe message to the server, and are waiting for a ParseComplete
    # message.
    MULTI_QUERY_SENT_PARSE_DESCRIBE = 300

    #: We've received a ParseComplete, and are waiting for the ParameterDescription+RowDescription
    #: messages.
    MULTI_QUERY_RECEIVED_PARSE_COMPLETE = 301

    #: We've received a ParameterDescription, and are waiting for a RowDescription. This state
    #: may be skipped.
    MULTI_QUERY_RECEIVED_PARAMETER_DESCRIPTION = 302

    #: We've received our RowDescription, and are waiting for the ReadyForQuery message.
    MULTI_QUERY_DESCRIBE_SYNC = 303

    #: We've sent a Bind message, and are waiting for a BindComplete.
    MULTI_QUERY_SENT_BIND = 310

    #: We've received a BindComplete or have unsuspended the portal, and are waiting for the data
    #: row messages.
    MULTI_QUERY_READING_DATA_ROWS = 311

    #: We've received a PortalSuspended message, and need to send another Execute.
    MULTI_QUERY_RECEIVED_PORTAL_SUSPENDED = 312

    #: We've received a CommandComplete messagee, and we are waiting for the ReadyForQuery
    #: message.
    MULTI_QUERY_RECEIVED_COMMAND_COMPLETE = 313

    ## Misc states.
    #: We're ready to send queries to the server. This is the state inbetween everything happening.
    READY_FOR_QUERY = 999

    #: An unrecoverable error has happened, and the protocol will no longer work.
    UNRECOVERABLE_ERROR = 1000

    #: A recoverable error has happened, and the protocol is waiting for a ReadyForQuery message.
    RECOVERABLE_ERROR = 1001

    #: The connection has been terminated.
    TERMINATED = 9999


# noinspection PyMethodMayBeStatic,PyUnresolvedReferences
class SansIOClient:
    """
    Sans-I/O state machine for the PostgreSQL C<->S protocol. This operates as an in-memory buffer
    that takes in Python-side structures and turns them into bytes to be sent to the server, and
    receives bytes from the server and turns them into Python-side structures.
    """

    _LOGGER_COUNTER = it_count()

    PROTOCOL_MAJOR = 3
    PROTOCOL_MINOR = 0

    _COMMANDS_WITH_COUNTS: Collection[str] = {"DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY"}

    _PROTECTED_STATUSES: Mapping[str, str] = {
        "IntervalStyle": "iso_8601",
        "DateStyle": "ISO, DMY",
    }

    def __init__(
        self,
        username: str,
        database: str | None = None,
        password: str | None = None,
        application_name: str = "pg-purepy",
        logger_name: str | None = None,
        ignore_unknown_types: bool = True,
    ):
        """
        :param username: The username to authenticate as. Mandatory.
        :param database: The database to connect to. Defaults to ``username``.
        :param password: The password to authenticate with.
        :param application_name: The application name to send. Defaults to ``pg-purepy``.
        :param logger_name: The name of the logger to use. Defaults to a counter.
        :param ignore_unknown_types: If True, unknown types are returned as strings. Otherwise,
                                     raises an exception.
        """
        if not logger_name:
            logger_name = __name__ + f".protocol-{next(self._LOGGER_COUNTER)}"

        if database is None:
            database = username

        #: Mapping of miscellaneous connection parameters.
        self.connection_params: dict[str, Any] = {}

        #: If this connection is authenticated or not.
        self.is_authenticated: bool = False

        #: The converter classes used to convert between PostgreSQL and Python types.
        self.converters: dict[int, Converter] = {}

        #: Boolean value for if the current connection is in a transaction.
        self.in_transaction: bool = False

        # used for converters, and stores some parameter data so we dont have to update it in two
        # places.
        self._conversion_context = ConversionContext(client_encoding="UTF8")
        self._ignore_unknown_types = ignore_unknown_types

        # used when decoding data rows. unset by CommandComplete.
        self._last_row_description: RowDescription | None = None

        # used in two places:
        # 1) when a prepared statement is sent, we cache the name so we can return the ParseComplete
        # 2) when a describe is sent, we cache the name to be used in the resulting object
        self._last_named_query: str | None = None

        # used when a Describe message arrives.
        self._last_parameter_oids: list[int] = []

        # stored data used for generating various packets
        self.username = username
        self.database = database
        self.application_name: str = application_name
        self._password = password

        # the state of the protocol machine.
        self._state: ProtocolState = ProtocolState.STARTUP

        ## authentication
        # current authentication request. used by various auth functions
        self._auth_request: AuthenticationRequest | None = None
        # scramp client, used for SASL
        self._scramp_client: ScramClient | None = None

        ## buffering
        # infinitely growable buffer used to store incoming packets
        self._buffer = bytearray()
        # last packet code received
        self._current_packet_code = 0
        # number of bytes remaining in the current packet
        self._current_packet_remaining = 0
        # buffer used to store partial packet data
        self._current_packet_buffer = bytearray()
        # if the last receive started processing for a packet, but we didn't receive the full
        # data, we're currently processing a partial packet
        self._processing_partial_packet = False

        self._logger = logging.getLogger(logger_name)

        apply_default_converters(self)

    @property
    def state(self) -> ProtocolState:
        """
        The current state of this connection.
        """
        return self._state

    @state.setter
    def state(self, value: ProtocolState) -> None:
        before = self._state
        self._logger.trace(f"Protocol changing state from {before} to {value}")  # type: ignore
        self._state = value

    @property
    def ready(self) -> bool:
        """
        If the server is ready for another query.
        """
        return self._state == ProtocolState.READY_FOR_QUERY

    @property
    def encoding(self) -> str:
        """
        The client encoding.
        """
        return self._conversion_context.client_encoding

    @property
    def timezone(self) -> tzinfo:
        """
        The server timezone.
        """
        return self._conversion_context.timezone

    @property
    def has_pending_data(self) -> bool:
        """
        Checks if this has pending data within the buffer.
        """
        return len(self._buffer) > 0

    @property
    def dead(self) -> bool:
        """
        If the protocol is "dead", i.e. unusable via an error or terminated.
        """
        return self._state in {ProtocolState.UNRECOVERABLE_ERROR, ProtocolState.TERMINATED}

    ## Internal API ##
    def _check_password(self) -> None:
        if self._password is None:
            raise MissingPasswordError(
                "Server asked for authentication but we don't have a password"
            )

    def _handle_parameter_status(self, body: Buffer) -> ParameterStatus:
        """
        Handles a parameter status message. This will update the parameter statuses on the
        """
        encoding = self._conversion_context.client_encoding
        name, value = body.read_cstring(encoding), body.read_cstring(encoding)

        if name in self._PROTECTED_STATUSES and value != self._PROTECTED_STATUSES[name]:
            self.state = ProtocolState.UNRECOVERABLE_ERROR
            raise ProtocolParseError(f"Attempted to change protected connection parameter {name}!")

        if name == "client_encoding":
            self._conversion_context.client_encoding = value
        elif name == "TimeZone":
            gotten = dateutil.tz.gettz(value)
            if gotten is None:
                raise ValueError(f"PG returned invalid timezone {value}!")

            # coerce 'UTC' zoneinfo into tzutc
            if gotten == dateutil.tz.gettz("UTC"):
                gotten = dateutil.tz.UTC

            self._conversion_context.timezone = gotten
        else:
            self.connection_params[name] = value

        self._logger.trace(f"Parameter status: {name} -> {value}")  # type: ignore

        return ParameterStatus(name, value)

    def _decode_error_response(
        self,
        body: Buffer,
        *,
        recoverable: bool,
        notice: bool,
    ) -> ErrorOrNoticeResponse:
        """
        Decodes an error response, returning the :class:`.ErrorOrNoticeResponse` containing all the
        fields from the message.
        """
        kwargs: dict[str, Any] = {"recoverable": recoverable, "notice": notice}
        ignored_codes: set[int] = {ord("F"), ord("L"), ord("R")}

        while body:
            try:
                raw_code = body.read_byte()
                if raw_code == 0:  # final null sep
                    break

                if raw_code in ignored_codes:
                    # discard body
                    body.read_cstring(encoding="ascii")
                    continue

                code = ErrorResponseFieldType(raw_code)
            except ValueError:
                code = ErrorResponseFieldType.UNKNOWN

            str_field = body.read_cstring(encoding="ascii")
            if code == ErrorResponseFieldType.UNKNOWN:
                self._logger.warning(
                    f"Encountered unknown field type in error with value {str_field}"
                )
            else:
                kwargs[code.name.lower()] = str_field

        return ErrorOrNoticeResponse(**kwargs)

    def _decode_row_description(self, body: Buffer) -> RowDescription:
        """
        Decodes the row description message.
        """
        field_count = body.read_short()
        self._logger.trace(f"Got {field_count} fields in this row description.")  # type: ignore
        fields = []

        for _i in range(0, field_count):
            name = body.read_cstring(encoding=self.encoding)
            table_oid = body.read_int()
            column_idx = body.read_short()
            type_oid = body.read_int()

            if type_oid not in self.converters:
                if self._ignore_unknown_types:
                    self._logger.warning(f"Unknown type OID: {type_oid}")
                else:
                    raise ProtocolParseError(f"Unknown type OID: {type_oid}")

            type_size = body.read_short()
            type_mod = body.read_int()
            format_code = body.read_short()  # noqa: F841, good for code explicitness

            desc = ColumnDescription(
                name=name,
                table_oid=table_oid,
                column_index=column_idx,
                type_oid=type_oid,
                column_length=type_size,
                type_modifier=type_mod,
            )
            fields.append(desc)

        return RowDescription(fields)

    def _decode_data_row(self, body: Buffer) -> DataRow:
        """
        Decodes a single data row packet.
        """
        assert self._last_row_description is not None, "missing row description"

        column_values: list[Any | None] = []
        count = body.read_short()

        for idx in range(0, count):
            size = body.read_int()

            # -1 size means null.
            if size < 0:
                column_values.append(None)
                continue

            data = body.read_bytes(size)

            col_desc = self._last_row_description.columns[idx]
            col_oid = col_desc.type_oid
            converter = self.converters.get(col_oid)
            if converter is None:
                column_values.append(data.decode(self.encoding))
            else:
                column_values.append(
                    converter.from_postgres(self._conversion_context, data.decode(self.encoding))
                )

        return DataRow(self._last_row_description, column_values)

    def _decode_command_complete(self, body: Buffer) -> CommandComplete:
        """
        Decodes a "command complete" message.
        """
        tag = body.read_cstring(encoding=self.encoding)
        split = tag.split(" ")

        if split[0] == "INSERT":
            row_count = int(split[2])
        elif split[0] in self._COMMANDS_WITH_COUNTS:
            row_count = int(split[1])
        else:
            row_count = None

        return CommandComplete(split[0], row_count)

    _handle_during_STARTUP = "You need to call do_startup()!"

    ### Authentication ###
    @unrecoverable_error
    def _handle_during_SENT_STARTUP(
        self, code: BackendMessageCode, body: Buffer
    ) -> AuthenticationCompleted | AuthenticationRequest:
        if code == BackendMessageCode.NEGOTIATE_PROTOCOL_VERSION:
            raise ProtocolParseError("Server does not support our protocol")

        if code == BackendMessageCode.AUTHENTICATION_REQUEST:
            # various different options
            decoded = body.read_int()

            # AuthenticationOk. We skip authentication entirely.
            if decoded == 0:
                self.is_authenticated = True
                self.state = ProtocolState.AUTHENTICATED_WAITING_FOR_COMPLETION
                return AuthenticationCompleted()

            # AuthenticationCleartextPassword
            if decoded == AuthenticationMethod.CLEARTEXT:
                self._check_password()

                self.state = ProtocolState.CLEARTEXT_STARTUP
                self._auth_request = AuthenticationRequest(method=AuthenticationMethod.CLEARTEXT)
                return self._auth_request

            # AuthenticationMD5
            if decoded == AuthenticationMethod.MD5:
                self._check_password()

                self.state = ProtocolState.MD5_STARTUP
                md5_bytes = body.read_bytes(4)
                self._auth_request = AuthenticationRequest(
                    method=AuthenticationMethod.MD5, md5_salt=md5_bytes
                )
                return self._auth_request

            # AuthenticationSASL
            if decoded == AuthenticationMethod.SASL:
                self._check_password()

                self.state = ProtocolState.SASL_STARTUP
                methods = body.read_all_cstrings(encoding=self.encoding, drop_empty=True)
                self._auth_request = AuthenticationRequest(
                    method=AuthenticationMethod.SASL,
                    sasl_methods=methods,
                )
                return self._auth_request

            raise NotImplementedError(f"Unknown authentication method: {decoded}")

        raise UnknownMessageError(f"Don't know how to handle {code} in {self.state}")

    def _handle_during_SASL_FIRST_SENT(
        self, code: BackendMessageCode, body: Buffer
    ) -> SASLContinue:
        if code == BackendMessageCode.AUTHENTICATION_REQUEST:
            body_code = body.read_int()
            if body_code == 11:  # SASL Continue
                sasl_body = body.read_remaining().decode("ascii")

                assert self._scramp_client
                self._scramp_client.set_server_first(sasl_body)
                self.state = ProtocolState.SASL_FIRST_RECEIVED
                return SASLContinue()

            raise ProtocolParseError(
                f"Unexpected response: Expected SASLContinue, but got {body_code}"
            )

        raise UnknownMessageError(f"Unknown message code {code} for state {self.state}")

    @unrecoverable_error
    def _generic_auth_handle(
        self, code: BackendMessageCode, body: Buffer
    ) -> AuthenticationCompleted | SASLComplete:
        """
        Authentication handler shared between various authentication "finalisation" methods.
        """
        if code == BackendMessageCode.AUTHENTICATION_REQUEST:
            body_code = body.read_int()
            if body_code == 0:  # AuthenticationOk
                self.is_authenticated = True
                self.state = ProtocolState.AUTHENTICATED_WAITING_FOR_COMPLETION
                return AuthenticationCompleted()

            if body_code == 12:  # SASLComplete
                return SASLComplete()

            raise ProtocolParseError(
                f"Unexpected response: Expected AuthenticationOk, but got {body_code}"
            )

        raise UnknownMessageError(f"Unknown message code {code} for state {self.state}")

    _handle_during_CLEARTEXT_SENT = _generic_auth_handle
    _handle_during_MD5_SENT = _generic_auth_handle
    _handle_during_SASL_FINAL_SENT = _generic_auth_handle

    def _got_ready_for_query(self, body: Buffer) -> ReadyForQuery:
        """
        Helper function used when a ReadyForQuery is received.
        """
        self.state = ProtocolState.READY_FOR_QUERY
        rfqs = ReadyForQueryState(body.read_byte())
        self.in_transaction = rfqs != ReadyForQueryState.IDLE

        self._last_row_description = None  # common reset

        return ReadyForQuery(rfqs)

    @unrecoverable_error
    def _handle_ready_for_query(self, code: BackendMessageCode, body: Buffer) -> ReadyForQuery:
        """
        Generic handler for when the server is ready for query.
        """
        if code == BackendMessageCode.READY_FOR_QUERY:
            return self._got_ready_for_query(body)

        raise UnknownMessageError(f"Expected ReadyForQuery, got {code!r}")

    @unrecoverable_error
    def _handle_during_AUTHENTICATED_WAITING_FOR_COMPLETION(
        self,
        code: BackendMessageCode,
        body: Buffer,
    ) -> ReadyForQuery | BackendKeyData:
        """
        Handles incoming messages once we've authenticated.
        """
        if code == BackendMessageCode.READY_FOR_QUERY:
            return self._got_ready_for_query(body)

        if code == BackendMessageCode.BACKEND_KEY_DATA:
            pid = body.read_int()
            key = body.read_uint()
            return BackendKeyData(pid, key)

        raise UnknownMessageError(f"Expected ReadyForQuery, got {code!r}")

    ### Simple Query ###
    def _got_row_description(self, body: Buffer) -> RowDescription:
        """
        Common logic for when we get a row description.
        """
        row_desc = self._decode_row_description(body)
        self.state = ProtocolState.SIMPLE_QUERY_RECEIVED_ROW_DESCRIPTION
        self._logger.trace(f"Incoming data has {len(row_desc.columns)} columns.")  # type: ignore
        self._last_row_description = row_desc
        return row_desc

    def _handle_during_SIMPLE_QUERY_SENT_QUERY(
        self, code: BackendMessageCode, body: Buffer
    ) -> CommandComplete | RowDescription | ErrorOrNoticeResponse:
        """
        Handles incoming messages once we've sent a query execution message.
        """
        if code == BackendMessageCode.COMMAND_COMPLETE:
            # commands such as Begin or Rollback don't return rows, thus don't return a
            # RowDescription.
            self.state = ProtocolState.SIMPLE_QUERY_RECEIVED_COMMAND_COMPLETE
            self._logger.trace("Query returned no rows")  # type: ignore
            return self._decode_command_complete(body)

        if code == BackendMessageCode.ROW_DESCRIPTION:
            return self._got_row_description(body)

        if code == BackendMessageCode.ERROR_RESPONSE:
            # this is a recoverable error, and simply moves onto the ReadyForQuery message.
            self.state = ProtocolState.RECOVERABLE_ERROR
            error = self._decode_error_response(body, recoverable=True, notice=False)
            self._logger.warning(f"Error during query: {error.severity}: {error.message}")
            return error

        raise UnknownMessageError(f"Expected RowDescription, got {code!r}")

    @unrecoverable_error
    def _handle_during_SIMPLE_QUERY_RECEIVED_ROW_DESCRIPTION(
        self, code: BackendMessageCode, body: Buffer
    ) -> DataRow | CommandComplete:
        """
        Handles incoming data rows during a simple query.
        """
        if code == BackendMessageCode.DATA_ROW:
            return self._decode_data_row(body)

        if code == BackendMessageCode.COMMAND_COMPLETE:
            self.state = ProtocolState.SIMPLE_QUERY_RECEIVED_COMMAND_COMPLETE
            self._last_row_description = None
            return self._decode_command_complete(body)

        raise UnknownMessageError(f"Expected DataRow or CommandComplete, got {code!r}")

    @unrecoverable_error
    def _handle_during_SIMPLE_QUERY_RECEIVED_COMMAND_COMPLETE(
        self, code: BackendMessageCode, body: Buffer
    ) -> RowDescription | ReadyForQuery:
        """
        Waits for the ReadyForQuery response, or the next row description if this was a multi-query.
        """
        if code == BackendMessageCode.ROW_DESCRIPTION:
            # multiple queries in one statement
            return self._got_row_description(body)

        if code == BackendMessageCode.READY_FOR_QUERY:
            return self._got_ready_for_query(body)

        raise UnknownMessageError(f"Unexpected message {code!r}")

    _handle_during_SIMPLE_QUERY_ERRORED: Callable[
        [SansIOClient, BackendMessageCode, Buffer], PostgresMessage
    ] = _handle_ready_for_query

    ### Multi query, Prepared Statements ###
    def _handle_during_MULTI_QUERY_SENT_PARSE_DESCRIBE(
        self, code: BackendMessageCode, body: Buffer
    ) -> ParseComplete | ErrorOrNoticeResponse:
        """
        Waits for the ParseComplete response.
        """
        if code == BackendMessageCode.PARSE_COMPLETE:
            self.state = ProtocolState.MULTI_QUERY_RECEIVED_PARSE_COMPLETE
            return ParseComplete(self._last_named_query)

        if code == BackendMessageCode.ERROR_RESPONSE:
            # bad query
            self.state = ProtocolState.RECOVERABLE_ERROR
            return self._decode_error_response(body, recoverable=True, notice=False)

        raise UnknownMessageError(f"Expected ParseComplete, got {code!r}")

    def _multi_query_got_description(self, body: Buffer) -> PreparedStatementInfo:
        decoded = self._decode_row_description(body)
        lnq = self._last_named_query if self._last_named_query else None
        self._last_named_query = None
        message = PreparedStatementInfo(
            lnq, ParameterDescription(self._last_parameter_oids), decoded
        )
        self._last_parameter_oids = []

        self.state = ProtocolState.MULTI_QUERY_DESCRIBE_SYNC

        return message

    def _multi_query_got_no_data(self) -> PreparedStatementInfo:
        lnq = self._last_named_query if self._last_named_query else None
        self._last_named_query = None
        message = PreparedStatementInfo(lnq, ParameterDescription(self._last_parameter_oids), None)

        self.state = ProtocolState.MULTI_QUERY_DESCRIBE_SYNC
        return message

    @unrecoverable_error
    def _handle_during_MULTI_QUERY_RECEIVED_PARSE_COMPLETE(
        self, code: BackendMessageCode, body: Buffer
    ) -> ParameterDescription | PreparedStatementInfo:
        """
        Decodes a possible ParameterDescription message, or a RowDescription message.
        """
        if code == BackendMessageCode.PARAMETER_DESCRIPTION:
            count = body.read_short()
            oids = [body.read_int() for _ in range(0, count)]
            self._last_parameter_oids = oids
            self.state = ProtocolState.MULTI_QUERY_RECEIVED_PARAMETER_DESCRIPTION
            return ParameterDescription(oids)

        if code == BackendMessageCode.ROW_DESCRIPTION:
            return self._multi_query_got_description(body)

        raise UnknownMessageError(f"Expected ParameterDescription or RowDescription, got {code!r}")

    @unrecoverable_error
    def _handle_during_MULTI_QUERY_RECEIVED_PARAMETER_DESCRIPTION(
        self, code: BackendMessageCode, body: Buffer
    ) -> PreparedStatementInfo:
        """
        Decodes the incoming row description message.
        """
        # Both of these change state to MULTI_QUERY_DESCRIBE_SYNC
        if code == BackendMessageCode.ROW_DESCRIPTION:
            # Changes state to MULTI_QUERY_DESCRIBE_SYNC
            return self._multi_query_got_description(body)

        if code == BackendMessageCode.NO_DATA:
            return self._multi_query_got_no_data()

        raise UnknownMessageError(f"Expected RowDescription or NoData, got {code!r}")

    _handle_during_MULTI_QUERY_DESCRIBE_SYNC = _handle_ready_for_query

    ### Multi query, incoming data ###
    def _handle_during_MULTI_QUERY_SENT_BIND(
        self, code: BackendMessageCode, body: Buffer
    ) -> BindComplete | ErrorOrNoticeResponse:
        """
        Waits for the BindComplete message.
        """
        if code == BackendMessageCode.BIND_COMPLETE:
            self.state = ProtocolState.MULTI_QUERY_READING_DATA_ROWS
            return BindComplete()

        if code == BackendMessageCode.ERROR_RESPONSE:
            self.state = ProtocolState.RECOVERABLE_ERROR
            return self._decode_error_response(body, recoverable=True, notice=False)

        raise UnknownMessageError(f"Expected BindComplete, got {code!r}")

    def _handle_during_MULTI_QUERY_READING_DATA_ROWS(
        self, code: BackendMessageCode, body: Buffer
    ) -> DataRow | CommandComplete | ErrorOrNoticeResponse | PortalSuspended:
        """
        Reads in data rows, and waits for CommandComplete and ReadyForQuery.
        """
        if code == BackendMessageCode.DATA_ROW:
            return self._decode_data_row(body)

        if code == BackendMessageCode.COMMAND_COMPLETE:
            self.state = ProtocolState.MULTI_QUERY_RECEIVED_COMMAND_COMPLETE
            return self._decode_command_complete(body)

        if code == BackendMessageCode.ERROR_RESPONSE:
            # TODO: Maybe check the code, and reraise as unrecoverable?
            self.state = ProtocolState.RECOVERABLE_ERROR
            return self._decode_error_response(body, recoverable=True, notice=False)

        if code == BackendMessageCode.PORTAL_SUSPENDED:
            self.state = ProtocolState.MULTI_QUERY_RECEIVED_PORTAL_SUSPENDED
            return PortalSuspended()

        raise UnknownMessageError(f"Expected DataRow or CommandComplete, got {code!r}")

    # The docs seem to lie here, the server just sends us the PortalSuspended then
    # ReadyForQuery. I don't get it.
    # pg8000 seems to just drop it on the floor too... so that's what we do.
    _handle_during_MULTI_QUERY_RECEIVED_PORTAL_SUSPENDED = _handle_ready_for_query
    _handle_during_MULTI_QUERY_RECEIVED_COMMAND_COMPLETE = _handle_ready_for_query
    _handle_during_RECOVERABLE_ERROR = _handle_ready_for_query

    ## Public API ##
    def add_converter(self, converter: Converter) -> None:
        """
        Adds a new :class:`.Converter` to this protocol.
        """
        self.converters[converter.oid] = converter

    def receive_bytes(self, data: bytes) -> None:
        """
        Receives incoming bytes from the server. This merely appends to an internal buffer; you
        need to call :meth:`.next_event` to do anything.

        :param data: The incoming data from PostgreSQL.
        """
        # these are assertions so that ``python -O`` optimises them out. its a bug to do anything
        # once the protocol is in UNRECOVERABLE_ERROR anyway.
        assert self._state != ProtocolState.UNRECOVERABLE_ERROR, "state is unrecoverable error"
        assert self._state != ProtocolState.TERMINATED, "state is terminated"

        self._logger.trace(f"Protocol: Received {len(data)} bytes")  # type: ignore
        self._buffer += data

    def do_startup(self) -> bytes:
        """
        Gets the startup packet, and sets the state machine up for starting up.

        :return: A startup message, that should be sent to PostgreSQL.
        """
        if self._state != ProtocolState.STARTUP:
            raise IllegalStateError("Can't do startup outside of the startup state")

        packet_body = struct.pack("!i", (self.PROTOCOL_MAJOR << 16) | self.PROTOCOL_MINOR)
        packet_body += pack_strings("user", self.username)
        packet_body += pack_strings("database", self.database)
        packet_body += pack_strings("application_name", self.application_name)
        packet_body += pack_strings("client_encoding", self._conversion_context.client_encoding)
        packet_body += pack_strings("IntervalStyle", "iso_8601")
        packet_body += pack_strings("DateStyle", "ISO, DMY")
        packet_body += b"\x00"
        size = struct.pack("!i", (len(packet_body) + 4))

        self.state = ProtocolState.SENT_STARTUP
        return size + packet_body

    def do_terminate(self) -> bytes:
        """
        Gets the Terminate message. This closes the protocol!
        """
        self.state = ProtocolState.TERMINATED
        return TERMINATE_MESSAGE

    def do_simple_query(self, query_text: str) -> bytes:
        """
        Performs a simple (static) query. This query cannot have any dynamic parameters.
        """
        if self._state != ProtocolState.READY_FOR_QUERY:
            raise IllegalStateError("The server is not ready for queries")

        packet_body = pack_strings(query_text, encoding=self.encoding)
        header = FrontendMessageCode.QUERY.to_bytes(length=1, byteorder="big")
        header += (len(packet_body) + 4).to_bytes(length=4, byteorder="big")

        self.state = ProtocolState.SIMPLE_QUERY_SENT_QUERY
        return header + packet_body

    def do_create_prepared_statement(self, name: str | None, query_text: str) -> bytes:
        """
        Creates a prepared statement. If ``name`` is not specified, this will create the unnamed
        prepared statement.
        """
        # TIL: You can do Parse, then Describe, then Flush, and get all the messages back at once.
        # This saves a Flush and a Sync round trip.

        if self._state != ProtocolState.READY_FOR_QUERY:
            raise IllegalStateError("The server is not ready for queries")

        ## Parse
        if name is None:
            packet_body_1 = b"\x00"  # empty string
            self._last_named_query = None
        else:
            packet_body_1 = pack_strings(name, encoding="ascii")
            self._last_named_query = name

        packet_body_1 += pack_strings(query_text, encoding=self.encoding)
        # no OIDs. (yet). probably in the future...
        packet_body_1 += b"\x00\x00"

        header_1 = FrontendMessageCode.PARSE.to_bytes(length=1, byteorder="big")
        header_1 += (len(packet_body_1) + 4).to_bytes(length=4, byteorder="big")
        full_packet = header_1 + packet_body_1

        ## Describe
        packet_body_2 = b"S"

        if name:
            packet_body_2 += pack_strings(name, encoding="ascii")
        else:
            packet_body_2 += b"\x00"

        header_2 = FrontendMessageCode.DESCRIBE.to_bytes(length=1, byteorder="big")
        header_2 += (len(packet_body_2) + 4).to_bytes(length=4, byteorder="big")
        full_packet += header_2 + packet_body_2
        full_packet += FLUSH_MESSAGE
        full_packet += SYNC_MESSAGE

        self.state = ProtocolState.MULTI_QUERY_SENT_PARSE_DESCRIBE
        return full_packet

    def do_bind_execute(
        self, info: PreparedStatementInfo, params: Any, row_count: int | None = None
    ) -> bytes:
        """
        Binds the specified ``params`` to the prepared statement specified by ``info``,
        then executes it.

        If ``row_count`` is not None, then only that many rows will be returned at maximum.
        Otherwise, an unlimited amount of rows will be returned.

        :returns: The combined bind-execute-flush-sync packet to send to the server.
        """
        wanted_params = len(info.parameter_oids.oids)
        if len(params) != wanted_params:
            raise ValueError(f"Expected exactly {wanted_params} parameters, got {len(params)}")

        ## Packet: Bind

        ### Bind: Portal Name
        # Unsupported. Empty.
        packet_body_1: bytes = pack_strings("")

        ### Bind: Source Prepared Statement
        if info.name is None:
            packet_body_1 += b"\x00"
        else:
            packet_body_1 += pack_strings(info.name, encoding="ascii")

        ### Bind: Number of parameter format codes.
        packet_body_1 += struct.pack(">h", wanted_params)
        ### Bind: Parameter format codes.
        for _ in range(0, wanted_params):
            packet_body_1 += struct.pack(">h", 0)

        ### Bind: Number of parameter values.
        packet_body_1 += struct.pack(">h", wanted_params)
        ### Bind: Parameter values.
        for oid, obb in zip(info.parameter_oids.oids, params, strict=True):
            if obb is None:
                # Nulls are special cased with a -1 length.
                packet_body_1 += struct.pack(">i", -1)
                continue

            try:
                converter = self.converters[oid]
            except KeyError:
                raise ValueError(f"Missing converter for {oid}, can't convert {obb}") from None

            converted = converter.to_postgres(self._conversion_context, obb)
            assert isinstance(converted, str), f"Converter returned {type(obb)}, not string!"

            encoded = converted.encode(self.encoding)
            packet_body_1 += struct.pack(">i", len(encoded))
            packet_body_1 += encoded

        ### Bind: Result columns formats.
        wanted_results = len(info.row_description.columns) if info.row_description else 0

        packet_body_1 += struct.pack(">h", wanted_results)
        for _ in range(0, wanted_results):
            packet_body_1 += struct.pack(">h", 0)

        full_packet = FrontendMessageCode.BIND.to_bytes(length=1, byteorder="big")
        full_packet += (len(packet_body_1) + 4).to_bytes(length=4, byteorder="big")
        full_packet += packet_body_1

        ## Execute packet
        ### Execute: Unnamed portal.
        packet_body_2 = pack_strings()
        row_count = row_count or 0

        ### Execute: Row count.
        packet_body_2 += struct.pack(">i", row_count)

        full_packet += FrontendMessageCode.EXECUTE.to_bytes(length=1, byteorder="big")
        full_packet += (len(packet_body_2) + 4).to_bytes(length=4, byteorder="big")
        full_packet += packet_body_2

        ## These messages include a Flush + Sync.
        full_packet += FLUSH_MESSAGE
        full_packet += SYNC_MESSAGE

        self.state = ProtocolState.MULTI_QUERY_SENT_BIND
        self._last_row_description = info.row_description
        return full_packet

    def next_event(self) -> PostgresMessage | NeedData:
        """
        Reads the next event from the message queue.

        :return: Either a :class:`.PostgresMessage`, or the special ``NEED_DATA`` constant.
        """
        if (
            self.state == ProtocolState.UNRECOVERABLE_ERROR
            or self.state == ProtocolState.TERMINATED
        ):
            raise IllegalStateError("The protocol is broken and won't work.")

        self._logger.trace(f"Called next_event(), state is {self.state.name}")  # type: ignore

        if len(self._buffer) == 0:
            return NEED_DATA

        # This hurts my brain

        if self._processing_partial_packet:
            # rough flow:
            # 1) copy off the main buffer into the packet buffer
            # 2) if we have enough data, unflip the flag, reset buffers
            # 3) if we don't hjave enough data, just return NEED_DATA, we'll check again next loop

            remaining = self._current_packet_remaining
            assert remaining > 0, "partial packet needs actual data to read"
            data, self._buffer = self._buffer[0:remaining], self._buffer[remaining:]
            self._current_packet_buffer += data

            self._current_packet_remaining -= len(data)
            if self._current_packet_remaining > 0:
                return NEED_DATA

            self._processing_partial_packet = False
            code = self._current_packet_code
            message_data = self._current_packet_buffer
            self._current_packet_buffer = bytearray()
        else:
            # no code + size, don't care
            if len(self._buffer) < 5:
                return NEED_DATA

            code = self._buffer[0]
            size = int.from_bytes(self._buffer[1:5], byteorder="big") - 4
            message_data = self._buffer[5 : size + 5]

            if len(message_data) < size:
                self._logger.trace(  # type: ignore
                    f"Received truncated packet of {len(message_data)} size, need {size} bytes"
                )
                # not enough data yet.
                self._processing_partial_packet = True
                self._current_packet_buffer = message_data
                self._current_packet_code = code
                self._current_packet_remaining = size - len(message_data)
                # reset buffer, otherwise we try and read the message code off again >.>
                self._buffer = bytearray()
                return NEED_DATA
            else:  # noqa: RET505
                # yes enough data, set the buffer to the data after the size bytes for future
                # processing
                self._buffer = self._buffer[size + 5:]

        code = BackendMessageCode(code)
        self._logger.trace(f"Got incoming message {code!r}")  # type: ignore

        try:
            method = getattr(self, f"_handle_during_{self.state.name}")
        except AttributeError:
            raise AttributeError(f"No method exists to process state {self.state.name}") from None

        if isinstance(method, str):
            raise IllegalStateError(method)

        buffer = Buffer(message_data)

        # These messages can happen at any state.
        # As such, we yield them out unconditionally. Typically, a client ignores them and carries
        # on reading messages off.

        if code == BackendMessageCode.PARAMETER_STATUS:
            return self._handle_parameter_status(buffer)

        if code == BackendMessageCode.NOTICE_RESPONSE:
            return self._decode_error_response(body=buffer, recoverable=True, notice=True)

        # if we made it here, we can actually process the packet since we have it in full

        result: PostgresMessage | ErrorOrNoticeResponse = method(code, buffer)
        if result is None:
            raise Exception(f"{method.__qualname__} returned None! This is a bug!")

        return result

    def get_needed_synchronisation(self) -> bytes:
        """
        Gets the bytes to send to the server if we need to do synchronisation. This is used e.g.
        during a complex query message where a Sync packet is needed, or authentication where
        multiple messages may need to be sent.

        This may change the state of the protocol machine.
        """
        packet_body = b""

        # Blank states are marked eexplicitly for the sake of ease of reading.

        ## Authentication States ##
        if self.state == ProtocolState.CLEARTEXT_STARTUP:
            self._check_password()
            self._logger.trace("Received cleartext password authentication request.")  # type: ignore

            code = FrontendMessageCode.PASSWORD

            # type ignore is fine, ``_check_password`` verifies itt.
            packet_body += self._password.encode(encoding="ascii")  # type: ignore
            self.state = ProtocolState.CLEARTEXT_SENT

        elif self.state == ProtocolState.MD5_STARTUP:
            assert self._auth_request, "state is MD5_STARTUP but no auth request packet?"
            salt = self._auth_request.md5_salt
            assert salt, "state is MD5_STARTUP but salt is None?"
            self._check_password()

            assert self._password

            code = FrontendMessageCode.PASSWORD
            hashed_password = md5(self._password.encode("ascii") + self.username.encode("ascii"))
            inner_password = hashed_password.hexdigest().encode("ascii")
            encoded_password = md5(inner_password + salt).hexdigest().encode("ascii")
            packet_body += b"md5"
            packet_body += encoded_password
            packet_body += b"\x00"

            self.state = ProtocolState.MD5_SENT

        elif self.state == ProtocolState.SASL_STARTUP:
            code = FrontendMessageCode.PASSWORD
            assert self._auth_request, "state is SASL_STARTUP but no auth request packet?"

            self._scramp_client = ScramClient(
                mechanisms=self._auth_request.sasl_methods,
                username=self.username,
                password=self._password,
            )

            message = self._scramp_client.get_client_first()
            packet_body += pack_strings(self._scramp_client.mechanism_name)
            packet_body += len(message).to_bytes(length=4, byteorder="big", signed=False)
            packet_body += message.encode("ascii")

            self.state = ProtocolState.SASL_FIRST_SENT

        elif self.state == ProtocolState.SASL_FIRST_RECEIVED:
            assert self._scramp_client, "state is SASL_FIRST_RECEIVED but scramp client is null?"

            code = FrontendMessageCode.PASSWORD
            message = self._scramp_client.get_client_final()
            packet_body += message.encode("ascii")
            self.state = ProtocolState.SASL_FINAL_SENT

        else:
            return b""

        msg_len = (len(packet_body) + 4).to_bytes(length=4, byteorder="big", signed=False)
        return code.to_bytes(length=1, byteorder="big") + msg_len + packet_body


# TLS helpers
def check_if_tls_accepted(reply: bytes) -> bool:
    """
    Checks if the server accepted the TLS request.
    """
    return reply == b"S"
