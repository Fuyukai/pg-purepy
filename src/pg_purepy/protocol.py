"""
The guts of the library - the protocol state machine.
"""
from __future__ import annotations

import enum
import functools
import logging
import struct
from collections import deque
from hashlib import md5
from itertools import count
from typing import Union, Optional, Callable, Dict

import attr
from pg_purepy.converters import (
    Converter,
    DEFAULT_CONVERTERS,
    ConversionContext,
    DEFAULT_TEXT_CONVERTER,
)
from pg_purepy.exc import (
    MissingPasswordError,
    ProtocolParseError,
    UnknownMessageError,
    IllegalStateError,
)
from pg_purepy.messages import (
    AuthenticationRequest,
    ParameterStatus,
    ErrorResponseFieldType,
    ErrorResponse,
    AuthenticationCompleted,
    AuthenticationMethod,
    ReadyForQueryState,
    ReadyForQuery,
    PostgresMessage,
    CommandComplete,
    RowDescription,
    ColumnDescription,
    DataRow,
    BackendKeyData,
)
from pg_purepy.util import unpack_strings, pack_strings, Buffer
from scramp import ScramClient

# static messages with no params
FLUSH_MESSAGE = b"H\x00\x00\x00\x04"
SYNC_MESSAGE = b"S\x00\x00\x00\x04"
TERMINATE_MESSAGE = b"X\x00\x00\x00\x04"
COPY_DONE_MESSAGE = b"c\x00\x00\x00\x04"
EXECUTE_MESSAGE = b"E\x00\x00\x00\t\x00\x00\x00\x00\x00"
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


#: Special singleton sentinel returned to signify the state machine needs more data.
NEED_DATA = object()

#: Special singleton sentinel used to specify a state in the state machine cannot process any
#: events.
_NO_HANDLE = object()


def unrecoverable_error(
    fn: Callable[[ProtocolMachine, BackendMessageCode, Buffer], PostgresMessage]
) -> Callable[[ProtocolMachine, BackendMessageCode, Buffer], PostgresMessage]:
    """
    Decorator that will automatically set the state to an unrecoverable error if an error response
    is found.
    """

    @functools.wraps(fn)
    def wrapper(self: ProtocolMachine, code: BackendMessageCode, body: Buffer):
        if code == BackendMessageCode.ERROR_RESPONSE:
            self.state = ProtocolState.UNRECOVERABLE_ERROR
            error = self._decode_error_response(body)
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

    #: During a simple query, the backend returned an error, signifying something wrong with our
    #: query. We're waiting for the Ready For Query message.
    SIMPLE_QUERY_ERRORED = 103

    #: We're ready to send queries to the server. This is the state inbetween everything happening.
    READY_FOR_QUERY = 999

    #: An unrecoverable error has happened, and the protocol will no longer work.
    UNRECOVERABLE_ERROR = 1000


# noinspection PyMethodMayBeStatic
class ProtocolMachine(object):
    """
    Sans-I/O state machine for the PostgreSQL C<->S protocol. This operates as an in-memory buffer
    that takes in Python-side structures and turns them into bytes to be sent to the server, and
    receives bytes from the server and turns them into Python-side structures.

    **Core usage:**

    1. Call ``do_<method>`` where ``<method>`` is the thing you want to do.
    2. Send the bytes returned to the PostgreSQL server.
    3. Read from the PostgreSQL server.
    4. Feed the read bytes using :meth:`.ProtocolMachine.receive_bytes`.
    5. Call :meth:`.ProtocolMachine.next_event` until it returns ``NEED_DATA``.

    How you handle the events returned from ``next_event`` is up to you.

    **Authentication:**

    Authentication is handled within the protocol machine slightly differently. If
    :meth:`.next_event` returns :class:`.AuthenticationRequest`, the loop should look like this:

    .. code-block:: python3

        while not protocol.is_authenticated():
            to_send = protocol.do_authentication()

            send_data(to_send)
            data = read_data()
            protocol.receive_bytes(data)

    Once the loop exits, the connection is authenticated.
    """

    _LOGGER_COUNTER = count()

    PROTOCOL_MAJOR = 3
    PROTOCOL_MINOR = 0

    _COMMANDS_WITH_COUNTS = {"DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY"}

    def __init__(
        self,
        username: str,
        database: str = None,
        password: str = None,
        application_name: Optional[str] = "pg-purepy",
        logger_name: str = None,
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
            logger_name = __name__ + f":protocol-{next(self._LOGGER_COUNTER)}"

        if database is None:
            database = username

        #: Mapping of miscellaneous connection parameters.
        self.connection_params = {}

        #: The converter classes used to convert between PostgreSQL and Python types.
        self.converters: Dict[int, Converter] = {i.oid: i for i in DEFAULT_CONVERTERS}

        # used for converters, and stores some parameter data so we dont have to update it in two
        # places.
        self._conversion_context = ConversionContext(client_encoding="UTF8")
        self._ignore_unknown_types = ignore_unknown_types

        # used when decoding data rows. unset by CommandComplete.
        self._last_row_description: Optional[RowDescription] = None

        # stored data used for generating various packets
        self.username = username
        self.database = database
        self.application_name = application_name
        self._password = password

        # the state of the protocol machine.
        self._state: ProtocolState = ProtocolState.STARTUP

        ## authentication
        # private because ``is_authenticated()`` consumes buffer data
        self._is_authenticated = False
        # current authentication request. used by various auth functions
        self._auth_request: Optional[AuthenticationRequest] = None
        # scramp client, used for SASL
        self._scramp_client: Optional[ScramClient] = None

        ## buffering
        # infinitely growable buffer used to store incoming packets
        self._buffer = bytearray()
        # last packet code received
        self._current_packet_code = b""
        # number of bytes remaining in the current packet
        self._current_packet_remaining = 0
        # buffer used to store partial packet data
        self._current_packet_buffer = bytearray()
        # if the last receive started processing for a packet, but we didn't receive the full
        # data, we're currently processing a partial packet
        self._processing_partial_packet = False

        self._logger = logging.getLogger(logger_name)

    @property
    def state(self) -> ProtocolState:
        """
        The current state of this connection.
        """
        return self._state

    @state.setter
    def state(self, value: ProtocolState):
        before = self._state
        self._logger.debug(f"Protocol changing state from {before} to {value}")
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
    def has_pending_data(self) -> bool:
        """
        Checks if this has pending data within the buffer.
        """
        return len(self._buffer) > 0

    ## Internal API ##
    def _check_password(self):
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

        if name == "client_encoding":
            self._conversion_context.client_encoding = value
        else:
            self.connection_params[name] = value

        self._logger.debug(f"Parameter status: {name} -> {value}")

        return ParameterStatus(name, value)

    def _decode_error_response(self, body: Buffer) -> ErrorResponse:
        """
        Decodes an error response, returning the :class:`.ErrorResponse` containing all the fields
        from the message.
        """
        kwargs = {}

        while body:
            try:
                raw_code = body.read_byte()
                if raw_code == 0:  # final null sep
                    break

                code = ErrorResponseFieldType(raw_code)
            except ValueError:
                code = ErrorResponseFieldType.UNKNOWN

            str_field = body.read_cstring(encoding=self.encoding)
            if code == ErrorResponseFieldType.UNKNOWN:
                self._logger.warning(
                    f"Encountered unknown field type in error with value {str_field}"
                )
            else:
                kwargs[code.name.lower()] = str_field

        return ErrorResponse(**kwargs)

    def _decode_row_description(self, body: Buffer) -> RowDescription:
        """
        Decodes the row description row.
        """
        field_count = body.read_short()
        self._logger.debug(f"Got {field_count} fields in this row description.")
        fields = []

        for i in range(0, field_count):
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
            format_code = body.read_short()

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

        column_values = []
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
            converter = self.converters.get(col_oid, DEFAULT_TEXT_CONVERTER)
            data = converter.from_postgres(self._conversion_context, data.decode(self.encoding))
            column_values.append(data)

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
    def _handle_during_SENT_STARTUP(self, code: BackendMessageCode, body: Buffer):
        if code == BackendMessageCode.NEGOTIATE_PROTOCOL_VERSION:
            raise ProtocolParseError("Server does not support our protocol")

        elif code == BackendMessageCode.AUTHENTICATION_REQUEST:
            # various different options
            decoded = body.read_int()

            # AuthenticationOk. We skip authentication entirely.
            if decoded == 0:
                self._is_authenticated = True
                self.state = ProtocolState.AUTHENTICATED_WAITING_FOR_COMPLETION
                return AuthenticationCompleted()

            # AuthenticationCleartextPassword
            elif decoded == AuthenticationMethod.CLEARTEXT:
                self._check_password()

                self.state = ProtocolState.CLEARTEXT_STARTUP
                self._auth_request = AuthenticationRequest(method=AuthenticationMethod.CLEARTEXT)
                return self._auth_request

            # AuthenticationMD5
            elif decoded == AuthenticationMethod.MD5:
                self._check_password()

                self.state = ProtocolState.MD5_STARTUP
                md5_bytes = body.read_bytes(4)
                self._auth_request = AuthenticationRequest(
                    method=AuthenticationMethod.MD5, md5_salt=md5_bytes
                )
                return self._auth_request

            # AuthenticationSASL
            elif decoded == AuthenticationMethod.SASL:
                self._check_password()

                self.state = ProtocolState.SASL_STARTUP
                methods = body.read_all_cstrings(encoding=self.encoding, drop_empty=True)
                self._auth_request = AuthenticationRequest(
                    method=AuthenticationMethod.SASL,
                    sasl_methods=methods,
                )
                return self._auth_request
            else:
                raise NotImplementedError(f"Unknown authentication method: {decoded}")

        else:
            raise UnknownMessageError(f"Don't know how to handle {code} in {self.state}")

    @unrecoverable_error
    def _generic_auth_handle(self, code: BackendMessageCode, body: Buffer):
        """
        Authentication handler shared between various authentication "finalisation" methods.
        """
        if code == BackendMessageCode.AUTHENTICATION_REQUEST:
            body_code = body.read_int()
            if body_code == 0:  # AuthenticationOk
                self._is_authenticated = True
                self.state = ProtocolState.AUTHENTICATED_WAITING_FOR_COMPLETION
                return AuthenticationCompleted()
            else:
                raise ProtocolParseError(
                    f"Unexpected response: Expected AuthenticationOk, but got {body_code}"
                )
        else:
            raise ProtocolParseError(f"Unknown message code {code} for state {self.state}")

    _handle_during_CLEARTEXT_SENT = _generic_auth_handle
    _handle_during_MD5_SENT = _generic_auth_handle
    _handle_during_SASL_FINAL_SENT = _generic_auth_handle

    @unrecoverable_error
    def _handle_during_AUTHENTICATED_WAITING_FOR_COMPLETION(
        self,
        code: BackendMessageCode,
        body: Buffer,
    ):
        """
        Handles incoming messages once we've authenticated.
        """
        if code == BackendMessageCode.READY_FOR_QUERY:
            self.state = ProtocolState.READY_FOR_QUERY
            state = ReadyForQueryState(body.read_byte())

            return ReadyForQuery(state)
        elif code == BackendMessageCode.BACKEND_KEY_DATA:
            pid = body.read_int()
            key = body.read_int()
            return BackendKeyData(pid, key)
        else:
            raise ProtocolParseError(f"Expected ReadyForQuery, got {code!r}")

    ### Simple Query ###
    def _got_row_description(self, body: Buffer):
        """
        Common logic for when we get a row description.
        """
        row_desc = self._decode_row_description(body)
        self.state = ProtocolState.SIMPLE_QUERY_RECEIVED_ROW_DESCRIPTION
        self._logger.debug(f"Incoming data has {len(row_desc.columns)} columns.")
        self._last_row_description = row_desc
        return row_desc

    def _handle_during_SIMPLE_QUERY_SENT_QUERY(self, code: BackendMessageCode, body: Buffer):
        """
        Handles incoming messages once we've sent a query exection message.
        """
        if code == BackendMessageCode.ROW_DESCRIPTION:
            return self._got_row_description(body)

        elif code == BackendMessageCode.ERROR_RESPONSE:
            # this is a recoverable error, and simply moves onto the ReadyForQuery message.
            self.state = ProtocolState.SIMPLE_QUERY_ERRORED
            error = self._decode_error_response(body)
            self._logger.warning(f"Error during query: {error.severity}: {error.message}")
            return error

        else:
            raise ProtocolParseError(f"Expected RowDescription, got {code!r}")

    @unrecoverable_error
    def _handle_during_SIMPLE_QUERY_RECEIVED_ROW_DESCRIPTION(
        self, code: BackendMessageCode, body: Buffer
    ):
        """
        Handles incoming data rows during a simple query.
        """
        if code == BackendMessageCode.DATA_ROW:
            return self._decode_data_row(body)

        elif code == BackendMessageCode.COMMAND_COMPLETE:
            self.state = ProtocolState.SIMPLE_QUERY_RECEIVED_COMMAND_COMPLETE
            self._last_row_description = None
            return self._decode_command_complete(body)

        else:
            raise ProtocolParseError(f"Expected DataRow or CommandComplete, got {code!r}")

    @unrecoverable_error
    def _handle_during_SIMPLE_QUERY_RECEIVED_COMMAND_COMPLETE(
        self, code: BackendMessageCode, body: Buffer
    ):
        """
        Waits for the ReadyForQuery response, or the next row description if this was a multi-query.
        """
        if code == BackendMessageCode.ROW_DESCRIPTION:
            # multiple queries in one statement
            return self._got_row_description(body)

        elif code == BackendMessageCode.READY_FOR_QUERY:
            self.state = ProtocolState.READY_FOR_QUERY
            return ReadyForQuery(ReadyForQueryState(body.read_byte()))

        raise ProtocolParseError(f"Unexpected message {code!r}")

    @unrecoverable_error
    def _handle_during_SIMPLE_QUERY_ERRORED(self, code: BackendMessageCode, body: Buffer):
        """
        Waits for the ReadyForQuery response.
        """
        if code == BackendMessageCode.READY_FOR_QUERY:
            self._last_row_description = None
            self.state = ProtocolState.READY_FOR_QUERY
            return ReadyForQuery(ReadyForQueryState(body.read_byte()))

        raise ProtocolParseError(f"Expected ReadyForQuery, got {code!r}")

    ## Public API ##
    def add_converter(self, converter: Converter):
        """
        Adds a new converter to this protocol.
        """
        self.converters[converter.oid] = converter

    def receive_bytes(self, data: bytes):
        """
        Receives incoming bytes from the server. This merely appends to an internal buffer; you
        need to call :meth:`.next_event` to do anything.

        :param data: The incoming data from PostgreSQL.
        """
        # these are assertions so that ``python -O`` optimises them out. its a bug to do anything
        # once the protocol is in UNRECOVERABLE_ERROR anyway.
        assert self._state != ProtocolState.UNRECOVERABLE_ERROR, "state is unrecoverable error"

        self._logger.debug(f"Protocol: Received {len(data)} bytes")
        self._buffer += data

    def do_startup(self) -> bytes:
        """
        Gets the startup packet, and sets the state machine up for starting up.

        :return: A startup message, that should be sent to PostgreSQL.
        """
        assert self._state != ProtocolState.UNRECOVERABLE_ERROR, "state is unrecoverable error"

        if self._state != ProtocolState.STARTUP:
            raise IllegalStateError("Can't do startup outside of the startup state")

        self._logger.debug("Sending startup message...")
        packet_body = struct.pack("!i", (self.PROTOCOL_MAJOR << 16) | self.PROTOCOL_MINOR)
        packet_body += pack_strings("user", self.username)
        packet_body += pack_strings("database", self.database)
        packet_body += pack_strings("application_name", self.application_name)
        packet_body += pack_strings("client_encoding", self._conversion_context.client_encoding)
        packet_body += b"\x00"
        size = struct.pack("!i", (len(packet_body) + 4))

        self.state = ProtocolState.SENT_STARTUP
        return size + packet_body

    def do_simple_query(self, query_text: str) -> bytes:
        """
        Performs a simple (static) query. This query cannot have any dynamic parameters.
        """
        if self._state != ProtocolState.READY_FOR_QUERY:
            raise IllegalStateError("The server is not ready for queries")

        self._logger.debug(f"Sending static query {query_text}")
        packet_body = pack_strings(query_text, encoding=self._conversion_context.client_encoding)
        header = FrontendMessageCode.QUERY.to_bytes(length=1, byteorder="big")
        header += (len(packet_body) + 4).to_bytes(length=4, byteorder="big")

        self.state = ProtocolState.SIMPLE_QUERY_SENT_QUERY
        return header + packet_body

    def do_create_prepared_statement(self, name: Optional[str], query_text: str) -> bytes:
        """
        Creates a prepared statement. If ``name`` is not specified, this will create the unnamed
        prepared statement.
        """

    def next_event(self) -> Union[PostgresMessage, object]:
        """
        Reads the next event from the message queue.

        :return: Either a :class:`.PostgresMessage`, or the special :data:`NEED_DATA` constant.
        """
        self._logger.debug(f"Called next_event(), state is {self.state.name}")

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
            self._logger.debug(f"Partial packet: {data} / {self._buffer}")
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
                self._logger.debug(
                    f"Received truncated packet of {len(message_data)} size, " f"need {size} bytes"
                )
                # not enough data yet.
                self._processing_partial_packet = True
                self._current_packet_buffer = message_data
                self._current_packet_code = code
                self._current_packet_remaining = size - len(message_data)
                # reset buffer, otherwise we try and read the message code off again >.>
                self._buffer = bytearray()
                return NEED_DATA
            else:
                # yes enough data, set the buffer to the data after the size bytes for future
                # processing
                self._buffer = self._buffer[size + 5 :]

        try:
            method = getattr(self, f"_handle_during_{self.state.name}")
        except AttributeError:
            raise AttributeError(f"No method exists to process state {self.state.name}") from None

        if isinstance(method, str):
            raise IllegalStateError(method)

        code = BackendMessageCode(code)
        buffer = Buffer(message_data)

        # we have special handling for parameter statuses, as they can handle in any state
        # and repeating this block over and over would be annoying.
        if code == BackendMessageCode.PARAMETER_STATUS:
            return self._handle_parameter_status(buffer)

        # if we made it here, we can actually process the packet since we have it in full

        result = method(code, buffer)
        if result is None:
            raise Exception(f"{method.__qualname__} returned None! This is a bug!")

        self._logger.debug(f"Got message object: {result}")
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

        if self.state == ProtocolState.CLEARTEXT_STARTUP:
            self._check_password()
            self._logger.debug("Received cleartext password authentication request.")

            code = FrontendMessageCode.PASSWORD
            packet_body += self._password.encode(encoding="utf-8")
            self.state = ProtocolState.CLEARTEXT_SENT

        elif self.state == ProtocolState.MD5_STARTUP:
            self._check_password()

            code = FrontendMessageCode.PASSWORD
            inner_passsword = md5(self._password.encode("utf-8") + self.username.encode("utf-8"))
            encoded_password = (
                md5(inner_passsword.hexdigest().encode("ascii") + self._auth_request.md5_salt)
                .hexdigest()
                .encode("ascii")
            )
            packet_body += b"md5"
            packet_body += encoded_password
            packet_body += b"\x00"

            self.state = ProtocolState.MD5_SENT

        elif self.state == ProtocolState.SASL_STARTUP:
            code = FrontendMessageCode.PASSWORD
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

        # blank states, nothing to send.
        elif self.state in (
            ProtocolState.CLEARTEXT_SENT,
            ProtocolState.MD5_SENT,
            ProtocolState.SASL_FIRST_SENT,
        ):
            return b""

        elif self.state == ProtocolState.SASL_FIRST_RECEIVED:
            code = FrontendMessageCode.PASSWORD
            message = self._scramp_client.get_client_final()
            packet_body += message.encode("ascii")
            self.state = ProtocolState.SASL_FINAL_SENT

        # similar blank state
        elif self.state == ProtocolState.SASL_FINAL_SENT:
            return b""

        elif self.state == ProtocolState.AUTHENTICATED_WAITING_FOR_COMPLETION:
            return b""

        else:
            return b""

        msg_len = (len(packet_body) + 4).to_bytes(length=4, byteorder="big", signed=False)
        return code.to_bytes(length=1, byteorder="big") + msg_len + packet_body

    def is_authenticated(self) -> bool:
        """
        Checks if this connection is authenticated. This will perform internal work during the
        authentication loop, but only return normally outside of the authentication loop.
        """
        if self._is_authenticated:
            return True

        # this method actually pumps events
        evt = self.next_event()
        if evt is NEED_DATA:
            return False

        if isinstance(evt, AuthenticationCompleted):
            self._is_authenticated = True
            return True


# TLS helpers
def check_if_tls_accepted(reply: bytes) -> bool:
    """
    Checks if the server accepted the TLS request.
    """
    return reply == b"S"
