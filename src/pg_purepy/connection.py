"""
The AnyIO implementation of the PostgreSQL client.
"""

from __future__ import annotations

import logging
import types
import warnings
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import aclosing, asynccontextmanager
from datetime import tzinfo
from os import PathLike, fspath
from ssl import SSLContext
from typing import (
    Any,
    Self,
    TypeVar,
)

import anyio
from anyio import EndOfStream, Lock
from anyio.abc import ByteStream, SocketStream
from anyio.streams.tls import TLSStream

from pg_purepy.conversion.abc import Converter
from pg_purepy.dbapi import convert_paramstyle
from pg_purepy.exc import IllegalStateError, PostgresqlError
from pg_purepy.messages import (
    BackendKeyData,
    BaseDatabaseError,
    BindComplete,
    CommandComplete,
    DataRow,
    ErrorOrNoticeResponse,
    PostgresMessage,
    PreparedStatementInfo,
    QueryResultMessage,
    wrap_error,
)
from pg_purepy.protocol import (
    NEED_DATA,
    SSL_MESSAGE,
    NeedData,
    ProtocolParseError,
    ReadyForQuery,
    SansIOClient,
    check_if_tls_accepted,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RollbackTimeoutError(PostgresqlError):
    """
    Raised when a ROLLBACK failed.
    """


class AsyncPostgresConnection:
    """
    An asynchronous connection to a PostgreSQL server. This class should not be directly
    instantiated; instead, use :func:`.open_database_connection`.
    """

    def __init__(
        self,
        address_or_path: str | PathLike[str],
        port: int,
        ssl_context: SSLContext | None,
        stream: ByteStream,
        state: SansIOClient,
        block_transactions: bool = False,
    ):
        self._addr = address_or_path
        self._port = port
        self._ssl_context = ssl_context

        self._stream = stream
        self._protocol = state

        self._query_lock = Lock()

        # marks if the connection is dead, usually if a connection error happens during read/write.
        self._dead = False

        # backend PID as returned from postgresql.
        self._pid: int = -1
        self._secret_key: int = -1

        self._block_transactions = block_transactions

    @property
    def ready(self) -> bool:
        """
        Returns if this connection is ready for another query.
        """
        return self._protocol.ready

    @property
    def in_transaction(self) -> bool:
        """
        Returns if this connection is currently in a transaction.
        """
        return self._protocol.in_transaction

    @property
    def dead(self) -> bool:
        """
        Returns if this connection is dead or otherwise unusable.
        """
        if self._dead:
            return True

        return self._protocol.dead

    @property
    def connection_parameters(self) -> Mapping[str, str]:
        """
        Returns a read-only view of the current connection;
        """
        return types.MappingProxyType(self._protocol.connection_params)

    @property
    def server_timezone(self) -> tzinfo:
        """
        Returns the timezone of the server.
        """
        return self._protocol.timezone

    def add_converter(self, converter: Converter) -> None:
        """
        Registers a :class:`.Converter` with this connection.
        """
        self._protocol.add_converter(converter)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} pid='{self._pid!r}'>"

    async def _read(self) -> bytes:
        try:
            return await self._stream.receive()
        except (ConnectionError, EndOfStream):
            self._dead = True
            raise

    async def _write(self, item: bytes) -> None:
        try:
            return await self._stream.send(item)
        except (ConnectionError, EndOfStream):
            self._dead = True
            raise

    async def _do_startup(self) -> None:
        """
        Sends the startup message.
        """

        data = self._protocol.do_startup()
        await self._write(data)

    async def _terminate(self) -> None:
        """
        Terminates the protocol. Does not close the connection.
        """

        data = self._protocol.do_terminate()
        await self._write(data)
        self._dead = True

    async def _read_until_ready(
        self
    ) -> AsyncGenerator[ErrorOrNoticeResponse | PostgresMessage | NeedData, None]:
        """
        Yields events until the connection is ready. This is an asynchronous generator. You can
        discard events you don't care about.

        This must ONLY be called if the protocol is NOT ready.
        """

        if self._protocol.ready:
            await anyio.sleep(0)
            return

        while True:
            while True:
                next_event = self._protocol.next_event()
                if next_event is NEED_DATA:
                    break

                if isinstance(next_event, ErrorOrNoticeResponse) and next_event.notice:
                    if next_event.severity == "WARNING":
                        err = wrap_error(next_event)
                        warnings.warn(str(err), stacklevel=2)

                elif isinstance(next_event, BackendKeyData):
                    logger.debug(
                        f"Got backend key data: {next_event.secret_key} / {next_event.pid}"
                    )
                    self._secret_key = next_event.secret_key
                    self._pid = next_event.pid

                yield next_event

                if isinstance(next_event, ReadyForQuery):
                    await anyio.sleep(0)  # checkpoint()
                    return

            to_send = self._protocol.get_needed_synchronisation()
            if to_send:
                await self._write(to_send)

            received = await self._read()
            self._protocol.receive_bytes(received)

    async def wait_until_ready(self) -> None:
        """
        Waits until the connection is ready. This discards all events. Useful in the authentication
        loop.
        """

        async with aclosing(self._read_until_ready()) as gen:
            async for message in gen:
                if isinstance(message, ErrorOrNoticeResponse):
                    err = wrap_error(message)
                    raise err

    async def _wait_for_message(self, typ: type[T], *, wait_until_ready: bool = True) -> T:
        """
        Waits until a message of type ``typ`` arrives.

        This will wait until the ReadyForQuery message arrives to avoid requiring extra
        synchronisation, if ``wait_until_ready`` is True. If it never arrives, this will deadlock!
        """

        message_found = None
        async with aclosing(self._read_until_ready()) as gen:
            async for item in gen:
                if isinstance(item, typ):
                    if not wait_until_ready:
                        return item

                    message_found = item

                elif isinstance(item, ErrorOrNoticeResponse):
                    raise wrap_error(item)

            if message_found is None:
                raise IllegalStateError(f"No message of type {typ} was yielded")

            return message_found

    ## Low-level API ##
    async def create_prepared_statement(self, name: str, query: str) -> PreparedStatementInfo:
        """
        Creates a prepared statement. This is part of the low-level query API.

        :param name: The name of the prepared statement.
        :param query: The query to use.
        """

        to_send = self._protocol.do_create_prepared_statement(name=name, query_text=query)
        await self._write(to_send)
        return await self._wait_for_message(PreparedStatementInfo)

    async def lowlevel_query(
        self,
        query: str | PreparedStatementInfo,
        *params: str,
        max_rows: int | None = None,
        **kwargs: dict[str, Any],
    ) -> AsyncGenerator[QueryResultMessage, None]:
        """
        Performs a query to the server. This is an asynchronous generator; you must iterate over
        values in order to get the messages returned from the server.
        """

        async with self._query_lock:
            # always wait until ready! we do not like getting random messages from the last client
            # intermixed
            if not self._protocol.ready:
                await self.wait_until_ready()

            simple_query = all(
                (
                    not (params or kwargs),
                    not isinstance(query, PreparedStatementInfo),
                    max_rows is None,
                )
            )

            logger.debug(f"EXECUTE:\n{query}")
            if simple_query:
                data = self._protocol.do_simple_query(query)  # type: ignore
                await self._write(data)
            else:
                if not isinstance(query, PreparedStatementInfo):
                    real_query, new_params = convert_paramstyle(query, kwargs)  # type: ignore
                    params = params + new_params
                    info = await self.create_prepared_statement(name="", query=real_query)
                else:
                    info = query

                bound_data = self._protocol.do_bind_execute(info, params, max_rows)
                await self._write(bound_data)
                # we need to get BindComplete because we need to yield the statement's
                # RowDescription out, for a more "consistent" view.
                await self._wait_for_message(BindComplete, wait_until_ready=False)
                # no error, so the query is gonna complete successfully

                if info.row_description:
                    yield info.row_description

            async with aclosing(self._read_until_ready()) as agen:
                async for message in agen:
                    if isinstance(message, ErrorOrNoticeResponse) and not message.notice:
                        err = wrap_error(message)
                        raise err

                    if isinstance(message, QueryResultMessage):
                        yield message

    ## Mid-level API. ##
    @asynccontextmanager
    async def query(
        self,
        query: str | PreparedStatementInfo,
        *params: str,
        max_rows: int | None = None,
        **kwargs: Any,
    ) -> AsyncGenerator[QueryResult, None]:
        """
        Mid-level query API.

        The ``query`` parameter can either be a string or a :class:`~.PreparedStatementInfo`, as
        returned from :func:`.AsyncPostgresConnection.create_prepared_statement`. If it is a
        string, and it has parameters, they must be provided as keyword arguments.
        If it is a pre-prepared statement, and it has parameters, they must be provided as
        positional arguments.

        If keyword arguments are provided or a prepared statement is passed, an extended query with
        secure argument parsing will be used. Otherwise, a simple query will be used, which saves
        bandwidth over the extended query protocol.

        If the server is currently in a failed transaction, then your query will be ignored. Make
        sure to issue a rollback beforehand, if needed.

        This is an asynchronous context manager that yields a :class:`.QueryResult`, that can
        be asynchronously iterated over for the data rows of the query. Once all data rows have
        been iterated over, you can call :meth:`~.QueryResult.row_count` to get the total row count.

        If ``max_rows`` is specified, then the query will only return up to that many rows.
        Otherwise, an unlimited amount may potentially be returned.
        """

        async with aclosing(
            self.lowlevel_query(query, *params, max_rows=max_rows, **kwargs),
        ) as agen:
            yield QueryResult(agen.__aiter__())
            # always wait
            await self.wait_until_ready()

    async def _safely_rollback(self, exc: BaseException | None) -> None:
        """
        Safely performs a rollback, even in the presence of an exception.
        """

        with anyio.move_on_after(5.0, shield=True) as scope:
            await self.execute("rollback;")

        if scope.cancel_called:
            with anyio.CancelScope(shield=True):
                await self._stream.aclose()
                self._dead = True

            raise RollbackTimeoutError(
                "Failed to rollback transaction in time, forcibly closing connection"
            ) from exc

    @asynccontextmanager
    async def with_transaction(self) -> AsyncGenerator[None, None]:
        """
        Asynchronous context manager that automatically opens and closes a transaction.
        """

        if self._block_transactions:
            raise ValueError(
                "This connection was already checked out from a pool in a "
                "transaction and this method should not be used."
            )

        try:
            await self.execute("begin;")
            yield
        except (Exception, anyio.get_cancelled_exc_class()) as e:
            await self._safely_rollback(e)
            raise
        else:
            await self.execute("commit;")

    ### DBAPI style methods ###
    async def fetch(
        self,
        query: str | PreparedStatementInfo,
        *params: Any,
        max_rows: int | None = None,
        **kwargs: Any,
    ) -> list[DataRow]:
        """
        Eagerly fetches the result of a query. This returns a list of :class:`~.DataRow` objects.

        If you wish to lazily load the results of a query, use
        :meth:`.query` instead.

        :param query: Either a :class:`str` that contains the query text,
                      or a :class:`~.PreparedStatementInfo` that represents a pre-prepared query.
        :param params: The positional arguments for the query.
        :param max_rows: The maximum rows to return.
        :param kwargs: The colon arguments for the query.
        """

        async with self.query(query, *params, max_rows=max_rows, **kwargs) as q:
            return [i async for i in q]

    async def fetch_one(
        self,
        query: str | PreparedStatementInfo,
        *params: Any,
        **kwargs: Any,
    ) -> DataRow | None:
        """
        Like :meth:`.fetch`, but only fetches one row.
        """

        row = await self.fetch(query, *params, **kwargs)

        try:
            return row[0]
        except IndexError:
            return None

    async def execute(
        self,
        query: str | PreparedStatementInfo,
        *params: Any,
        max_rows: int | None = None,
        **kwargs: Any,
    ) -> int:
        """
        Executes a query, returning its row count. This will discard all data rows.

        :param query: Either a :class:`str` that contains the query text,
                      or a :class:`~.PreparedStatementInfo` that represents a pre-prepared query.
        :param params: The positional arguments for the query.
        :param max_rows: The maximum rows to return.
        :param kwargs: The colon arguments for the query.
        """

        async with self.query(query, *params, max_rows=max_rows, **kwargs) as q:
            return await q.row_count()


class QueryResult(AsyncIterator[DataRow]):
    """
    Wraps the execution of a query. This can be asynchronously iterated over in order to get
    incoming data rows.
    """

    def __init__(self, iterator: AsyncIterator[PostgresMessage]):
        """
        :param iterator: An iterator of :class:`.PostgresMessage` instances returned from the
                         server.
        """

        self._iterator = iterator
        self._row_count = -1

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> bool:
        return False

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> DataRow:
        # infinitely loops until we get a message we care about

        while True:
            next_message = await self._iterator.__anext__()
            if isinstance(next_message, DataRow):
                return next_message

            if isinstance(next_message, CommandComplete):
                # some messages don't have a row count, e.g. CREATE
                if next_message.row_count is None:
                    self._row_count = 0
                else:
                    self._row_count = next_message.row_count

                raise StopAsyncIteration

    async def row_count(self) -> int:
        """
        Gets the row count for this query.

        .. warning::

            This will discard any remaining data rows in the currently executing query.
        """

        if self._row_count >= 0:
            await anyio.sleep(0)  # checkpoint
            return self._row_count

        async for _ in self:
            pass

        return self._row_count


# TODO: support byte paths


async def _open_socket(
    address_or_path: str | PathLike[str],
    *,
    port: int = 5432,
    ssl_context: SSLContext | None = None,
) -> SocketStream | TLSStream:
    """
    Opens the socket to the PostgreSQL server.
    """

    address_or_path = fspath(address_or_path)
    sock: SocketStream | TLSStream
    if address_or_path.startswith("/"):
        logger.debug(f"Opening unix connection to {address_or_path}")
        sock = await anyio.connect_unix(address_or_path)
    else:
        logger.debug(f"Opening TCP connection to {address_or_path}:{port}")
        sock = await anyio.connect_tcp(remote_host=address_or_path, remote_port=port)

    try:
        if ssl_context:
            logger.debug("Using TLS for the connection...")

            await sock.send(SSL_MESSAGE)
            response = await sock.receive(1)
            if not check_if_tls_accepted(response):
                raise ProtocolParseError("Requested TLS, but server said no")

            sock = await TLSStream.wrap(
                sock, hostname=address_or_path, ssl_context=ssl_context, standard_compatible=True
            )
    except BaseException:
        await sock.aclose()
        raise

    return sock


async def _open_connection(
    address_or_path: str | PathLike[str],
    username: str,
    *,
    port: int = 5432,
    password: str | None = None,
    database: str | None = None,
    ssl_context: SSLContext | None = None,
) -> tuple[SocketStream | TLSStream, AsyncPostgresConnection]:
    """
    Actual implementation of connection opening.
    """

    sock = await _open_socket(address_or_path, port=port, ssl_context=ssl_context)

    protocol = SansIOClient(username, database, password)
    conn = AsyncPostgresConnection(
        address_or_path,  # used for pool cancellation
        port,
        ssl_context,
        sock,
        protocol,
    )

    try:
        await conn._do_startup()
        await conn.wait_until_ready()
    except BaseException:
        await sock.aclose()
        raise

    return sock, conn


@asynccontextmanager
async def open_database_connection(
    address_or_path: str | PathLike[str],
    username: str,
    *,
    port: int = 5432,
    password: str | None = None,
    database: str | None = None,
    ssl_context: SSLContext | None = None,
) -> AsyncGenerator[AsyncPostgresConnection, None]:
    """
    Opens a new connection to the PostgreSQL database server. This is an asynchronous context
    manager.

    .. code-block:: python3

        async with open_database_connection("localhost", username="postgres") as db:
            ...

    Required parameters:

    :param address_or_path: The address of the server or the *absolute path* of its Unix socket.
    :param username: The username to authenticate with.

    Optional parameters:

    :param port: The port to connect to. Ignored for unix sockets.
    :param password: The password to authenticate with.
    :param database: The database to connect to. Defaults to the username.
    :param ssl_context: The SSL context to use for TLS connection. Enables TLS if specified.
    """

    sock, conn = await _open_connection(
        address_or_path=address_or_path,
        username=username,
        port=port,
        password=password,
        database=database,
        ssl_context=ssl_context,
    )

    async with sock:
        # this sucks but we send a Terminate in the normal case, a Terminate in the case of a
        # database error, and a regular socket/TLS close in all other cases.
        try:
            yield conn
            await conn._terminate()
        except BaseDatabaseError:
            await conn._terminate()
            raise
