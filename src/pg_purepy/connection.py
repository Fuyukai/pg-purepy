"""
The AnyIO implementation of the PostgreSQL client.
"""

from __future__ import annotations

import warnings
from contextlib import asynccontextmanager
from os import PathLike
from ssl import SSLContext
from typing import (
    Union,
    AsyncContextManager,
    AsyncIterator,
    TypeVar,
    Type,
    List,
    Optional,
)

import anyio
from anyio import Lock
from anyio.abc import ByteStream
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from anyio.streams.tls import TLSStream
from pg_purepy.dbapi import convert_paramstyle
from pg_purepy.exc import IllegalStateError
from pg_purepy.messages import (
    ErrorOrNoticeResponse,
    wrap_error,
    QueryResultMessage,
    PreparedStatementInfo,
    BaseDatabaseError,
    BindComplete,
    PostgresMessage,
    DataRow,
    CommandComplete,
)
from pg_purepy.protocol import (
    SansIOClient,
    SSL_MESSAGE,
    check_if_tls_accepted,
    ProtocolParseError,
    NEED_DATA,
    ReadyForQuery,
)

try:
    from contextlib import aclosing
except ImportError:
    from async_generator import aclosing

T = TypeVar("T")


class AsyncPostgresConnection(object):
    """
    An asynchronous connection to a PostgreSQL server. This class should not be directly
    instantiated; instead, use :meth:`.open_database_connection`.
    """

    def __init__(
        self,
        stream: ByteStream,
        state: SansIOClient,
    ):
        self._stream = stream
        self._protocol = state

        self._query_lock = Lock()

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

    async def _do_startup(self):
        """
        Sends the startup message.
        """
        data = self._protocol.do_startup()
        await self._stream.send(data)

    async def _terminate(self):
        """
        Terminates the protocol. Does not close the connection.
        """
        data = self._protocol.do_terminate()
        await self._stream.send(data)

    async def _read_until_ready(self):
        """
        Yields events until the connection is ready. This is an asynchronous generator. You can
        discard events you don't care about.

        This must ONLY be called if the protocol is NOT ready.
        """
        while not self._protocol.ready:
            while True:
                next_event = self._protocol.next_event()
                if next_event is NEED_DATA:
                    break

                if isinstance(next_event, ErrorOrNoticeResponse) and next_event.notice:
                    if next_event.severity == "WARNING":
                        err = wrap_error(next_event)
                        warnings.warn(str(err))

                yield next_event

                if isinstance(next_event, ReadyForQuery):
                    await anyio.sleep(0)  # checkpoint()
                    return

            to_send = self._protocol.get_needed_synchronisation()
            if to_send:
                await self._stream.send(to_send)

            received = await self._stream.receive(65536)
            self._protocol.receive_bytes(received)

    async def wait_until_ready(self):
        """
        Waits until the conneection is ready. This discards all events. Useful in the authentication
        loop.
        """
        async with aclosing(self._read_until_ready()) as gen:
            async for message in gen:
                if isinstance(message, ErrorOrNoticeResponse):
                    err = wrap_error(message)
                    raise err

    async def _wait_for_message(self, typ: Type[T], *, wait_until_ready: bool = True) -> T:
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
        await self._stream.send(to_send)
        pc = await self._wait_for_message(PreparedStatementInfo)
        return pc

    async def lowlevel_query(
        self,
        query: Union[str, PreparedStatementInfo],
        *params,
        **kwargs,
    ) -> AsyncIterator[QueryResultMessage]:
        """
        Performs a query to the server. This is an asynchronous generator; you must iterate over
        values in order to get the messages returned from the server.
        """
        async with self._query_lock:
            # always wait until ready! we do not like getting random messages from the last client
            # intermixed
            if not self._protocol.ready:
                await self.wait_until_ready()

            simple_query = not ((params or kwargs) or isinstance(query, PreparedStatementInfo))
            if simple_query:
                data = self._protocol.do_simple_query(query)
                await self._stream.send(data)
            else:
                if not isinstance(query, PreparedStatementInfo):
                    real_query, params = convert_paramstyle(query, kwargs)
                    info = await self.create_prepared_statement(name="", query=real_query)
                else:
                    info = query

                bound_data = self._protocol.do_bind_execute(info, params)
                await self._stream.send(bound_data)
                # we need to get BindComplete because we need to yield the statement's
                # RowDescription out, for a more "consistent" view.
                await self._wait_for_message(BindComplete, wait_until_ready=False)
                # no error, so the query is gonna complete successfully
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
        query: Union[str, PreparedStatementInfo],
        *params,
        **kwargs,
    ) -> AsyncContextManager[QueryResult]:
        """
        Mid-level query API.

        The ``query`` parameter can either be a string or a :class:`~.PreparedStatementInfo`, as
        returned from :func:`~.create_prepared_statement`. If it is a string, and it has parameters,
        they must be provided as keyword arguments. If it is a pre-prepared statement, and it has
        parameters, they must be provided as positional arguments.

        If keyword arguments are provided or a prepared statement is passed, an extended query with
        secure argument parsing will be used. Otherwise, a simple query will be used, which saves
        bandwidth over the extended query protocol.

        If the server is currently in a failed transaction, then your query will be ignored. Make
        sure to issue a rollback beforehand, if needed.

        This is an asynchronous context manager that yields a :class:`.QueryResult`, that can
        be asynchronously iterated over for the data rows of the query. Once all data rows have
        been iterated over, you can call :func:`.QueryResult.row_count` to get the total row count.
        """
        async with aclosing(self.lowlevel_query(query, *params, **kwargs)) as agen:
            yield QueryResult(agen.__aiter__())
            # always wait
            await self.wait_until_ready()

    @asynccontextmanager
    async def with_transaction(self) -> None:
        """
        Asynchronous context manager that automatically opens and closes a transaction.
        """
        try:
            await self.execute("begin;")
            yield
        except Exception:
            await self.execute("rollback;")
            raise
        else:
            await self.execute("commit;")

    ### DBAPI style methods ###
    async def fetch(
        self, query: Union[str, PreparedStatementInfo], *params, **kwargs
    ) -> List[DataRow]:
        """
        Eagerly fetches the result of a query. This returns a list of :class:`~.DataRow` objects.

        If you wish to lazily load the results of a query, use
        :meth:`~.AsyncPostgresConnection.query` instead.

        :param query: Either a :class:`str` that contains the query text,
                      or a :class:`~.PreparedStatementInfo` that represents a pre-prepared query.
        """
        async with self.query(query, *params, **kwargs) as q:
            return [i async for i in q]

    async def execute(self, query: Union[str, PreparedStatementInfo], *params, **kwargs) -> int:
        """
        Executes a query, returning its row count. This will discard all data rows.

        :param query: Either a :class:`str` that contains the query text,
              or a :class:`~.PreparedStatementInfo` that represents a pre-prepared query.
        """
        async with self.query(query, *params, **kwargs) as q:
            return await q.row_count()


class QueryResult:
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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        # infinitely loops until we get a message we care about

        while True:
            next_message = await self._iterator.__anext__()
            if isinstance(next_message, DataRow):
                return next_message
            elif isinstance(next_message, CommandComplete):
                # some messages don't have a row count, e.g. CREATE
                if next_message.row_count is None:
                    self._row_count = 0
                else:
                    self._row_count = next_message.row_count

                raise StopAsyncIteration

    async def row_count(self):
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


# noinspection PyProtectedMember
@asynccontextmanager
async def open_database_connection(
    address_or_path: Union[str, PathLike],
    username: str,
    *,
    port: int = 5432,
    password: str = None,
    database: str = None,
    ssl_context: SSLContext = None,
) -> AsyncContextManager[AsyncPostgresConnection]:
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
    if address_or_path.startswith("/"):
        sock = await anyio.connect_unix(address_or_path)
    else:
        sock = await anyio.connect_tcp(remote_host=address_or_path, remote_port=port)

    try:
        if ssl_context:
            await sock.send(SSL_MESSAGE)
            response = await sock.receive(1)
            if not check_if_tls_accepted(response):
                raise ProtocolParseError("Requested TLS, but server said no")

            sock = await TLSStream.wrap(
                sock, hostname=address_or_path, ssl_context=ssl_context, standard_compatible=True
            )
    except BaseException:
        with anyio.move_on_after(delay=0.5, shield=True):
            await sock.aclose()

        raise

    async with sock:
        protocol = SansIOClient(username, database, password)
        conn = AsyncPostgresConnection(
            sock,
            protocol,
        )

        await conn._do_startup()
        await conn.wait_until_ready()

        # this sucks but we send a Terminate in the normal case, a Terminate in the case of a
        # database error, and a regular close in all other cases.
        try:
            yield conn
            await conn._terminate()
        except BaseDatabaseError:
            await conn._terminate()
            raise
