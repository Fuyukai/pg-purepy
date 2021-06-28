from __future__ import annotations

from contextlib import asynccontextmanager
from os import PathLike
from ssl import SSLContext
from typing import Union, AsyncContextManager, AsyncIterator, List

import anyio
from anyio import Lock
from anyio.abc import ByteStream
from anyio.streams.tls import TLSStream
from pg_purepy.messages import ErrorResponse, wrap_error, QueryResultMessage, DataRow
from pg_purepy.protocol import (
    ProtocolMachine,
    SSL_MESSAGE,
    check_if_tls_accepted,
    ProtocolParseError,
    NEED_DATA,
    ReadyForQuery,
)


class SimpleQuery(object):
    """"""

    def __init__(self, conn: AsyncPostgresConnection):
        self._conn = conn


class AsyncPostgresConnection(object):
    """
    An asynchronous connection to a PostgreSQL server. This class should not be directly
    instantiated; instead, use :meth:`.open_database_connection`.
    """

    def __init__(
        self,
        stream: ByteStream,
        state: ProtocolMachine,
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

    async def _do_startup(self):
        """
        Sends the startup message.
        """
        data = self._protocol.do_startup()
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

                yield next_event

                if isinstance(next_event, ReadyForQuery):
                    await anyio.sleep(0)  # checkpoint()
                    return

            to_send = self._protocol.get_needed_synchronisation()
            if to_send:
                await self._stream.send(to_send)

            received = await self._stream.receive(65536)
            self._protocol.receive_bytes(received)

    async def _wait_until_ready(self):
        """
        Waits until the conneection is ready. This discards all events. Useful in the authentication
        loop.
        """
        async for message in self._read_until_ready():
            if isinstance(message, ErrorResponse):
                err = wrap_error(message)
                raise err

    async def simple_query(self, query: str) -> AsyncIterator[QueryResultMessage]:
        """
        Issues a simple query to the server. This does NOT support parameter binding; this should
        be used for static queries only.

        This is an asynchronous generator; it returns :class:`.QueryResultMessage` instances.
        This will be one :class:`.RowDescription`, zero or more :class:`.DataRow`, and one
        :class:`.CommandComplete`,
        """
        async with self._query_lock:
            # we always wait until ready, so that if the client picked up an exception
            # but recovered from it, we don't try and send a query without the ReadyForQuery
            # message arriving.
            if not self._protocol.ready:
                await self._wait_until_ready()

            data = self._protocol.do_simple_query(query)
            await self._stream.send(data)

            async for message in self._read_until_ready():
                if isinstance(message, ErrorResponse):
                    err = wrap_error(message)
                    raise err

                if isinstance(message, QueryResultMessage):
                    yield message

    async def simple_query_fetch(self, query: str) -> List[DataRow]:
        """
        Like :meth:`~.simple_query`, but returns a list of data rows eagerly loaded rather than
        lazily loaded.
        """
        return [item async for item in self.simple_query(query) if isinstance(item, DataRow)]


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
        protocol = ProtocolMachine(username, database, password)
        conn = AsyncPostgresConnection(sock, protocol)

        await conn._do_startup()
        await conn._wait_until_ready()
        yield conn
