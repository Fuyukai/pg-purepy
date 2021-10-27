from __future__ import annotations

import functools
import logging
import os
import warnings
from contextlib import asynccontextmanager
from typing import AsyncContextManager, Awaitable, Callable, List, Optional, Set

import anyio
import attr
from anyio.abc import SocketStream, TaskGroup

from pg_purepy import ArrayConverter
from pg_purepy.connection import AsyncPostgresConnection, _open_connection
from pg_purepy.conversion.abc import Converter
from pg_purepy.exc import ConnectionForciblyKilledError, ConnectionInTransactionWarning
from pg_purepy.messages import DataRow

logger = logging.getLogger(__name__)


@attr.s(slots=True)
class OpenedConnection:
    sock: SocketStream = attr.ib()
    conn: AsyncPostgresConnection = attr.ib()


class PooledDatabaseInterface(object):
    """
    Connection pool based PostgreSQL interface.
    """

    def __init__(self, count: int, nursery: TaskGroup, conn_args, conn_kwargs):
        self._connection_count = count

        self._write, self._read = anyio.create_memory_object_stream(
            max_buffer_size=count, item_type=OpenedConnection
        )

        self._conn_args = conn_args
        self._conn_kwargs = conn_kwargs

        # list of converters used when opening a new connection
        self._converters = set()

        # used to add new converters. non-queue.
        self._raw_connections: Set[AsyncPostgresConnection] = set()

        self._nursery = nursery
        logger.debug(f"Opening {count} connections to the database.")

        for _ in range(0, count):
            partial = functools.partial(self._open_new_connection)
            nursery.start_soon(partial)

    @property
    def max_connections(self) -> int:
        """
        The maximum number of connections this pool may have idle.
        """
        return self._connection_count

    @property
    def idle_connections(self) -> int:
        """
        The number of the connections that haven't currently been checked out.
        """
        return self._read.statistics().current_buffer_used

    @property
    def waiting_tasks(self) -> int:
        """
        The number of tasks that are currently waiting for a connection to be used.
        """
        return self._read.statistics().tasks_waiting_receive

    async def _open_new_connection(self, *args, **kwargs):
        sock, conn = await _open_connection(*self._conn_args, **self._conn_kwargs)  #
        self._raw_connections.add(conn)
        for converter in self._converters:
            conn.add_converter(converter=converter)

        wrapped = OpenedConnection(sock, conn)

        try:
            self._write.send_nowait(wrapped)  # noqa
        except anyio.WouldBlock:
            self._raw_connections.remove(conn)
            raise RuntimeError(
                "Asked to create a new connection, but there's already too many "
                "connections in the queue!"
            )

    async def __aenter__(self):
        return self

    async def _cleanup(self):
        # Cancellation-resistant exit algorithm. This takes inspiration from
        # https://github.com/richardsheridan/trio-parallel/blob/37d0451f632e40fc8fa0bc1646180a83274219a3/trio_parallel/_proc.py#L56-L85
        # using a double shielded nursery setup that will try and gracefully exit the connection,
        # then forcibly disconnect if required.

        gathered = []
        while True:
            try:
                gathered.append(self._read.receive_nowait())
            except anyio.WouldBlock:
                break

        assert len(gathered) <= self._connection_count

        swallowed = []
        forcibly_killed = []

        async def kill(o: OpenedConnection):
            if o.conn.dead:
                try:
                    # This *always* marks the connection as terminated, so even if this fails then
                    # .dead will be True for the second nursery.
                    await o.conn._terminate()  # noqa
                except Exception as e:
                    swallowed.append(e)
                else:
                    # connection successfully exited, we can continue to trying to close the socket
                    # directly.
                    gathered.remove(o)

            # even if we're subsequently cancelled, this will still always close the socket.
            await o.sock.aclose()

        # step 1: close all connections gracefully. this will try and send the Terminate message,
        # then close the socket, sending the TLS close message as appropriate.
        # This is shielded + a deadline so that external cancellations are ignored but it won't
        # deadlock trying to send to a server that is ignoring us.
        async with anyio.create_task_group() as n1:
            n1.cancel_scope.shield = True
            n1.cancel_scope.deadline = anyio.current_time() + 5

            for conn in gathered:
                n1.start_soon(kill, conn)

        # Step 2: if the deadline expired, we then have to just go "fuck it" and forcibly kill
        # everything. The termination message is never sent because all of the connections are
        # considered dead by here.
        if n1.cancel_scope.cancel_called:
            async with anyio.create_task_group() as n2:
                n2.cancel_scope.shield = True

                for conn in gathered:
                    forcibly_killed.append(conn)
                    n2.start_soon(kill, conn)

        # kill any waiting tasks... this should never be a problem but you honestly never know.
        self._read.close()
        self._write.close()

        if not (swallowed or forcibly_killed):
            return False  # don't suppress error inside context manager

        exceptions = [*swallowed]
        if forcibly_killed:
            for i in forcibly_killed:
                exceptions.append(ConnectionForciblyKilledError(i.conn))

        raise anyio.ExceptionGroup(*exceptions)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._cleanup()

    @asynccontextmanager
    async def _checkout_connection(self):
        """
        Checks out a single connection from the connection pool.
        """
        logger.debug("Checking out new connection from the pool...")
        checkout = await self._read.receive()
        logger.debug(
            f"Connection acquired, current unused pool size: "
            f"{self._read.statistics().current_buffer_used}"
        )

        if checkout.conn.in_transaction:
            warnings.warn(
                f"Connection {checkout.conn} still has a transaction open. Forcing rollback."
            )
            await checkout.conn.execute("ROLLBACK;")

        try:
            yield checkout.conn
            if checkout.conn.in_transaction:
                warn = ConnectionInTransactionWarning(
                    f"Connection {checkout.conn} is being checked back in with a transaction open. "
                    f"Forcing rollback."
                )
                warnings.warn(warn, stacklevel=3)
                await checkout.conn.execute("ROLLBACK;")

        finally:
            if checkout.conn.dead:
                logger.warning(
                    f"Connection {checkout.conn} is dead for some reason, scheduling "
                    f"creation of a new connection"
                )
                with anyio.CancelScope(shield=True):
                    try:
                        self._raw_connections.remove(checkout.conn)
                        await checkout.sock.aclose()
                    finally:
                        self._nursery.start_soon(self._open_new_connection)
            else:
                try:
                    self._write.send_nowait(checkout)  # noqa
                except anyio.WouldBlock:
                    raise RuntimeError(
                        "Attempted to send connection back to queue, but there's "
                        "already too many connections in the queue!"
                    )

    ## High-level methods. ##
    @asynccontextmanager
    async def checkout_in_transaction(self):
        """
        Checks out a new connection that automatically runs a transaction. This method MUST be used
        if you wish to execute something in a transaction.
        """
        async with self._checkout_connection() as conn:  # type: AsyncPostgresConnection
            async with conn.with_transaction():
                yield conn

    async def execute(self, query: str, *params, **kwargs) -> int:
        """
        Executes a query on the next available connection. See
        :meth:`.AsyncPostgresConnection.execute` for more information.
        """
        async with self._checkout_connection() as conn:  # type: AsyncPostgresConnection
            return await conn.execute(query, *params, **kwargs)

    async def fetch(self, query: str, *params, **kwargs) -> List[DataRow]:
        """
        Fetches the result of a query on the next available connection. See
        :meth:`.AsyncPostgresConnection.fetch` for more information.
        """
        async with self._checkout_connection() as conn:  # type: AsyncPostgresConnection
            return await conn.fetch(query, *params, **kwargs)

    async def fetch_one(self, query: str, *params, **kwargs) -> Optional[DataRow]:
        """
        Like :meth:`.fetch`, but only returns one row. See
        :meth:`.AsyncPostgresConnection.fetch_one` for more information.
        """
        async with self._checkout_connection() as conn:
            return await conn.fetch_one(query, *params, **kwargs)

    ## Utility Methods ##
    async def find_oid_for_type(self, type_name: str) -> Optional[int]:
        """
        Finds the OID for the type with the specified name.
        """
        row = await self.fetch_one("select oid from pg_type where typname = :name", name=type_name)

        if row is None:
            return None

        return row.data[0]

    def add_converter(self, converter: Converter):
        """
        Registers a converter for all the connections on this pool.
        """
        self._converters.add(converter)

        for conn in self._raw_connections:
            conn.add_converter(converter)

    async def add_converter_using(
        self, fn: Callable[[AsyncPostgresConnection], Awaitable[Optional[Converter]]]
    ):
        """
        Adds a converter using the specified async function. Useful primarily for extension types
        where the oids aren't fixed.
        """
        async with self._checkout_connection() as conn:
            converter = await fn(conn)

        if converter is not None:
            self.add_converter(converter)

    async def add_converter_with_array(self, converter: Converter, **kwargs):
        """
        Registers a converter, and adds the array type converter to it too.
        """
        self.add_converter(converter)

        async with self._checkout_connection() as conn:  # type: AsyncPostgresConnection
            row = await conn.fetch_one(
                "select typarray::oid from pg_type where oid = :oid", oid=converter.oid
            )
            if row is None:
                return

            arrcv = ArrayConverter(oid=row[0], subconverter=converter, **kwargs)
            self.add_converter(arrcv)


def determine_conn_count():
    """
    Determines the appropriate default connection count.
    """
    return (os.cpu_count() * 2) + 1


@asynccontextmanager
async def open_pool(
    connection_count: int = determine_conn_count(), *args, **kwargs
) -> AsyncContextManager[PooledDatabaseInterface]:
    """
    Opens a new connection pool to a PostgreSQL server. This is an asynchronous context manager.

    This takes the same arguments and keyworrd arguments as
    :func:`.open_database_connection`, except for the optional
    ``connection_count`` parameter.

    :param connection_count: The ideal number of connections to keep open at any one time. The
                             pool may shrink slightly as connections are closed due to network
                             errors and aren't immediately re-opened.

    By default, the connection count is (CPU_COUNT * 2) + 1.
    """

    async with anyio.create_task_group() as tg, PooledDatabaseInterface(
        connection_count, tg, args, kwargs
    ) as pool:
        yield pool
