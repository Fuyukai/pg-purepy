from __future__ import annotations

import logging
import os
import struct
import warnings
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, Literal, Self

import anyio
import attr
from anyio.abc import SocketStream, TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from anyio.streams.tls import TLSStream

from pg_purepy import ArrayConverter
from pg_purepy.connection import (
    AsyncPostgresConnection,
    RollbackTimeoutError,
    _open_connection,
    _open_socket,
)
from pg_purepy.conversion.abc import Converter
from pg_purepy.exc import ConnectionForciblyKilledError, ConnectionInTransactionWarning
from pg_purepy.messages import (
    DataRow,
    RecoverableDatabaseError,
)

logger = logging.getLogger(__name__)


@attr.s(slots=True)
class OpenedConnection:
    sock: SocketStream | TLSStream = attr.ib()
    conn: AsyncPostgresConnection = attr.ib()


class PooledDatabaseInterface:
    """
    Connection pool based PostgreSQL interface.
    """

    def __init__(self, count: int, nursery: TaskGroup, conn_args: Any, conn_kwargs: Any):
        self._connection_count = count

        self._write: MemoryObjectSendStream[OpenedConnection]
        self._read: MemoryObjectReceiveStream[OpenedConnection]

        self._write, self._read = anyio.create_memory_object_stream[OpenedConnection](
            max_buffer_size=count
        )

        self._conn_args = conn_args
        self._conn_kwargs = conn_kwargs

        # list of converters used when opening a new connection
        self._converters: set[Converter] = set()

        # used to add new converters. non-queue.
        self._raw_connections: set[AsyncPostgresConnection] = set()

        self._nursery = nursery

    async def _start(self, count: int) -> None:
        logger.debug(f"Opening {count} connections to the database.")

        for _ in range(0, count):
            await self._open_new_connection()

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

    @staticmethod
    async def _cancel_query(conn: AsyncPostgresConnection) -> None:
        """
        Cancels the query running on this connection.
        """

        logger.debug(f"Cancelling query running on #{conn}")

        sock = await _open_socket(conn._addr, port=conn._port, ssl_context=conn._ssl_context)

        async with sock:
            # yay magic numbers
            data = struct.pack(">IIII", 16, 80877102, conn._pid, conn._secret_key)
            await sock.send(data)

    async def _open_new_connection(self) -> None:
        sock, conn = await _open_connection(*self._conn_args, **self._conn_kwargs)
        self._raw_connections.add(conn)
        for converter in self._converters:
            conn.add_converter(converter=converter)

        wrapped = OpenedConnection(sock, conn)

        try:
            self._write.send_nowait(wrapped)
        except anyio.WouldBlock:
            self._raw_connections.remove(conn)
            raise RuntimeError(
                "Asked to create a new connection, but there's already too many "
                "connections in the queue!"
            ) from None

    async def __aenter__(self) -> Self:
        return self

    async def _cleanup(self) -> Literal[False]:
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

        async def kill(o: OpenedConnection) -> None:
            if o.conn.dead:
                try:
                    # This *always* marks the connection as terminated, so even if this fails then
                    # .dead will be True for the second nursery.
                    await o.conn._terminate()
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

        exceptions: list[Exception] = [*swallowed]
        if forcibly_killed:
            exceptions += [ConnectionForciblyKilledError(i.conn) for i in forcibly_killed]

        raise ExceptionGroup("Error when closing down connection pool!", exceptions)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return await self._cleanup()

    @asynccontextmanager
    async def _checkout_connection(
        self, start_new_transaction: bool = False
    ) -> AsyncGenerator[AsyncPostgresConnection, None]:
        """
        Checks out a single connection from the connection pool.
        """

        logger.debug("Checking out new connection from the pool...")
        checkout = await self._read.receive()
        logger.debug(
            "Connection acquired, current unused pool size: "
            f"{self._read.statistics().current_buffer_used}"
        )

        if checkout.conn.in_transaction:
            warnings.warn(
                f"Connection {checkout.conn} still has a transaction open. Forcing rollback.",
                stacklevel=2,
            )
            await checkout.conn._safely_rollback(None)

        try:
            if start_new_transaction:
                logger.debug("Starting new transaction...")
                await checkout.conn.execute("BEGIN;")

            yield checkout.conn

            if start_new_transaction:
                await checkout.conn.execute("COMMIT;")

            if checkout.conn.in_transaction:
                warn = ConnectionInTransactionWarning(
                    f"Connection {checkout.conn} is being checked back in with a transaction open. "
                    "Forcing rollback."
                )
                warnings.warn(warn, stacklevel=3)
                await checkout.conn._safely_rollback(None)

        except anyio.get_cancelled_exc_class():
            # open a new conn to psql and cancel the query
            with anyio.move_on_after(delay=5.0, shield=True) as scope:
                await self._cancel_query(conn=checkout.conn)

                try:
                    await checkout.conn.wait_until_ready()
                except RecoverableDatabaseError as e:
                    if e.response.code == "57014":
                        pass
                    else:
                        raise

            if scope.cancel_called:
                logger.warning(f"Failed to cancel query running on {checkout.conn}")

            raise

        finally:
            rollback_failed = None
            if checkout.conn.in_transaction:
                try:
                    await checkout.conn._safely_rollback(None)
                except RollbackTimeoutError as e:
                    rollback_failed = e

            if checkout.conn.dead:
                logger.warning(
                    f"Connection {checkout.conn} is disconnected, scheduling "
                    "creation of a new connection"
                )
                with anyio.CancelScope(shield=True):
                    try:
                        self._raw_connections.remove(checkout.conn)
                        await checkout.sock.aclose()
                    finally:
                        self._nursery.start_soon(self._open_new_connection)
            else:
                try:
                    self._write.send_nowait(checkout)
                except anyio.WouldBlock:
                    raise RuntimeError(
                        "Attempted to send connection back to queue, but there's "
                        "already too many connections in the queue!"
                    ) from None

            if rollback_failed:
                raise rollback_failed

    ## High-level methods. ##
    @asynccontextmanager
    async def checkout_in_transaction(self) -> AsyncGenerator[AsyncPostgresConnection, None]:
        """
        Checks out a new connection that automatically runs a transaction. This method MUST be used
        if you wish to execute something in a transaction.
        """

        async with self._checkout_connection(start_new_transaction=True) as conn:
            yield conn

    async def execute(self, query: str, *params: Any, **kwargs: Any) -> int:
        """
        Executes a query on the next available connection. See
        :meth:`.AsyncPostgresConnection.execute` for more information.
        """

        async with self._checkout_connection() as conn:
            return await conn.execute(query, *params, **kwargs)

    async def fetch(self, query: str, *params: Any, **kwargs: Any) -> list[DataRow]:
        """
        Fetches the result of a query on the next available connection. See
        :meth:`.AsyncPostgresConnection.fetch` for more information.
        """

        async with self._checkout_connection() as conn:
            return await conn.fetch(query, *params, **kwargs)

    async def fetch_one(self, query: str, *params: Any, **kwargs: Any) -> DataRow | None:
        """
        Like :meth:`.fetch`, but only returns one row. See
        :meth:`.AsyncPostgresConnection.fetch_one` for more information.
        """

        async with self._checkout_connection() as conn:
            return await conn.fetch_one(query, *params, **kwargs)

    ## Utility Methods ##
    async def find_oid_for_type(self, type_name: str) -> int | None:
        """
        Finds the OID for the type with the specified name.
        """

        row = await self.fetch_one("select oid from pg_type where typname = :name", name=type_name)

        if row is None:
            return None

        return row.data[0]

    def add_converter(self, converter: Converter) -> None:
        """
        Registers a converter for all the connections on this pool.
        """
        self._converters.add(converter)

        for conn in self._raw_connections:
            conn.add_converter(converter)

    async def add_converter_using(
        self, fn: Callable[[AsyncPostgresConnection], Awaitable[Converter | None]]
    ) -> None:
        """
        Adds a converter using the specified async function. Useful primarily for extension types
        where the oids aren't fixed.
        """

        async with self._checkout_connection() as conn:
            converter = await fn(conn)

        if converter is not None:
            self.add_converter(converter)

    async def add_converter_with_array(self, converter: Converter, **kwargs: Any) -> None:
        """
        Registers a converter, and adds the array type converter to it too.
        """

        self.add_converter(converter)

        async with self._checkout_connection() as conn:
            row = await conn.fetch_one(
                "select typarray::oid from pg_type where oid = :oid", oid=converter.oid
            )
            if row is None:
                return

            arrcv = ArrayConverter(oid=row[0], subconverter=converter, **kwargs)  # type: ignore
            self.add_converter(arrcv)


def determine_conn_count() -> int:
    """
    Determines the appropriate default connection count.
    """

    if count := os.cpu_count():
        return (count * 2) + 1

    return 2  # fuck it.


@asynccontextmanager
async def open_pool(
    connection_count: int | None = None, *args: Any, **kwargs: Any
) -> AsyncGenerator[PooledDatabaseInterface, None]:
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

    if connection_count is None:
        connection_count = determine_conn_count()

    async with anyio.create_task_group() as tg, PooledDatabaseInterface(
        connection_count, tg, args, kwargs
    ) as pool:
        await pool._start(connection_count)

        yield pool
