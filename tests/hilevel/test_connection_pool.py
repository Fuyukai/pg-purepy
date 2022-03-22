import anyio
import pytest

from pg_purepy import ConnectionInTransactionWarning, UnrecoverableDatabaseError
from tests.hilevel import open_pool

pytestmark = pytest.mark.anyio


async def test_successful_pool_usage():
    """
    Tests a successful pool usage.
    """

    results = set()

    async with open_pool(conn_count=3) as p:
        async def execute(num: int):
            # the sleep ensures that the connections actually run in parallel, as trio will
            # schedule the tasks before that sleep expires.
            result = await p.fetch_one(f"select pg_sleep(0.25), {num};")
            results.add(result.data[1])

        async with anyio.create_task_group() as nursery:
            for i in range(0, 3):
                nursery.start_soon(execute, i + 1)

        assert results == {1, 2, 3}


async def test_automatic_reconnection():
    """
    Tests the pool automatically reconnecting dead connections.
    """

    async with open_pool(conn_count=1) as p:
        with pytest.raises(UnrecoverableDatabaseError) as e:
            await p.execute("select pg_terminate_backend(pg_backend_pid());")

        assert e.value.response.code == "57P01"

        assert (await p.execute("select 2;")) == 1


async def test_pool_transactions():
    """
    Tests connection pool transactions.
    """

    async with open_pool(conn_count=1) as p:
        with pytest.raises(RuntimeError):
            async with p.checkout_in_transaction() as conn:
                assert conn.in_transaction
                await conn.execute(
                    "create table test_transaction_helper_error (id int primary key);"
                )
                raise RuntimeError()

            result = await p.fetch_one(
                "select count(*) from pg_tables where tablename = :name;",
                name="test_transaction_helper_error",
            )

            assert result.data[0] == 0


async def test_transaction_rollback():
    """
    Tests that transactions are automatically rolled back.
    """

    async with open_pool(conn_count=1) as p:
        with pytest.warns(ConnectionInTransactionWarning):
            await p.execute("begin;")


async def test_cancellation():
    """
    Tests query cancellation integration.
    """

    async with open_pool(conn_count=1) as p:
        # timeout block causes an automatic cancel after a second, which (should) send the
        # cancellation request.
        with anyio.move_on_after(1.0):
            await p.execute("select pg_sleep_for('1 minutes');")

        # longer cooldown but still less than a minutee to let the cancellation happen
        with anyio.move_on_after(5.0) as s:
            await p.execute("select 1;")

        assert not s.cancel_called


async def test_cancellation_with_transactions():
    """
    Tests cancellation alongside transactions.
    """

    async with open_pool(conn_count=1) as p:
        with anyio.move_on_after(1.0):
            async with p.checkout_in_transaction() as conn:
                await conn.execute("select pg_sleep_for('1 minutes');")

        # eww
        assert not next(iter(p._raw_connections)).in_transaction
