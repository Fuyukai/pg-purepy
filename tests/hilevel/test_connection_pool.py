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


async def test_transactions():
    """
    Tests that transactions are automatically rolled back.
    """
    async with open_pool(conn_count=1) as p:
        with pytest.warns(ConnectionInTransactionWarning):
            await p.execute("begin;")
