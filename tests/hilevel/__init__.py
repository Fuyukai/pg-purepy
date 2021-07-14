from contextlib import asynccontextmanager
from typing import AsyncContextManager

from pg_purepy.pool import PooledDatabaseInterface
from pg_purepy.pool import open_pool as _open_pool
from tests.util import POSTGRES_ADDRESS, POSTGRES_PASSWORD, POSTGRES_USERNAME


@asynccontextmanager
async def open_pool(conn_count: int = None) -> AsyncContextManager[PooledDatabaseInterface]:
    async with _open_pool(
        connection_count=conn_count,
        address_or_path=POSTGRES_ADDRESS,
        username=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
    ) as c:
        yield c
