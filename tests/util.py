import os
from contextlib import asynccontextmanager
from typing import AsyncContextManager

from pg_purepy import AsyncPostgresConnection, open_database_connection
from pg_purepy.pool import PooledDatabaseInterface, open_pool as _open_pool

POSTGRES_ADDRESS = os.environ.get("POSTGRES_ADDRESS", "127.0.0.1")
POSTGRES_USERNAME = os.environ.get("POSTGRES_USERNAME", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "password")  # lol!


@asynccontextmanager
async def open_connection() -> AsyncContextManager[AsyncPostgresConnection]:
    async with open_database_connection(
        address_or_path=POSTGRES_ADDRESS, username=POSTGRES_USERNAME, password=POSTGRES_PASSWORD
    ) as conn:
        yield conn


@asynccontextmanager
async def open_pool(conn_count: int = None) -> AsyncContextManager[PooledDatabaseInterface]:
    async with _open_pool(
        connection_count=conn_count,
        address_or_path=POSTGRES_ADDRESS,
        username=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
    ) as c:
        yield c
