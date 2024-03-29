import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from pg_purepy import AsyncPostgresConnection, open_database_connection

POSTGRES_ADDRESS = os.environ.get("POSTGRES_ADDRESS", "127.0.0.1")
POSTGRES_PORT: int = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USERNAME = os.environ.get("POSTGRES_USERNAME", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")  # lol!
POSTGRES_DATABASE = os.environ.get("POSTGRES_DATABASE", "postgres")


@asynccontextmanager
async def open_connection() -> AsyncGenerator[AsyncPostgresConnection, None]:
    async with open_database_connection(
        address_or_path=POSTGRES_ADDRESS,
        port=POSTGRES_PORT,
        username=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
    ) as conn:
        yield conn
