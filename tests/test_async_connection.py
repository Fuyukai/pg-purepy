import os
from contextlib import asynccontextmanager
from typing import AsyncContextManager

import pytest
from pg_purepy import (
    MissingPasswordError,
    InvalidPasswordError,
    RowDescription,
    DataRow,
    CommandComplete,
    DatabaseError,
)
from pg_purepy.connection import open_database_connection, AsyncPostgresConnection

from tests.util import open_connection, POSTGRES_ADDRESS, POSTGRES_USERNAME


async def test_basic_connection():
    """
    Tests opening a basic connection to the PostgreSQL server.
    """
    async with open_connection() as conn:
        assert conn.ready


async def test_connection_with_invalid_password():
    """
    Tests opening a connection with an invalid password.
    """
    with pytest.raises(InvalidPasswordError):
        async with open_database_connection(
            address_or_path=POSTGRES_ADDRESS, username=POSTGRES_USERNAME, password=""
        ) as conn:
            pass


async def test_needs_password():
    """
    Tests opening a connection where a password is needed, but none is provided.
    """
    with pytest.raises(MissingPasswordError):
        async with open_database_connection(
            address_or_path=POSTGRES_ADDRESS,
            username=POSTGRES_USERNAME,
        ) as conn:
            pass

            async with conn.execute_bound(
                "select * from whatever where row = $0;", "something"
            ) as query:
                async for row in query:
                    ...

            async with conn.named_query("select * from whatever where row = $0;") as query:
                binder = query.binder()
                binder.bind(0, "something")
                async with binder.execute() as result:
                    async for row in result:
                        ...


async def test_basic_select():
    """
    Tests a basic SELECT statement.
    """
    async with open_connection() as conn:
        # fun fact: select null; returns text.
        result = [i async for i in conn.simple_query("select null;")]
        assert len(result) == 3
        desc, row, count = result

        assert isinstance(desc, RowDescription)
        assert len(desc.columns) == 1
        assert desc.columns[0].name == "?column?"

        assert isinstance(row, DataRow)
        assert len(row.data) == 1
        assert row.data[0] is None

        assert isinstance(count, CommandComplete)
        assert count.row_count == 1


async def test_query_after_error():
    """
    Tests running a second query after a first query raises an error.
    """
    async with open_connection() as conn:
        with pytest.raises(DatabaseError) as e:
            async for _ in conn.simple_query("select * from nonexistent;"):
                pass

        assert e.value.response.code == "42P01"

        result = [i async for i in conn.simple_query("select 1;")]
        row = result[1]
        assert isinstance(row, DataRow)
        assert len(row.data) == 1
        assert row.data[0] == 1


async def test_multiple_queries_one_message():
    """
    Tests running multiple queries in one message.
    """
    async with open_connection() as conn:
        result = [i async for i in conn.simple_query("select 1; select 2;")]

        assert len(result) == 6
        row1, row2 = result[1], result[4]

        assert isinstance(row1, DataRow)
        assert isinstance(row2, DataRow)
        assert row1.data[0] == 1
        assert row2.data[0] == 2
