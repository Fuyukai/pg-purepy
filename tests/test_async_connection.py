import logging
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
    RecoverableDatabaseError,
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


async def test_basic_select():
    """
    Tests a basic SELECT statement.
    """
    async with open_connection() as conn:
        # fun fact: select null; returns text.
        result = [i async for i in conn.lowlevel_query("select null;")]
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
        with pytest.raises(RecoverableDatabaseError) as e:
            _ = await conn.fetch("select * from nonexistent;")

        assert e.value.response.code == "42P01"

        result = await conn.fetch("select 1;")
        assert len(result) == 1

        row = result[0]
        assert isinstance(row, DataRow)
        assert len(row.data) == 1
        assert row.data[0] == 1


async def test_query_with_params():
    """
    Tests running a query with parameters.
    """
    async with open_connection() as conn:
        result = await conn.fetch("select 1 where 'a' = :x;", x="a")

        assert len(result) == 1
        row = result[0]
        assert isinstance(row, DataRow)
        assert row.data[0] == 1


async def test_transaction_status_manual():
    """
    Tests getting the transaction status during a query.
    """

    async with open_connection() as conn:
        await conn.execute("begin;")
        assert conn.in_transaction
        await conn.execute("rollback;")
        assert not conn.in_transaction


async def test_transaction_helper_normal():
    """
    Tests the transaction helper in a normal situation.
    """
    async with open_connection() as conn:
        async with conn.with_transaction():
            assert conn.in_transaction
            await conn.execute(
                "create temp table test_transaction_helper_normal (id int primary key);"
            )

        assert not conn.in_transaction
        result = await conn.fetch(
            "select count(*) from pg_tables where tablename = :name;",
            name="test_transaction_helper_normal",
        )

        assert result[0].data[0] == 1


async def test_transaction_helper_error():
    """
    Tests the transaction helper in an error situation.
    """
    async with open_connection() as conn:
        with pytest.raises(ValueError):
            async with conn.with_transaction():
                assert conn.in_transaction
                await conn.execute(
                    "create temp table test_transaction_helper_error (id int primary key);"
                )
                raise ValueError()

        assert not conn.in_transaction
        result = await conn.fetch(
            "select count(*) from pg_tables where tablename = :name;",
            name="test_transaction_helper_normal",
        )

        assert result[0].data[0] == 0
