import pytest
import trio.testing
from async_generator import aclosing
from pg_purepy import (
    MissingPasswordError,
    InvalidPasswordError,
    RowDescription,
    DataRow,
    CommandComplete,
    RecoverableDatabaseError,
)
from pg_purepy.connection import open_database_connection, QueryResult

from tests.util import open_connection, POSTGRES_ADDRESS, POSTGRES_USERNAME

pytestmark = pytest.mark.anyio


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
        # simple queries
        with pytest.raises(RecoverableDatabaseError) as e1:
            _ = await conn.fetch("select * from nonexistent;")

        assert e1.value.response.code == "42P01"

        result_1 = await conn.fetch("select 1;")
        assert len(result_1) == 1

        row_1 = result_1[0]
        assert isinstance(row_1, DataRow)
        assert len(row_1.data) == 1
        assert row_1.data[0] == 1

        with pytest.raises(RecoverableDatabaseError) as e2:
            await conn.fetch("select * from nonexistent where 'a' = :a;", a="a")

        assert e2.value.response.code == "42P01"

        result_2 = await conn.fetch("select 2 where 'a' = :a;", a="a")
        assert len(result_2) == 1

        row_2 = result_2[0]
        assert isinstance(row_2, DataRow)
        assert len(row_2.data) == 1
        assert row_2.data[0] == 2


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


async def test_get_row_count_no_rowcount():
    """
    Tests getting the row count of a command that doesn't return a row count.
    """
    async with open_connection() as conn:
        row_count = await conn.execute("checkpoint;")
        assert row_count == 0


async def test_multiple_queries_one_execute():
    """
    Tests executing multiple queries in one execute.
    """
    async with open_connection() as conn:
        async with aclosing(conn.lowlevel_query("select 1; select 2;")) as agen:
            results = [i async for i in agen]

        assert len(results) == 6
        row1, row2 = results[1], results[4]

        assert row1.data[0] == 1
        assert row2.data[0] == 2


## Transaction helper ##
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
            name="test_transaction_helper_error",
        )

        assert result[0].data[0] == 0


## Prepared statements ##
async def test_execute_prepared_statement_insert():
    """
    Tests executing a prepared statement with parameters.
    """
    async with open_connection() as conn:
        await conn.execute(
            "create temp table test_epsi (id serial primary key, foo text not null);"
        )

        st_no_params = await conn.create_prepared_statement(
            name="test_epsp_1", query="insert into test_epsi(foo) values ('one');"
        )
        rows_no_params = await conn.execute(st_no_params)
        assert rows_no_params == 1

        st_with_params = await conn.create_prepared_statement(
            name="test_epsp_2", query="insert into test_epsi(foo) values ($1);"
        )
        rows_params = await conn.execute(st_with_params, "two")
        assert rows_params == 1

        count = await conn.fetch("select count(*) from test_epsi;")
        assert len(count) == 1
        assert count[0].data[0] == 2

        rows = await conn.fetch("select * from test_epsi;")
        assert len(rows) == 2
        assert rows[0].data[1] == "one"
        assert rows[1].data[1] == "two"


## Specific subcommands ##
async def test_insert():
    """
    Tests inserting data into a table.
    """
    async with open_connection() as conn:
        await conn.execute(
            "create temp table test_insert (id serial primary key, foo text not null);"
        )
        row_count = await conn.execute("insert into test_insert(foo) values (:one);", one="test")
        assert row_count == 1
        result = await conn.fetch("select * from test_insert;")
        assert len(result) == 1
        assert result[0].data == [1, "test"]


async def test_unparameterised_insert():
    """
    Tests inserting static data.
    """

    async with open_connection() as conn:
        await conn.execute(
            "create temp table test_insert2 (id serial primary key, foo text not null);"
        )
        row_count = await conn.execute("insert into test_insert2(foo) values ('test');")
        assert row_count == 1
        result = await conn.fetch("select * from test_insert2;")
        assert len(result) == 1
        assert result[0].data == [1, "test"]


async def test_update():
    """
    Tests updating data in a table.
    """
    async with open_connection() as conn:
        await conn.execute(
            "create temp table test_update (id serial primary key, foo text not null);"
        )
        await conn.execute("insert into test_update(foo) values (:one);", one="test")
        pre_update = await conn.fetch("select * from test_update;")
        assert len(pre_update) == 1
        assert pre_update[0].data == [1, "test"]

        row_count = await conn.execute("update test_update set foo = :one;", one="test_2")
        assert row_count == 1
        post_update = await conn.fetch("select * from test_update;")
        assert len(post_update) == 1
        assert post_update[0].data == [1, "test_2"]


async def test_delete():
    """
    Tests deleting data in a table.
    """
    async with open_connection() as conn:
        await conn.execute(
            "create temp table test_delete (id serial primary key, foo text not null);"
        )
        await conn.execute("insert into test_delete(foo) values (:one);", one="test")
        pre_delete = await conn.fetch("select * from test_delete;")
        assert len(pre_delete) == 1
        assert pre_delete[0].data == [1, "test"]

        row_count = await conn.execute("delete from test_delete;")
        assert row_count == 1

        post_delete = await conn.fetch("select * from test_delete;")
        assert len(post_delete) == 0


## Notices ##
async def test_notices():
    """
    Tests handling notices in the stream.
    """
    async with open_connection() as conn:
        with pytest.warns(UserWarning):
            await conn.execute(
                "DO language plpgsql $$ BEGIN RAISE WARNING 'hello, world!'; END $$;"
            )

        with pytest.warns(None) as w:
            await conn.execute(
                "DO language plpgsql $$ BEGIN RAISE NOTICE 'hello, world!'; END $$;"
            )

        assert not w


## Misc ##
async def test_get_cached_row_count(anyio_backend):
    """
    Tests that getting the cached row count works.
    """
    async with open_connection() as conn:
        async with conn.query("select 1;") as query:  # type: QueryResult
            rows = [r async for r in query]
            assert rows[0].data[0] == 1

            assert await query.row_count() == 1

            if anyio_backend == "trio":
                with trio.testing.assert_checkpoints():
                    assert await query.row_count() == 1


async def test_insert_into_not_null():
    """
    Tests inserting into a not-null table.
    """
    async with open_connection() as conn:
        await conn.execute("create temp table test (id int primary key);")

        with pytest.raises(RecoverableDatabaseError) as e:
            await conn.execute("insert into test(id) values (:n);", n=None)

        assert e.value.response.code == "23502"
