from enum import Enum

import pytest

from pg_purepy import EnumConverter, UnrecoverableDatabaseError
from tests.hilevel import open_pool

pytestmark = pytest.mark.anyio


class TestEnum(Enum):
    ONE = "one"
    TWO = "two"
    THREE = "three"


async def test_get_type_oid():
    """
    Tests getting a type OID.
    """
    async with open_pool(conn_count=1) as pool:
        oid = await pool.find_oid_for_type("int4")
        assert oid == 23


async def test_adding_converter():
    """
    Tests adding a converter to connections.
    """
    async with open_pool(conn_count=1) as pool:
        await pool.execute("drop type if exists test_ace_t;")
        await pool.execute("create type test_ace_t as enum ('one', 'two', 'three');")
        oid = await pool.find_oid_for_type("test_ace_t")
        converter = EnumConverter(oid, TestEnum)

        pool.add_converter(converter)

        row_1 = await pool.fetch_one("select 'one'::test_ace_t;")
        assert row_1[0] == TestEnum.ONE

        # kill connection, to test that the converter is added after reconnect
        with pytest.raises(UnrecoverableDatabaseError) as e:
            await pool.execute("select pg_terminate_backend(pg_backend_pid());")

        assert e.value.response.code == "57P01"
        row_2 = await pool.fetch_one("select 'two'::test_ace_t;")
        assert row_2[0] == TestEnum.TWO


async def test_adding_converter_and_array():
    """
    Tests adding a converter and its array converter at the same time.
    """
    async with open_pool(conn_count=1) as pool:
        await pool.execute("drop type if exists test_acaa;")
        await pool.execute("create type test_acaa as enum ('one', 'two', 'three');")

        oid_row = await pool.fetch_one("select oid from pg_type where typname = 'test_acaa';")
        oid = oid_row.data[0]
        converter = EnumConverter(oid, TestEnum)
        pool.add_converter(converter)

        result_1 = await pool.fetch_one("select '{\"one\"}'::test_acaa[];")
        assert result_1[0] == "{one}"

        # safe, this uses a set and will just overwrite the old converter cleanly.
        await pool.add_converter_with_array(converter)
        result_2 = await pool.fetch_one("select '{\"one\"}'::test_acaa[];")
        assert result_2[0] == [TestEnum.ONE]
