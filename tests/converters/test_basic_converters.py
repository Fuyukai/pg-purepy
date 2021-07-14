import pytest

from tests.util import open_connection

pytestmark = pytest.mark.anyio


async def test_converters_int():
    """
    Tests int converters.
    """
    async with open_connection() as conn:
        for cast in (
            "int2",
            "int4",
            "int8",
            "oid",
        ):
            result_out = await conn.fetch(f"select 1::{cast};")
            assert result_out[0].data[0] == 1

            await conn.execute(f"create temp table test_int_{cast} (id {cast} primary key);")
            await conn.execute(f"insert into test_int_{cast} values ($1);", 39)
            result_in = await conn.fetch_one(f"select id from test_int_{cast};")
            assert result_in.data[0] == 39


async def test_converter_bool():
    """
    Tests the boolean converter.
    """
    async with open_connection() as conn:
        true = await conn.fetch_one("select true;")
        assert true.data[0] is True

        false = await conn.fetch_one("select false;")
        assert false.data[0] is False

        await conn.execute("create temp table test_bool (id int primary key, value boolean);")
        await conn.execute(
            "insert into test_bool(id, value) values (1, :t), (2, :f);", t=True, f=False
        )
        rows = await conn.fetch("select value from test_bool;")
        assert rows[0].data[0] is True
        assert rows[1].data[0] is False


async def test_converter_bytea():
    """
    Tests the bytea converter.
    """
    async with open_connection() as conn:
        # trick: both in and out conversion
        ba = await conn.fetch_one(r"select $1::bytea;", b"\x00\x01")
        assert isinstance(ba.data[0], bytes)
        assert ba.data[0] == b"\x00\x01"
