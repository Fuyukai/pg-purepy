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

            result_in = await conn.fetch(f"select $1::{cast};", 2)
            assert result_in[0].data[0] == 2


async def test_converter_bool():
    """
    Tests the boolean converter.
    """
    async with open_connection() as conn:
        true = await conn.fetch("select true;")
        assert true[0].data[0] is True

        false = await conn.fetch("select false;")
        assert false[0].data[0] is False


async def test_converter_bytea():
    """
    Tests the bytea converter.
    """
    async with open_connection() as conn:
        # trick: both in and out conversion
        ba = (await conn.fetch(r"select $1::bytea;", b"\x00\x01"))[0]
        assert isinstance(ba.data[0], bytes)
        assert ba.data[0] == b"\x00\x01"
