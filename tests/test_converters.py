from tests.util import open_connection


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
            result = await conn.fetch(f"select 1::{cast};")
            assert result[0].data[0] == 1


async def test_converter_bool():
    """
    Tests the boolean converter.
    """
    async with open_connection() as conn:
        true = await conn.fetch("select true;")
        assert true[0].data[0] is True

        false = await conn.fetch("select false;")
        assert false[0].data[0] is False
