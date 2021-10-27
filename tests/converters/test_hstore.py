import pytest

from pg_purepy.conversion.hstore import get_hstore_converter
from tests.util import open_connection

pytestmark = pytest.mark.anyio


async def test_hstore():
    """
    Tests conversion for the hstore type.
    """

    async with open_connection() as conn:
        await conn.execute("create extension if not exists hstore;")
        converter = await get_hstore_converter(conn)
        conn.add_converter(converter)

        result_1 = await conn.fetch_one("select 'a=>b'::hstore;")
        assert result_1.data == [{"a": "b"}]

        result_2 = await conn.fetch_one("select $1::hstore;", {"c": "d"})
        assert result_2.data == [{"c": "d"}]
