import pytest

from tests.util import open_connection

pytestmark = pytest.mark.anyio


async def test_basic_arrays():
    """
    Tests basic array functionality.
    """
    async with open_connection() as conn:
        await conn.execute("create temp table test_ba_t (arr int4[] primary key);")
        await conn.execute("insert into test_ba_t(arr) values (:arr)", arr=[1, 2, 3])

        data_row = await conn.fetch_one("select * from test_ba_t;")
        assert data_row
        assert data_row[0] == [1, 2, 3]


async def test_string_array():
    """
    Tests string arrays with commas.
    """
    async with open_connection() as conn:
        await conn.execute("create temp table test_sa_t (arr text[] primary key);")
        await conn.execute("insert into test_sa_t values (:arr)", arr=["one, two", "three,"])
        data_row = await conn.fetch_one("select * from test_sa_t;")

        assert data_row
        assert data_row[0] == ["one, two", "three,"]
