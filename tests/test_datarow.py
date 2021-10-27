import pytest

from tests.util import open_connection

pytestmark = pytest.mark.anyio


async def test_data_row_to_dict():
    async with open_connection() as conn:
        row = await conn.fetch_one('select 1 as "column";')
        row = row.to_dict()
        assert "column" in row
        assert row["column"] == 1
