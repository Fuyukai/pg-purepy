from itertools import product

import pytest

from tests.util import open_connection

pytestmark = pytest.mark.anyio


JSON_TYPES = ["json", "jsonb"]
JSON_EXAMPLES = [('{"abc": "def"}', {"abc": "def"}), ("[1, 2, 3]", [1, 2, 3])]


async def test_loading_json():
    async with open_connection() as conn:
        for type_, (text, expected) in product(JSON_TYPES, JSON_EXAMPLES):
            result = await conn.fetch_one(f"select '{text}'::{type_}")
            assert result
            assert result.data[0] == expected


async def test_inserting_json():
    async with open_connection() as conn:
        await conn.execute("create temp table test (j jsonb primary key);")
        await conn.execute("insert into test (j) values ($1)", {"key": "value"})
        res = await conn.fetch_one("select j from test;")
        assert res
        assert res.data[0] == {"key": "value"}
