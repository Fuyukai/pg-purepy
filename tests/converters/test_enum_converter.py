from enum import Enum

import pytest
from pg_purepy.conversion.enums import EnumConverter

from tests.util import open_connection

pytestmark = pytest.mark.anyio


class StandardEnum(Enum):
    ONE = 1
    TWO = 2
    THREE = 3


class StringEnum(str, Enum):
    THREE = "one"
    ONE = "two"
    TWO = "three"


async def test_basic_enum_conversion():
    """
    Tests basic enum conversion.
    """

    async with open_connection() as conn:
        await conn.execute("drop type if exists test_bec;")
        await conn.execute("create type test_bec as enum ('one', 'two', 'three');")
        await conn.execute("create temp table test_bec_t (id test_bec primary key);")

        oid_row = await conn.fetch_one("select oid from pg_type where typname = 'test_bec';")
        assert oid_row
        oid = oid_row.data[0]
        assert oid
        converter = EnumConverter(oid, StandardEnum)
        conn.add_converter(converter)

        # ok now for the actual fun stuff
        await conn.execute("insert into test_bec_t(id) values (:one);", one=StandardEnum.ONE)
        data_row = await conn.fetch_one("select id from test_bec_t;")
        assert data_row
        assert data_row.data[0] == StandardEnum.ONE


async def test_string_member_enum_conversion():
    """
    Tests enum conversion using string members.
    """

    async with open_connection() as conn:
        await conn.execute("drop type if exists test_smec;")
        await conn.execute("create type test_smec as enum ('one', 'two', 'three');")
        await conn.execute("create temp table test_smec_t (id test_smec primary key);")

        oid_row = await conn.fetch_one("select oid from pg_type where typname = 'test_smec';")
        assert oid_row
        oid = oid_row.data[0]
        assert oid
        converter = EnumConverter(oid, StringEnum, use_member_values=True)
        conn.add_converter(converter)

        await conn.execute("insert into test_smec_t(id) values ('one');")
        data_row = await conn.fetch_one("select id from test_smec_t;")
        assert data_row
        assert data_row.data[0] == StringEnum.THREE
