"""
Tests date/time related converters.
"""

import datetime
from typing import cast

import pytest
from whenever import OffsetDateTime

from tests.util import open_connection

pytestmark = pytest.mark.anyio


async def test_timestamptz_converter():
    """
    Tests converting to/from a TIMESTAMP WITH TIMEZONE.
    """
    async with open_connection() as conn:
        await conn.execute("set timezone to 'UTC';")
        assert conn.server_timezone == "UTC"

        await conn.execute("create temp table test_ttz_in (id int primary key, dt timestamptz);")
        juno = OffsetDateTime.parse_rfc3339("2011-08-05T16:25:00Z")
        # assert juno.zon == conn.server_timezone

        await conn.execute(
            "insert into test_ttz_in(id, dt) values (1, :dt);",
            dt=OffsetDateTime.parse_rfc3339("2011-08-05T16:25:00Z"),
        )

        row_1 = await conn.fetch_one("select dt from test_ttz_in;")
        assert row_1
        assert isinstance(row_1.data[0], OffsetDateTime)
        assert row_1.data[0] == juno

        await conn.execute("set timezone to 'Europe/Helsinki';")
        row_2 = await conn.fetch_one("select dt from test_ttz_in;")
        assert row_2

        dt = cast(OffsetDateTime, row_2.data[0])
        assert dt.hour == 19
        assert dt.hour != juno.hour
        assert dt == juno


async def test_date_converter():
    """
    Tests converting to/from a DATE.
    """
    async with open_connection() as conn:
        await conn.execute("create temp table test_dci (dt date primary key);")
        lolk = datetime.date(2015, 8, 11)
        await conn.execute("insert into test_dci(dt) values (:date)", date=lolk)
        result = await conn.fetch_one("select * from test_dci;")
        assert result

        assert result.data[0] == lolk


async def test_time_converter():
    """
    Tests converting to/from a TIME.
    """
    async with open_connection() as conn:
        await conn.execute("create temp table test_tc (dt time primary key);")
        rn = datetime.time(1, 35, 33)
        await conn.execute("insert into test_tc values (:time);", time=rn)
        result = await conn.fetch_one("select * from test_tc;")

        assert result
        assert result.data[0] == rn


async def test_dt_infinity():
    """
    Tests using infinity in datetimes.
    """
    async with open_connection() as conn:
        await conn.execute("create temp table test_dti_in (dt timestamptz);")
        await conn.execute("insert into test_dti_in(dt) values (:inf);", inf="infinity")

        result = await conn.fetch_one("select * from test_dti_in;")
        assert result
        assert result.data[0] == "infinity"
