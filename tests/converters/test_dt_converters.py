"""
Tests date/time related converters.
"""
import datetime

import arrow
import dateutil.tz
import pytest
from dateutil.tz import gettz, UTC

from tests.util import open_connection

pytestmark = pytest.mark.anyio


async def test_timestamptz_converter():
    """
    Tests converting to/from a TIMESTAMP WITH TIMEZONE.
    """
    async with open_connection() as conn:
        await conn.execute("set timezone to 'UTC';")
        assert conn.server_timezone == dateutil.tz.UTC

        await conn.execute("create temp table test_ttz_in (id int primary key, dt timestamptz);")
        juno = arrow.get("2011-08-05T16:25:00Z")
        assert juno.tzinfo == conn.server_timezone

        await conn.execute(
            "insert into test_ttz_in(id, dt) values (1, :dt);", dt=arrow.get("2011-08-05T16:25:00Z")
        )

        row_1 = await conn.fetch_one("select dt from test_ttz_in;")
        assert isinstance(row_1.data[0], arrow.Arrow)
        assert row_1.data[0] == juno

        await conn.execute("set timezone to 'Europe/Helsinki';")
        row_2 = await conn.fetch_one("select dt from test_ttz_in;")
        dt: arrow.Arrow = row_2.data[0]
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
        assert result.data[0] == rn
