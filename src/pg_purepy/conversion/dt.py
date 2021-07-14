from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import arrow
from pg_purepy.conversion.abc import Converter

if TYPE_CHECKING:
    from pg_purepy.protocol import ConversionContext


class TimestampTzConverter(Converter):
    """
    Converts from a TIMESTAMP WITH TIMEZONE to an Arrow object.
    """

    oid = 1184

    def from_postgres(self, context: ConversionContext, data: str) -> arrow.Arrow:
        # TIMESTAMPTZ are stored in UTC, and are converted to the server's timezone on retrieval.
        # So we provide the returned date in the server's timezone.
        return arrow.get(data, tzinfo=context.timezone)

    def to_postgres(self, context: ConversionContext, data: arrow.Arrow) -> str:
        # postgres just straight up accepts RFC 3339 format, even if it doesn't produce it.
        # This will coincidentally work with vanilla datetimes, because `isoformat` is a method
        # on both.
        return data.isoformat()


STATIC_TIMESTAMPTZ_CONVERTER = TimestampTzConverter()


class TimestampNoTzConverter(Converter):
    """
    Converts from a TIMESTAMP WITHOUT TIMEZONE to an Arrow object.
    """

    oid = 1114

    def from_postgres(self, context: ConversionContext, data: str) -> arrow.Arrow:
        return arrow.get(data)  # No timezone!

    def to_postgres(self, context: ConversionContext, data: arrow.Arrow) -> str:
        # we can once again just do isoformat, because postgres will parse it, go "nice offset.
        # fuck you" and completely discard it.
        #
        # postgres@127:postgres> select '2021-07-13T22:16:36+01:00'::timestamp;
        # +---------------------+
        # | timestamp           |
        # |---------------------|
        # | 2021-07-13 22:16:36 |
        # +---------------------+

        return data.isoformat()


STATIC_TIMESTAMPNOTZ_CONVERTER = TimestampNoTzConverter()


class DateConverter(Converter):
    """
    Converts from a DATE to a :class:`datetime.date` object.
    """

    oid = 1082

    def from_postgres(self, context: ConversionContext, data: str) -> datetime.date:
        return datetime.date.fromisoformat(data)

    def to_postgres(self, context: ConversionContext, data: datetime.date) -> str:
        return data.isoformat()


STATIC_DATE_CONVERTER = DateConverter()


class TimeConverter(Converter):
    """
    Converts from a TIME WITHOUT TIMEZONE to a :class`datetime.time` object.
    """

    oid = 1083

    def from_postgres(self, context: ConversionContext, data: str) -> datetime.time:
        return datetime.time.fromisoformat(data)

    def to_postgres(self, context: ConversionContext, data: datetime.time) -> str:
        return data.isoformat()


STATIC_TIME_CONVERTER = TimeConverter()