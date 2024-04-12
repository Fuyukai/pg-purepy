from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, assert_never

import dateutil
import dateutil.parser
import whenever

from pg_purepy.conversion.abc import Converter
from pg_purepy.conversion.arrays import ArrayConverter

if TYPE_CHECKING:
    from pg_purepy.protocol import ConversionContext


class TimestampTzConverter(Converter):
    """
    Converts from a TIMESTAMP WITH TIMEZONE to an Arrow object.
    """

    oid = 1184

    def from_postgres(self, context: ConversionContext, data: str) -> whenever.OffsetDateTime | str:
        if data == "infinity" or data == "-infinity":
            return data

        # TIMESTAMPTZ are stored in UTC, and are converted to the server's timezone on retrieval.
        # So we provide the returned date in the server's timezone.
        parsed = dateutil.parser.isoparse(data)

        # can't directly pass the datetime as it'll complain about UTC.
        return whenever.OffsetDateTime.from_rfc3339(parsed.isoformat())

    def to_postgres(
        self,
        context: ConversionContext,
        data: Literal["infinity", "-infinity"] | whenever.OffsetDateTime,
    ) -> str:

        match data:
            case "infinity":
                return "infinity"

            case "-infinity":
                return "-infinity"

            case whenever.OffsetDateTime():
                return data.rfc3339()

            case _:
                assert_never(data)


STATIC_TIMESTAMPTZ_CONVERTER = TimestampTzConverter()
STATIC_TIMESTAMPTZA_CONVERTER = ArrayConverter(118, STATIC_TIMESTAMPTZ_CONVERTER)


class TimestampNoTzConverter(Converter):
    """
    Converts from a TIMESTAMP WITHOUT TIMEZONE to an Arrow object.
    """

    oid = 1114

    def from_postgres(self, context: ConversionContext, data: str) -> whenever.NaiveDateTime | str:
        if data == "infinity" or data == "-infinity":
            return data

        return whenever.NaiveDateTime.from_common_iso8601(data)

    def to_postgres(self, context: ConversionContext, data: whenever.NaiveDateTime) -> str:
        # we can once again just do isoformat, because postgres will parse it, go "nice offset.
        # fuck you" and completely discard it.
        #
        # postgres@127:postgres> select '2021-07-13T22:16:36+01:00'::timestamp;
        # +---------------------+
        # | timestamp           |
        # |---------------------|
        # | 2021-07-13 22:16:36 |
        # +---------------------+

        return data.common_iso8601()


STATIC_TIMESTAMPNOTZ_CONVERTER = TimestampNoTzConverter()
STATIC_TIMESTAMPNOTZA_CONVERTER = ArrayConverter(1115, STATIC_TIMESTAMPNOTZ_CONVERTER)


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
STATIC_DATEA_CONVERTER = ArrayConverter(1182, STATIC_DATE_CONVERTER)


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
STATIC_TIMEA_CONVERTER = ArrayConverter(1183, STATIC_TIME_CONVERTER)
