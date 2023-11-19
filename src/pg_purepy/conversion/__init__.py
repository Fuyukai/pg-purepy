"""
Package containing the converterss for Python types to PostgreSQL types.
"""

from __future__ import annotations

import itertools
from collections.abc import Sequence
from typing import TYPE_CHECKING

from pg_purepy.conversion.abc import Converter
from pg_purepy.conversion.arrays import ArrayConverter
from pg_purepy.conversion.basics import (
    BoolConverter,
    ByteaConverter,
    SimpleFunctionConverter,
    TextConverter,
)
from pg_purepy.conversion.dt import (
    STATIC_DATE_CONVERTER,
    STATIC_DATEA_CONVERTER,
    STATIC_TIME_CONVERTER,
    STATIC_TIMESTAMPNOTZ_CONVERTER,
    STATIC_TIMESTAMPNOTZA_CONVERTER,
    STATIC_TIMESTAMPTZ_CONVERTER,
    STATIC_TIMESTAMPTZA_CONVERTER,
)
from pg_purepy.conversion.enums import EnumConverter

if TYPE_CHECKING:
    from pg_purepy.protocol import SansIOClient


def _make_array_converters(
    oids: Sequence[int],
    cvs: Sequence[Converter],
    *,
    quote_inner: bool = False,
) -> list[Converter]:
    return [
        ArrayConverter(oid, cv, quote_inner=quote_inner)
        for (oid, cv) in zip(oids, cvs, strict=True)
    ]


KNOWN_INT_OIDS = (20, 21, 23, 26, 27, 28, 29)
INT_CONVERTERS = [SimpleFunctionConverter(oid, int, str) for oid in KNOWN_INT_OIDS]
KNOWN_INTA_OIDS = (1016, 1005, 1007, 1028, 1010, 1011, 1012)
INTA_CONVERTERS = _make_array_converters(KNOWN_INTA_OIDS, INT_CONVERTERS)

KNOWN_TEXT_OIDS = (18, 19, 25)
STR_CONVERTERS = [TextConverter(oid) for oid in KNOWN_TEXT_OIDS]
KNOWN_STRA_OIDS = (1009, 1002, 1003)
STRA_CONVERTERS = _make_array_converters(KNOWN_STRA_OIDS, STR_CONVERTERS, quote_inner=True)

KNOWN_FLOAT_OIDS = (700, 701)
FLOAT_CONVERTERS = [SimpleFunctionConverter(oid, float, str) for oid in KNOWN_FLOAT_OIDS]
KNOWN_FLOATA_OIDS = (1021, 1022)
FLOATA_CONVERTERS = _make_array_converters(KNOWN_FLOATA_OIDS, FLOAT_CONVERTERS)

STATIC_BOOLEAN_CONVERTER = BoolConverter()
STATIC_BOOLEANA_CONVERTER = ArrayConverter(1000, STATIC_BOOLEAN_CONVERTER)
STATIC_BYTES_CONVERTER = ByteaConverter()


def apply_default_converters(protocol: SansIOClient) -> None:
    """
    Applies the default converter objects to a protocol.
    """

    # fmt: off
    for cv in itertools.chain(
        INT_CONVERTERS, INTA_CONVERTERS,
        STR_CONVERTERS, STRA_CONVERTERS,
        FLOAT_CONVERTERS, FLOATA_CONVERTERS,
    ):
        protocol.add_converter(cv)
    # fmt: on

    protocol.add_converter(STATIC_BOOLEAN_CONVERTER)
    protocol.add_converter(STATIC_BOOLEANA_CONVERTER)

    protocol.add_converter(STATIC_BYTES_CONVERTER)

    # datetime converters
    protocol.add_converter(STATIC_TIMESTAMPTZ_CONVERTER)
    protocol.add_converter(STATIC_TIMESTAMPTZA_CONVERTER)

    protocol.add_converter(STATIC_TIMESTAMPNOTZ_CONVERTER)
    protocol.add_converter(STATIC_TIMESTAMPNOTZA_CONVERTER)

    protocol.add_converter(STATIC_DATE_CONVERTER)
    protocol.add_converter(STATIC_DATEA_CONVERTER)

    protocol.add_converter(STATIC_TIME_CONVERTER)
    protocol.add_converter(STATIC_DATEA_CONVERTER)


__all__ = ("Converter", "apply_default_converters", "EnumConverter", "ArrayConverter")
