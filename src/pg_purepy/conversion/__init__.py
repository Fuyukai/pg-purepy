"""
Package containing the converterss for Python types to PostgreSQL types.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pg_purepy.conversion.basics import (
    SimpleFunctionConverter,
    TextConverter,
    BoolConverter,
    ByteaConverter,
)

if TYPE_CHECKING:
    from pg_purepy.protocol import SansIOClient


KNOWN_INT_OIDS = (20, 21, 23, 26, 27, 28, 29)
INT_CONVERTERS = [SimpleFunctionConverter(oid, int, str) for oid in KNOWN_INT_OIDS]

KNOWN_TEXT_OIDS = (18, 19, 25)
STR_CONVERTERS = [TextConverter(oid) for oid in KNOWN_TEXT_OIDS]

KNOWN_FLOAT_OIDS = []

STATIC_BOOLEAN_CONVERTER = BoolConverter()
STATIC_BYTES_CONVERTER = ByteaConverter()


def apply_default_converters(protocol: SansIOClient):
    """
    Applies the default converter objects to a protocol.
    """
    for cvi in INT_CONVERTERS:
        protocol.add_converter(cvi)

    for cvs in STR_CONVERTERS:
        protocol.add_converter(cvs)

    protocol.add_converter(STATIC_BOOLEAN_CONVERTER)
    protocol.add_converter(STATIC_BYTES_CONVERTER)