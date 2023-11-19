from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pg_purepy.conversion._parse_hstore import _parse_hstore, _serialize_hstore
from pg_purepy.conversion.abc import ConversionContext, Converter

if TYPE_CHECKING:
    from pg_purepy.pool import AsyncPostgresConnection


class HStoreConverter(Converter):
    """
    Converter for the PostgreSQL hstore type.
    """

    def __init__(self, oid: int):
        self.oid = oid

    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        return _parse_hstore(data)

    def to_postgres(self, context: ConversionContext, data: Any) -> str:
        return _serialize_hstore(data)


async def get_hstore_converter(connection: AsyncPostgresConnection) -> HStoreConverter | None:
    """
    Gets the hstore converter for a connection. This may return None if the hstore extension
    is not currently enabled.
    """

    result = await connection.fetch_one(
        "select oid from pg_catalog.pg_type where pg_catalog.pg_type.typname = 'hstore';"
    )
    if not result:
        return None

    oid = result.data[0]
    assert oid

    return HStoreConverter(oid)
