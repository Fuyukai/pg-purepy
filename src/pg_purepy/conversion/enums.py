from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, cast

from pg_purepy.conversion.abc import Converter

if TYPE_CHECKING:
    from pg_purepy.protocol import ConversionContext


class EnumConverter(Converter):
    """
    A converter that lets you use Python enums for PostgreSQL enums.
    """

    def __init__(
        self,
        oid: int,
        enum_klass: type[Enum],
        *,
        use_member_values: bool = False,
        lowercase_names: bool = True,
    ):
        """
        :param oid: The PostgreSQL object ID of the enum type.
        :param enum_klass: The Python-side enumeration class.
        :param use_member_values: If True and the enum values are all strings, then the member
                                  values will be used rather than the names.
        :param lowercase_names: If ``use_member_values`` is False, and this is True, then the
                                member names will be lowercased. This is the general PostgreSQL
                                enum convention.
        """
        self.oid = oid
        self._member_klass = enum_klass
        self._use_members = use_member_values
        self._lowercase_names = lowercase_names

    def from_postgres(self, context: ConversionContext, data: str) -> Any:
        if self._use_members:
            return self._member_klass(data)

        try:
            return self._member_klass[data]
        except KeyError:
            if not self._lowercase_names:
                raise

            # hmm...
            return self._member_klass[data.upper()]

    def to_postgres(self, context: ConversionContext, data: Enum) -> str:
        if self._use_members:
            return cast(str, data.value)

        if self._lowercase_names:
            return data.name.lower()

        return data.name
