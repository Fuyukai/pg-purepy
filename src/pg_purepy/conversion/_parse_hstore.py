## The below function is taken and modified from the SQLAlchemy project which is licensed under the
## MIT License.
##
## Copyright 2005-2021 SQLAlchemy authors and contributors <see AUTHORS file>.
##
## Permission is hereby granted, free of charge, to any person obtaining a copy of
## this software and associated documentation files (the "Software"), to deal in
## the Software without restriction, including without limitation the rights to
## use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
## of the Software, and to permit persons to whom the Software is furnished to do
## so, subject to the following conditions:
##
## The above copyright notice and this permission notice shall be included in all
## copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIEkD, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
## SOFTWARE.

# mypy: ignore-errors

from __future__ import annotations

import re
from collections.abc import Mapping

# My best guess at the parsing rules of hstore literals, since no formal
# grammar is given.  This is mostly reverse engineered from PG's input parser
# behavior.


HSTORE_PAIR_RE = re.compile(
    r"""
(
  "(?P<key> (\\ . | [^"])* )"       # Quoted key
)
[ ]* => [ ]*    # Pair operator, optional adjoining whitespace
(
    (?P<value_null> NULL )          # NULL value
  | "(?P<value> (\\ . | [^"])* )"   # Quoted value
)
""",
    re.VERBOSE,
)

HSTORE_DELIMITER_RE = re.compile(
    r"""
[ ]* , [ ]*
""",
    re.VERBOSE,
)


def _parse_hstore(hstore_str) -> Mapping[str, str]:
    """
    Parse an hstore from its literal string representation.

    Attempts to approximate PG's hstore input parsing rules as closely as
    possible. Although currently this is not strictly necessary, since the
    current implementation of hstore's output syntax is stricter than what it
    accepts as input, the documentation makes no guarantees that will always
    be the case.
    """

    result = {}
    pos = 0
    pair_match = HSTORE_PAIR_RE.match(hstore_str)

    while pair_match is not None:
        key = pair_match.group("key").replace(r"\"", '"').replace("\\\\", "\\")
        if pair_match.group("value_null"):
            value = None
        else:
            value = pair_match.group("value").replace(r"\"", '"').replace("\\\\", "\\")
        result[key] = value

        pos += pair_match.end()

        delim_match = HSTORE_DELIMITER_RE.match(hstore_str[pos:])
        if delim_match is not None:
            pos += delim_match.end()

        pair_match = HSTORE_PAIR_RE.match(hstore_str[pos:])

    if pos != len(hstore_str):
        raise ValueError(f"failed to parse {hstore_str} at {pos}")

    return result


def _serialize_hstore(val) -> str:
    """Serialize a dictionary into an hstore literal.  Keys and values must
    both be strings (except None for values).
    """

    def esc(s, position):
        if position == "value" and s is None:
            return "NULL"

        if isinstance(s, str):
            return '"%s"' % s.replace("\\", "\\\\").replace('"', r"\"")

        raise ValueError(f"{s!r} in {position} position is not a string.")

    return ", ".join("{}=>{}".format(esc(k, "key"), esc(v, "value")) for k, v in val.items())
