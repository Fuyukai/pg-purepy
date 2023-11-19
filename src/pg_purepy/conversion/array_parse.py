# This code is copied from pg8000, at
# https://github.com/tlocke/pg8000/blob/e4a8b49825baa475dd65547e966767a9a98d6954/pg8000/converters.py#L454-L502
#
# The following licence applies to this file:
#
# Copyright (c) 2007-2009, Mathieu Fenniak
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# mypy: ignore-errors

from enum import Enum
from typing import Any


class ArrayState(Enum):
    InString = 1
    InEscape = 2
    InValue = 3
    Out = 4


def _parse_array(data, adapter) -> list[Any]:
    state = ArrayState.Out
    stack = [[]]
    val = []
    for c in data:
        if state == ArrayState.InValue:
            if c in ("}", ","):
                value = "".join(val)
                stack[-1].append(None if value == "NULL" else adapter(value))
                state = ArrayState.Out
            else:
                val.append(c)

        if state == ArrayState.Out:
            if c == "{":
                a = []
                stack[-1].append(a)
                stack.append(a)
            elif c == "}":
                stack.pop()
            elif c == ",":
                pass
            elif c == '"':
                val = []
                state = ArrayState.InString
            else:
                val = [c]
                state = ArrayState.InValue

        elif state == ArrayState.InString:
            if c == '"':
                stack[-1].append(adapter("".join(val)))
                state = ArrayState.Out
            elif c == "\\":
                state = ArrayState.InEscape
            else:
                val.append(c)
        elif state == ArrayState.InEscape:
            val.append(c)
            state = ArrayState.InString

    return stack[0][0]
