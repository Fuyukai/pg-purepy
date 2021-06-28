pg-purepy
=========

pg-purepy is a pure-Python PostgreSQL wrapper based on the `anyio`_ library.

A lot of this library was inspired by the `pg8000`_ library. Credits to that.

Usage
-----

Using ``pg-purepy`` has certain conditions:

1. ``pg-purepy`` only supports the latest version of Python. I do not and will not support older
   versions.

2. ``pg-purepy`` only supports using anyio with trio. asyncio support is incidental, and not
   actively supported. Complaining about it will give me a good incentive to actively break asyncio
   support.

3. ``pg-purepy`` is distributed under the LGPL. Abuse this and it goes under the AGPL.

.. _anyio: https://github.com/agronholm/anyio
.. _pg8000: https://github.com/tlocke/pg8000