.. _current-support:

Current Support For PostgreSQL Features
=======================================

This document contains the list of features that is currently supported, is planned to be supported,
and probably won't be supported.

Currently Supported
-------------------

- Basic querying of all types is supported across all APIs.

Plan To Be Supported
--------------------

- ``LISTEN``/``NOTIFY`` messages...

 * ... via a dedicated connection on the high-level API
 * ... via a channel on the medium-level API
 * ... being returned by ``next_event()`` at any state on the low-level API

- ``COPY`` support (API pending).

- Using SQL-side prepared queries.

- Converters...

 * ... for most other built-in types!

Won't Be Supported
------------------

- High-level APIs for multiple SQL queries in one query string. There's no nice API for this,
  unfortunately.
