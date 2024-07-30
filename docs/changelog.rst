Changelog
=========

0.11.1 (2024-07-30)
-------------------

- Fix for newer dependencies.

0.11.0 (2024-04-12)
-------------------

- Switch to the ``whenever`` package for date/time types, instead of Arrow. See 
  https://dev.arie.bovenberg.net/blog/python-datetime-pitfalls/ for more information as to why.
- ``pg-purepy`` now passes Pyright strict mode.
- :class:`.Converter` is now a generic type.

0.10.0 (2023-12-22)
-------------------

- Switch to the ``structlog`` package for logging.

0.9.5 (2023-12-20)
------------------

- Make :meth:`.AsyncPostgresConnection.fetch_one` raise an error in the event of an empty row,
  instead of returning an optional.

  This results in a more ergonomic API (as you don't need to check for None constantly) with the
  side effect of requiring potentially more error handling. This is the same approach used in
  the `sqlx <https://docs.rs/sqlx-core/0.7.3/src/sqlx_core/executor.rs.html#115-121>`_ library,
  for one example.

0.9.4 (2023-11-25)
------------------

- Add support for the JSON and JSONB types to the default converters.

0.9.0 (2023-11-19)
------------------

- Fix type hints and add a ``py.typed`` marker.
- Fix a mishap with ``ExceptionGroup`` in the pool handler.

0.8.2 (2023-06-09)
------------------

- Bump dependency versions.

0.8.1 (2022-04-21)
------------------

- Make the connection pool open all connections sequentially. This makes error reporting better,
  as errors such as invalid passwords are raised immediately, and without turning into an exception
  group.

0.8.0 (2022-03-22)
------------------

- Make rollbacks reliable during cancellation.

- Add cancellation support to the connection pool.

0.7.4 (2021-10-29)
------------------

- Add ``TRACE`` log level, used for protocol messages.

0.7.3 (2021-10-27)
------------------

- Add ``hstore`` support.

0.7.2 (2021-08-03)
------------------

- Add :meth:`.DataRow.to_dict` to turn a data row into a dictionary of values.

- Less log spam.

0.7.1 (2021-07-14)
------------------

- Add :attr:`.AsyncPostgresConnection.dead` for if a connection error occurs or the underlying
  state machine becomes corrupted.

- Implement :ref:`highlevel`.

- Add a ``max_row`` parameter to the mid-level query APIs. This allows specifying a maximum
  number of rows for a query at the protocol level, at the cost of always using the extended query
  syntax.

- Rework converters.

  - Add date/time converters.

  - Add converters for :class:`enum.Enum` instances.

  - Add array conversion support.

- :class:`.DataRow` now supports ``__getitem__`` syntax.

- Add an API that allows getting type OIDs from type names on the high-level API.

- Export :attr:`.SansIOClient.timezone` and :attr:`.AsyncPostgresConnection.server_timezone`.

- Protect certain server parameters from being set. The datetime converters need these to
  function properly.

0.7.0
-----

- Initial release.
