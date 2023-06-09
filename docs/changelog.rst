Changelog
=========

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