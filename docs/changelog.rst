Changelog
=========

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