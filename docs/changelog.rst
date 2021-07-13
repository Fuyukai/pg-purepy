Changelog
=========

0.7.1 (Unreleased)
------------------

 - Add :attr:`.AsyncPostgresConnection.dead` for if a connection error occurs or the underlying
   state machine becomes corrupted.

 - Implement :ref:`highlevel`.

 - Add a ``max_row`` parameter to the mid-level query APIs. This allows specifying a maximum
   number of rows for a query at the protocol level, at the cost of always using the extended query
   syntax.

0.7.0
-----

 - Initial release.