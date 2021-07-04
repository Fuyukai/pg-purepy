Message Types
=============

This document contains autoclass definitions for all of the various message types used throughout
the library. These are returned from all three layers of API, although some only at certain levels.

Base Classes
------------

These classes are never returned directly, but are supertypes to other message classes.

.. autoclass:: pg_purepy.messages.PostgresMessage

.. autoclass:: pg_purepy.messages.QueryResultMessage

State-related messages
----------------------

These classes usually represent some form of internal state change.

.. autoclass:: pg_purepy.messages.ParameterStatus
    :members:

.. autoclass:: pg_purepy.messages.ErrorOrNoticeResponse
    :members:

.. autoclass:: pg_purepy.messages.ReadyForQuery
    :members:

.. autoclass:: pg_purepy.messages.ReadyForQueryState
    :members:
    :undoc-members:

Auth-related messages
---------------------

These classes relate to the authentication loop.

.. autoclass:: pg_purepy.messages.AuthenticationRequest
    :members:

.. autoclass:: pg_purepy.messages.AuthenticationMethod
    :members:
    :undoc-members:

.. autoclass:: pg_purepy.messages.BackendKeyData
    :members:

.. autoclass:: pg_purepy.messages.AuthenticationCompleted



Query-related messages
----------------------

These classes relate to parts of the query cycle.

**Both simple and extended queries**:

.. autoclass:: pg_purepy.messages.RowDescription
    :members:

.. autoclass:: pg_purepy.messages.DataRow
    :members:

.. autoclass:: pg_purepy.messages.CommandComplete
    :members:

**Extended queries only:**

.. autoclass:: pg_purepy.messages.ParseComplete

.. autoclass:: pg_purepy.messages.ParameterDescription
    :members:

.. autoclass:: pg_purepy.messages.PreparedStatementInfo
    :members:

.. autoclass:: pg_purepy.messages.BindComplete

