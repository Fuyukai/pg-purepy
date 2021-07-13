Low-Level API
=============

The low-level API is a sans-IO_, in-memory PostgreSQL client. It implements creation and parsing of
PostgreSQL network protocol messages, and maintains internal state relating to the client
connection.

.. warning::

    Despite this class primarily being laid out similar to the PostgreSQL protocol itself, this
    is *not* a protocol state machine, but a client with its own semantics (e.g. you can never send
    a Parse message by itself, it must always be accompanied with a Describe).

Creation
--------

The primary class for the low-level client is :class:`~.SansIOClient`.

.. autoclass:: pg_purepy.SansIOClient
    :members: __init__, encoding, timezone, dead

Doing... Stuff
--------------

The client by itself doesn't really do anything. Instead, there's a process in order to make it do
something.

1. Call the ``do_*`` method for the thing you want to do, e.g. send a query. This updates internal
   states and returns the bytes for the command.
2. Send the bytes returned by the ``do_*`` function to the server.
3. Enter the Ready Loop.

The Ready Loop
--------------

The ready loop is the key construct for working with the low-level API. It consists of a simple loop
that does several steps.

First, you need to check if the protocol is already ready. If you don't do this, you'll likely get
a deadlock because the server will have nothing to send and you'll wait forever on reading.

.. autoattribute:: pg_purepy.SansIOClient.ready

Second, you need to check if there are any incoming messages from the server. This is done by
calling :meth:`.SansIOClient.next_event` in an inner ``while True`` loop until it returns the
special ``NO_DATA`` constant sentinel value. You should also do error handling wrt to
:class:`.ErrorOrNoticeResponse` messages, if so desired. If this inner loop returns
:class:`.ReadyForQuery`, then you have finished the whole loop and should break out of both the
inner and outer loops.

.. code-block:: python3

    while True:
        event = protocol.next_event()
        if event == NEED_DATA:
            break

        # optional, you can just yield it or whatever else
        if isinstance(event, ErrorResponse): ...

        do_something_with_event(event)
        if isinstance(event, ReadyForQuery):
            return

.. data:: pg_purepy.NO_DATA

    A dummy sentinel value used to signal the client needs more data to proceed.

.. automethod:: pg_purepy.SansIOClient.next_event

Next, you need to get any synchronisation data that the client needs to send to the server. This
may occur during authentication for example.

.. code-block:: python3

    to_send = protocol.get_needed_synchronisation()
    send_data(to_send)

.. automethod:: pg_purepy.SansIOClient.get_needed_synchronisation

Finally, you need to read incoming data from the server, and feed it into the protocol machine for
later processing. This data won't actually be processed until you call
:meth:`~.SansIOClient.next_event` in the next iteration of the loop.

.. code-block:: python3

    received = read_data()
    protocol.receive_bytes(received)

.. automethod:: pg_purepy.SansIOClient.receive_bytes

The ready loop in the mid-level API looks like this, for reference:

.. code-block:: python3

    while not self._protocol.ready:
        while True:
            next_event = self._protocol.next_event()
            if next_event is NEED_DATA:
                break

            if isinstance(next_event, ErrorOrNoticeResponse) and next_event.notice:
                if next_event.severity == "WARNING":
                    err = wrap_error(next_event)
                    warnings.warn(str(err))

            yield next_event

            if isinstance(next_event, ReadyForQuery):
                await anyio.sleep(0)  # checkpoint()
                return

        to_send = self._protocol.get_needed_synchronisation()
        if to_send:
            await self._stream.send(to_send)

        received = await self._stream.receive(65536)
        self._protocol.receive_bytes(received)

.. note::

    The order of this may look odd, but it helps prevent deadlocks. Draining the events first means
    that any events that may have been sent AFTER the ReadyForQuery, for example a second query
    that was queued by the client before the server sent the ReadyForQuery message.

The ready loop should be performed after every single action to completion, in order to drain data
from the server that is no longer relevant. If that's not possible, then methods should call the
ready loop before performing any other actions, for the same reason.

Sending Commands
----------------

Each ``do_*`` command performs a specific action or set of actions on the server. Each command will
cause the server to issue a specific set of responses in return, which are constructed into their
own message classes.

Additionally, the server may issue certain responses at any point in the connection. These will
be captured by the client and returned from :meth:`.next_event` automatically. These are:

- :class:`.ParameterStatus`, when a runtime parameter is set
- :class:`.ErrorOrNoticeResponse`, for server-sent errors or notices

.. warning::

    :class:`.ErrorOrNoticeResponse` instances will only be captured if they are either notices, or
    recoverable. Non-recoverable errors will be turned into protocol errors.

Once the the server has sent all messages, a :class:`.ReadyForQuery` event will be returned.

Startup
~~~~~~~

The first message you must send is the startup message. This contains the username and database
information.

.. automethod:: pg_purepy.SansIOClient.do_startup

Any additional messages may be sent during the synchronisation stage.

This will yield several messages, usually in this order:

1. The :class:`.AuthenticationRequest` describing how the client should authenticate, if needed.
2. Several :class:`.ParameterStatus` message, setting runtime parameters.
3. A :class:`.BackendKeyData` message, used for potential later cancellation.
4. A :class:`.AuthenticationCompleted` message, signifying that the client can now send commands.

Simple Queries
~~~~~~~~~~~~~~

Simple queries are static queries with no parameters that combine parsing and execution into one
step. These queries should be used by a higher-level client when no parameters are needed, for
network performance reasons.

To perform a simple query, you should call :meth:`.do_simple_query`.

.. automethod:: pg_purepy.SansIOClient.do_simple_query

This will yield several messages, usually in this order:

1. A :class:`.RowDescription`, if this query returns data, or a :class:`.ErrorOrNoticeResponse` if
   the query was invalid in some way.
2. Zero to N :class:`.DataRow` instances.
3. One :class:`.CommandComplete` instance.


Extended Queries
~~~~~~~~~~~~~~~~

Extended queries are queries using prepared statements and SQL injection invulnerable parameter
injection.

To perform an extended query, you need to create a prepared statement. This will issue a
Parse+Describe combo of messages to the server.

.. automethod:: pg_purepy.SansIOClient.do_create_prepared_statement

This will yield several messages, usually in this order:

1. A :class:`.ParseComplete`, or a :class:`.ErrorOrNoticeResponse` if the query was invalid in some
   way.
2. A :class:`.ParameterDescription`, if the query has parameters to be filled.
3. A :class:`.PreparedStatementInfo` wrapping the relevant info about the prepared query.

.. warning::

    Internally, the server actually returns either a :class:`.RowDescription` or a
    ``NoData`` message, but both are wrapped into the :class:`.PreparedStatementInfo`.

Once you have a prepared statement, you can then run the query.

.. automethod:: pg_purepy.SansIOClient.do_bind_execute

This will yield a similar sequence of messages as the simple query:

1. (Optional) A :class:`.ErrorOrNoticeResponse` if the parameters were invalid in some way.
2. A :class:`.BindComplete`.
3. 0 to N :class:`.DataRow` instances for the actual data of the query.

Termination
~~~~~~~~~~~

When you are done with a connection, you should send a Terminate message. This closes the client
and no further actions will work.

.. automethod:: pg_purepy.SansIOClient.do_terminate

Internal State
~~~~~~~~~~~~~~

The client object exposes the current state of the protocol state machine using
:attr:`.SansIOClient.state`.

.. autoattribute:: pg_purepy.SansIOClient.state

.. autoclass:: pg_purepy.protocol.ProtocolState
    :members:
    :undoc-members:


.. _sans-IO: https://sans-io.readthedocs.io/