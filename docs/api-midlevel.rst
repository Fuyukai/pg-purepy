.. _midlevel:

Mid-level API
=============

The mid-level API combines the client of the low-level API with the asynchronous capabilities of
the ``anyio`` library to create an actual, networked client. If you want to provide your own
connection pooling logic, for example, you will want to build on top of the mid-level API.

Connecting
----------

In order to get anywhere, you need to actually connect to the server.

.. autofunction:: pg_purepy.open_database_connection
    :async-with: conn

Querying
--------

There's two ways to query a PostgreSQL database.

- Eager queries, which load all data into memory at once.

- Lazy queries, which lets the client apply backpressure by iterating over every row as it arrives.

There are high-level APIs for both eager and lazy queries, which wraps a low-level API that allows
finer control of the actual messages arriving.

.. warning::

    Querying to the server is protected by a lock, as only one query can be issued at once. Allowing
    multiple queries simultaneously would require complex tracking logic for incoming messages, and
    wouldn't help anyway because the server only processes one query at a time.

    If you want the ability to do queries simultaneously, use the high-level API.

Querying, Eagerly
-----------------

Whilst ``pg-purepy`` doesn't export a DBAPI 2.0 API as such, there are two high-level functions
that resemble DBAPI. These two functions are likely the two most useful functions when querying,
but they are both *eager* functions and load the entire returned dataset into memory at once.

.. automethod:: pg_purepy.AsyncPostgresConnection.fetch

.. automethod:: pg_purepy.AsyncPostgresConnection.execute

For example, to insert some data, check how many rows were inserted, and verify it with a select:

.. code-block:: python3

    async with open_database_connection(...) as conn:
        inserted = await conn.execute("insert into some_table(...) values (...);")
        print(f"Inserted {inserted} rows")
        select_count = await conn.fetch("select count(*) from some_table;")
        row = select_count[0]
        assert row.data[0] == inserted

.. warning::

    Eager functions only support one query at a time, due to limitations in API design and the
    underlying protocol.

Querying, Lazily
----------------

If you have large data sets, or want to query lazily for other reasons, then
:func:`~.AsyncPostgresConnection.query` can be used. This function is an asynchronous context
manager, returning a :class:`~.QueryResult`.

.. automethod:: pg_purepy.AsyncPostgresConnection.query
    :async-with: query

.. autoclass:: pg_purepy.connection.QueryResult
    :members:

Example usage:

.. code-block:: python3

    async with conn.query("select * from table") as query:
        async for row in query:
            print(row.data)

        print("Total rows:", await query.row_count())


.. warning::

    Exiting from the asynchronous generator early will require the next query issued to keep
    reading the data rows of the previous query until the query returned. Use limits, or cursors,
    for particularly large queries.

.. warning::

    The lazy function only support one query at a time, due to limitations in API design and the
    underlying protocol.

Paramaterised Queries
---------------------

Parameterised queries are also supported, using either positional arguments or keyword arguments,
in either eager loading mode or lazy loading mode.
Positional argument parameters follow the PostgreSQL parameter syntax, where parameters are
specified with ``$N`` where N is the index of the parameter. Keyword argument parameters follow the
DBAPI colon-named syntax, where parameters are specified with ``:name`` where name is the keyword
passed to the function.

.. note::

    Internally, keyword argument parameters are converted into the positional format when creating
    the prepared statement. This means that only the positional format parameters are available
    when using explicitly created or loaded prepared statements.

.. tab:: Keywords

    .. code-block:: python3

        selected = await conn.fetch("select * from some_table where column = :name;",
                                    name=some_variable)

.. tab:: Positionals

    .. code-block:: python3

        inserted = await conn.execute("insert into some_table(foo) values ($0, $1);",
                                      x, y)

Low-level querying
------------------

If, for some reason, you need to access the messages returned during a query cycle, you can
use the method :meth:`~.AsyncPostgresConnection.lowlevel_query`.

.. automethod:: pg_purepy.AsyncPostgresConnection.lowlevel_query

This function yields out the raw :class:`.PostgresMessage` objects that are received from the
protocol, as well as handling any error responses.

.. warning::

    As this is a raw asynchronous generator, this must be wrapped in an ``aclosing()`` block.
    See https://github.com/python-trio/trio/issues/265.

.. code-block:: python3

    async with aclosing(conn.query("select * from table") as agen:
        async for message in agen:
            if isinstance(message, RowDescription):
                print(f"Got row description:", message)

            elif isinstance(message, DataRow):
                print(f"Got data row", message.data)

For most queries, this function will yield the following sequence of messages, in this order:

- Either a :class:`.RowDescription` instance, or a :class:`.NoData` instance.
- Zero to N :class:`.RowData` instances
- One :class:`.CommandComplete` instance.

The last message will always be a :class:`.CommandComplete` instance.

Error handling
--------------

The underlying low-level client reports server-side errors as :class:`.ErrorResponse` instances, but
the mid-level connection objects will turn these into proper exceptions in the query functions.

All exceptions raised from ErrorResponses inherit from :class:`.DatabaseError`.

.. autoexception:: pg_purepy.BaseDatabaseError

However, you shouldn't catch this exception as the client differentiates these into two subtypes -
recoverable errors via :class:`.RecoverableDatabaseError`, and unrecoverable errors via
:class:`.UnrecoverableDatabaseError`. A general rule is that you should *only* catch the recoverable
variant.

.. autoexception:: pg_purepy.RecoverableDatabaseError

.. autoexception:: pg_purepy.UnrecoverableDatabaseError

Transaction Helpers
-------------------

The mid-level API does nothing with transactions by default, operating in autocommit mode. However,
it does supply a transaction helper which will automatically commit at the end of the ``async with``
block, or rollback if an error happens.

.. automethod:: pg_purepy.AsyncPostgresConnection.with_transaction
    :async-with:

.. warning::

    This will NOT protect against different tasks from calling query functions inside your
    transaction. This would require overly complicated locking logic! Instead, wrap your acquisition
    of this inside a different lock, and guard all other transaction helpers with it.

Connection Properties
---------------------

The connection class exposes a handful of potentially useful properties:

.. autoattribute:: pg_purepy.AsyncPostgresConnection.ready

.. autoattribute:: pg_purepy.AsyncPostgresConnection.in_transaction
