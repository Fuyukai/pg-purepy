.. _highlevel:

High-level API
==============

.. versionadded:: 0.7.1

The high-level API is a wrapper over the mid-level connection API, combined with a connection pool.

Pooling
-------

The high-level API provides a connection pool that automatically maintains a certain amount of
idle connections to the PostgreSQL server. As only one query can be issued to a server at any one
time, this provides an effective way of performinng multiple queries concurrently.

.. warning::

    These connections are PERSISTENT connections. The pool does not reap connections for idle
    connections. If you use something ala pgbouncer, that automatically closes idle connections,
    and your application is relatively low activity, there *will* be a cascading failure as
    broken connections are checked out of the pool and aren't reconnected until an obvious error
    happens upon trying to query on a disconnected connection.

Connecting
----------

To create a connection pool, use :meth:`.open_pool`.

.. automethod:: pg_purepy.pool.open_pool

.. warning::

    Your connection count should be relatively low. The default is a very good idea for nearly all
    applications. Don't change it unless you have the benchmarks to prove it's a good idea.
    See: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing

.. autoclass:: pg_purepy.pool.PooledDatabaseInterface
    :members: max_connections, idle_connections, waiting_tasks

Querying
--------

The connection pool object has a similar high-level query API to the mid-level API.

.. automethod:: pg_purepy.PooledDatabaseInterface.execute

.. automethod:: pg_purepy.PooledDatabaseInterface.fetch

.. automethod:: pg_purepy.PooledDatabaseInterface.fetch_one

Transactions
------------

As two subsequent queries may not be on the same connection, transactions get tricky. For that end,
the pool has a special :meth:`.PooledDatabaseInterface.checkout_in_transaction` method which
checks out a connection for exclusive usage in a transaction block.

.. automethod:: pg_purepy.PooledDatabaseInterface.checkout_in_transaction

.. code-block:: python3

    async with pool.checkout_in_transaction() as conn:
        await conn.fetch("insert into ...")

The transaction will be automatically committed or rolled back as appropriate at the end of the
``async with`` block, and the connection will not be reused until the checkout is done.

.. _hilevel-converters:

Converters
----------

You can add converters like the other two APIs using :meth:`.PooledDatabaseInterface.add_converter`.
This will add it to all open connections, as well as any future connections that may be opened.

.. automethod:: pg_purepy.PooledDatabaseInterface.add_converter

If you wish to automatically add the array converter for converting PostgreSQL arrays of a custom
type that is converted, use :meth:`.PooledDatabaseInterface.add_converter_with_array`.

.. automethod:: pg_purepy.PooledDatabaseInterface.add_converter_with_array

Cancellation
------------

Application-level cancellation is supported automatically. If a query is cancelled via a cancel
scope, a cancellation request will be issued to the server to avoid having to drain more events
from the server if possible.

.. code-block:: python3

    # timeout block will automatically cancel the query after a while
    # and it will be returned to the pool for use (hopefully) immediately
    with anyio.move_on_after(timeout):
        async for result in pool.fetch(really_long_query):
            await do_long_running_operation(result)

This also works automatically in transactions - the insertion will be cancelled and the transaction
will be rolled back.
