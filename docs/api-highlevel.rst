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

To create a connection pool, use :meth:`~.open_pool`.

.. automethod:: pg_purepy.pool.open_pool
    :async-with: pool

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