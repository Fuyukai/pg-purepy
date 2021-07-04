.. pg-purepy documentation master file, created by
   sphinx-quickstart on Fri Jun 18 01:32:54 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pg-purepy's documentation!
=====================================

``pg-purepy`` is a Python 3.9+ library for connecting to a PostgreSQL server. As the name suggests,
it is written in pure-Python (no C dependencies), and exports three APIs:

1. A high-level API, based on a connection pool, that does most of the work for you.

2. A medium-level API, based on singular connections, that does the protocol parsing automatically
   but requires some glue.

3. A low-level API, which only does protocol parsing and has no niceness but is ideal for writing
   your own client.

Requirements
------------

1. ``pg-purepy`` requires at least Python 3.8. 3.7 may work, but is not actively supported.

2. ``pg-purepy`` requires the latest version of PostgreSQL, although in practice the protocol parser
   should work with versions from 9.4 upwards.

3. Whilst the asynchronous API of ``pg-purepy`` is written using the ``anyio`` library, this
   library only actively supports the Trio backend. asyncio compatibility should work, but it is
   incidental and not actively supported.

.. toctree::
   :maxdepth: 3
   :caption: Meta Information

   installation.rst
   current-support.rst

.. toctree::
   :maxdepth: 3
   :caption: API Usage

   messages.rst
   api-lowlevel.rst
   api-midlevel.rst
   api-highlevel.rst
