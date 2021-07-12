.. pg-purepy documentation master file, created by
   sphinx-quickstart on Fri Jun 18 01:32:54 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pg-purepy's documentation!
=====================================

``pg-purepy`` is a Python 3.9+ library for connecting to a PostgreSQL server. As the name suggests,
it is written in pure-Python (no C dependencies), and exports three APIs:

1. A high-level asynchronous AnyIO API based on connection pooling, that wraps multiple individual
   persistent connections.

2. An mid-level asynchronous anyio API, based on singular connections, that handles the protocol
   parsing and networking for you.

2. A low-level sans-IO API, which only does protocol parsing and has no niceness but is ideal for
   writing your own client.

Requirements
------------

1. ``pg-purepy`` requires at least Python 3.8. 3.7 may work, but is not actively supported.

2. ``pg-purepy`` requires the latest version of PostgreSQL, although in practice the protocol parser
   should work with versions from 9.4 upwards.

.. toctree::
   :maxdepth: 3
   :caption: Meta Information

   installation.rst
   changelog.rst
   current-support.rst

.. toctree::
   :maxdepth: 3
   :caption: API Usage

   messages.rst
   api-lowlevel.rst
   api-midlevel.rst
   api-highlevel.rst
