.. _conversion:

Converting Arguments and Results
================================

The PostgreSQL protocol provides both arguments and results in string formats. However, Python uses
objects of many different types, not just strings. ``pg-purepy`` can automatically convert between
Python objects and their PostgreSQL string representation, and vice-versa, provided it knows how to.
A converter is a class that tells the protocol machine how to convert objects back and forth.


Built-in converters
-------------------

``pg-purepy`` comes with several builtin converters, for many Python stdlib types and many
PostgreSQL core types.

"Fundamental" built-in types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- All Postgres integer types are mapped to Python :class:`int`. This includes the standard types
  such as ``int2``, ``int4``, ``int8``, but also the more esoteric types such as ``oid``.
- Both Postgres float types are mapped to Python :class:`float`.
- ``bytea`` is mapped to Python :class:`bytes`.
- Booleeans are mapped to Python :class:`bool`.

Date/Time types
~~~~~~~~~~~~~~~

- ``TIMESTAMP WITH TIMEZONE`` is mapped to :class:`~whenever.OffsetDatetime`.
- ``TIMESTAMP WITHOUT TIMEZONE`` is mapped to :class:`~whenever.NaiveDatetime`.
- ``DATE`` is mapped to :class:`datetime.date`.
- ``TIME WITHOUT TIMEZONE`` is mapped to :class:`datetime.time`. ``TIME WITH TIMEZONE``
  `isn't supported <https://wiki.postgresql.org/wiki/Don%27t_Do_This#Don.27t_use_timetz>`__.

.. note::

    I use Whenever over the vanilla ``datetime`` objects because I don't like ``datetime``. Write
    your own converter if you disagree with me.

Enumeration types
~~~~~~~~~~~~~~~~~

You can add support for your own custom enumeration types using :class:`.EnumConverter`.

.. autoclass:: pg_purepy.conversion.EnumConverter
    :members: __init__

Array Types
~~~~~~~~~~~

All built-in types have an array converter included, that will turn lists or tuples (or other
ordered sequences) into PostgreSQL arrays.

If you want to convert your own types to/from arrays, you need to register a separate array
converter.

.. autoclass:: pg_purepy.conversion.ArrayConverter
    :members: __init__

``hstore``
~~~~~~~~~~

The postgresql key-value type (known as ``hstore``) can be added as a converter.

.. code-block:: python3

    async with ... as pool:
        await pool.add_converter_using(get_hstore_converter)

Custom Converters
-----------------

If you need to convert a type that isn't supported by default, you can create a custom
:class:`.Converter`.

.. autoclass:: pg_purepy.conversion.abc.Converter
    :members: oid, from_postgres, to_postgres

The conversion context is passed to conversion functions, and contains attributes that may be
useful for your conversion.

.. autoclass:: pg_purepy.conversion.abc.ConversionContext
    :members:

Then, you can register converters with a method depending on your API.

.. automethod:: pg_purepy.connection.AsyncPostgresConnection.add_converter

.. automethod:: pg_purepy.protocol.SansIOClient.add_converter

The high-level API has its own API for converters. See :ref:`hilevel-converters`.
