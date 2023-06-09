Installation
============

``pg-purepy`` can be found on PyPI.

.. code-block:: fish

    $ poetry add pg-purepy

Dependencies
------------

``pg-purepy`` requires Python 3.10 or newer.

Whilst ``pg-purepy`` has no C dependencies, it does have some external Python dependencies.

- anyio_ is used for connecting to the database asynchronously.
- scramp_ is used for SASL authentication.
- attrs_ is used to create the message object classes.
- arrow_ is used for better datetime types.

.. _anyio: https://anyio.readthedocs.io/en/stable/
.. _scramp: https://github.com/tlocke/scramp
.. _attrs: https://www.attrs.org/en/stable/
.. _arrow: https://arrow.readthedocs.io/en/latest/
