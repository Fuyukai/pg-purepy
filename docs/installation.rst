Installation
============

``pg-purepy`` can be found on PyPI.

.. code-block:: fish

    $ poetry add pg-purepy

Dependencies
------------

``pg-purepy`` requires Python 3.8 or newer.

Whilst ``pg-purepy`` has no C dependencies, it does have some external Python dependencies.

- anyio_ is used for connecting to the database asynchronously.
- scramp_ is used for SASL authentication.
- attrs_ is used to create the message object classes.
- pendulum_ is used for better datetime types.
- async_generator_ is used pre-Python 3.10 for ``aclosing()``. This dependency will be removed at
  the Python 3.9 End-of-Life date (currently scheduled for 2025-10-05).

.. _anyio: https://anyio.readthedocs.io/en/stable/
.. _scramp: https://github.com/tlocke/scramp
.. _attrs: https://www.attrs.org/en/stable/
.. _pendulum: https://pendulum.eustace.io/
.. _async_generator: https://async-generator.readthedocs.io/en/latest/index.html
