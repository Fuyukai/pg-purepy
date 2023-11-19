# flake8: noqa

import logging

from pg_purepy.connection import (
    AsyncPostgresConnection,
    QueryResult,
    RollbackTimeoutError,
    open_database_connection,
)
from pg_purepy.conversion import *
from pg_purepy.exc import *
from pg_purepy.messages import *
from pg_purepy.pool import PooledDatabaseInterface, open_pool
from pg_purepy.protocol import (
    NEED_DATA,
    SSL_MESSAGE,
    ProtocolState,
    SansIOClient,
    check_if_tls_accepted,
)

if not hasattr(logging.Logger, "trace"):
    # TRACE_LEVEL = 5

    logging.addLevelName(5, "TRACE")

    def trace(self, message, *args, **kws):
        if self.isEnabledFor(5):
            self._log(5, message, args, **kws)

    logging.Logger.trace = trace  # type: ignore

    del trace
