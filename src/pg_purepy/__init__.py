# flake8: noqa

import logging
from pg_purepy.connection import (
    AsyncPostgresConnection as AsyncPostgresConnection,
    QueryResult as QueryResult,
    RollbackTimeoutError as RollbackTimeoutError,
    open_database_connection as open_database_connection,
)
from pg_purepy.conversion import (
    Converter as Converter,
    SimpleFunctionConverter as SimpleFunctionConverter,
    EnumConverter as EnumConverter,
)
from pg_purepy.exc import (
    PostgresqlError as PostgresqlError,
    ConnectionForciblyKilledError as ConnectionForciblyKilledError,
    ProtocolParseError as ProtocolParseError,
    MissingPasswordError as MissingPasswordError,
    UnknownMessageError as UnknownMessageError,
    IllegalStateError as IllegalStateError,
    ConnectionInTransactionWarning as ConnectionInTransactionWarning,
)
from pg_purepy.messages import (
    ErrorOrNoticeResponse as ErrorOrNoticeResponse,
    ErrorResponseFieldType as ErrorResponseFieldType,
    RowDescription as RowDescription,
    ColumnDescription as ColumnDescription,
    UnrecoverableDatabaseError as UnrecoverableDatabaseError,
)
from pg_purepy.pool import (
    PooledDatabaseInterface as PooledDatabaseInterface, 
    open_pool as open_pool,
)

if not hasattr(logging.Logger, "trace"):
    # TRACE_LEVEL = 5

    logging.addLevelName(5, "TRACE")

    def trace(self, message, *args, **kws):
        if self.isEnabledFor(5):
            self._log(5, message, args, **kws)

    logging.Logger.trace = trace  # type: ignore

    del trace
