class PostgresqlError(Exception):
    """
    Base exception class all other exceptions are derived from.
    """


class ProtocolParseError(PostgresqlError):
    """
    Base exception class for all protocol parsing related errors.
    """


class MissingPasswordError(ProtocolParseError):
    """
    Raised when the server asks for a password, but we don't have one.
    """


class UnknownMessageError(ProtocolParseError):
    """
    Raised when an unknown message is returned.
    """


class IllegalStateError(ProtocolParseError):
    """
    Raised when an operation is attempted that would result in an illegal state.
    """
