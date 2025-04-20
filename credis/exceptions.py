from typing import Union


class Error(Exception):
    """Base class for all exceptions raised by this module."""

    def __init__(self, code: Union[str, int], message: str):
        super().__init__(message)
        self.message = message
        self.code = code

    def __str__(self):
        return f"Error(code={self.code}, message={self.message})"

    def __repr__(self):
        return f"Error(code={self.code}, message={self.message})"


class ConnectionError(Error):
    """Exception raised for errors in the connection process."""

    def __init__(self, message: str):
        super().__init__("REDIS_2001", message)


class SentinelError(Error):
    """Exception raised for errors in the Sentinel process."""

    def __init__(self, message: str):
        super().__init__("REDIS_2002", message)


class InitError(Error):
    """Exception raised for errors in the initialization process."""

    def __init__(self, message: str):
        super().__init__("REDIS_2003", message)
