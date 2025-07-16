"""Custom exceptions for fast-dag."""


class FastDAGError(Exception):
    """Base exception for all fast-dag errors."""

    pass


class ValidationError(FastDAGError):
    """Raised when validation fails."""

    pass


class CycleError(ValidationError):
    """Raised when a cycle is detected in a DAG."""

    pass


class DisconnectedNodeError(ValidationError):
    """Raised when a node is not connected to the graph."""

    pass


class MissingConnectionError(ValidationError):
    """Raised when a required connection is missing."""

    pass


class InvalidNodeError(FastDAGError):
    """Raised when a node is invalid or improperly configured."""

    pass


class ExecutionError(FastDAGError):
    """Raised when an error occurs during execution."""

    pass


class TimeoutError(ExecutionError):
    """Raised when execution times out."""

    pass
