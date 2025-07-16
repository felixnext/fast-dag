"""Fast DAG is a library for building and executing DAGs in Python.

Copyright (c) 2025 Felix Geilert
"""

__version__ = "0.1.0"

from .core import (
    ConditionalReturn,
    Context,
    CycleError,
    DisconnectedNodeError,
    ExecutionError,
    FastDAGError,
    FSMContext,
    FSMReturn,
    InvalidNodeError,
    MissingConnectionError,
    Node,
    NodeType,
    SelectReturn,
    TimeoutError,
    ValidationError,
)
from .dag import DAG

__all__ = [
    "__version__",
    "Context",
    "FSMContext",
    "Node",
    "NodeType",
    "ConditionalReturn",
    "SelectReturn",
    "FSMReturn",
    "DAG",
    "FastDAGError",
    "ValidationError",
    "CycleError",
    "DisconnectedNodeError",
    "MissingConnectionError",
    "InvalidNodeError",
    "ExecutionError",
    "TimeoutError",
]
