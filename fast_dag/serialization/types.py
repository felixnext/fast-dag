"""Serializable type definitions for fast-dag."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SerializableNode:
    """Serializable representation of a Node."""

    name: str
    function: str  # Module path to function (e.g., "mymodule.myfunc")
    inputs: list[str] | None = None
    outputs: list[str] | None = None
    description: str | None = None
    node_type: str = "STANDARD"
    retry: int | None = None
    retry_delay: float | None = None
    timeout: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SerializableConnection:
    """Serializable representation of a connection between nodes."""

    source: str
    target: str
    output: str | None = None
    input: str | None = None


@dataclass
class SerializableDAG:
    """Serializable representation of a DAG."""

    name: str
    nodes: list[SerializableNode]
    connections: list[SerializableConnection]
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SerializableFSM:
    """Serializable representation of an FSM."""

    name: str
    nodes: list[SerializableNode]
    connections: list[SerializableConnection]
    initial_state: str | None = None
    terminal_states: list[str] = field(default_factory=list)
    max_cycles: int = 1000
    state_transitions: dict[str, dict[str, str]] = field(default_factory=dict)
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SerializableContext:
    """Serializable representation of execution context."""

    results: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class SerializableFSMContext(SerializableContext):
    """Serializable representation of FSM execution context."""

    state_history: list[str] = field(default_factory=list)
    cycle_results: dict[str, list[Any]] = field(default_factory=dict)
    cycle_count: int = 0
