"""Core type definitions for fast-dag."""

from dataclasses import dataclass
from enum import Enum
from typing import Any


class NodeType(Enum):
    """Types of nodes in a workflow."""

    STANDARD = "standard"
    CONDITIONAL = "conditional"
    SELECT = "select"
    FSM_STATE = "fsm_state"
    ANY = "any"
    ALL = "all"


@dataclass
class ConditionalReturn:
    """Return type for conditional branching nodes.

    Used to indicate which branch should be taken based on a condition.
    """

    condition: bool
    value: Any = None
    true_branch: str | None = None
    false_branch: str | None = None


@dataclass
class SelectReturn:
    """Return type for multi-way branching nodes.

    Used to select one of many possible branches.
    """

    branch: str
    value: Any = None


@dataclass
class FSMReturn:
    """Return type for FSM state nodes.

    Used to indicate state transitions and termination.
    """

    next_state: str | None = None
    value: Any = None
    stop: bool = False
