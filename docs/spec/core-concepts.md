# Core Concepts

This document defines the fundamental building blocks of fast-dag: Node, Context, DAG, and FSM.

## Node

A Node represents a unit of work in a workflow. It wraps a Python function and manages its execution, inputs, outputs, and metadata.

### Design Principles

1. **Function-First**: Nodes wrap regular Python functions with minimal overhead
2. **Auto-Introspection**: Automatically infer metadata from function signatures
3. **Type Safety**: Full type hints for IDE support and validation
4. **Flexible Construction**: Multiple ways to create and connect nodes

### Node Types

```python
from dataclasses import dataclass
from typing import Any, Callable
from enum import Enum

class NodeType(Enum):
    STANDARD = "standard"
    CONDITIONAL = "conditional"
    SELECT = "select"
    FSM_STATE = "fsm_state"

@dataclass
class Node:
    func: Callable[..., Any]
    name: str
    inputs: list[str]
    outputs: list[str]
    description: str | None = None
    node_type: NodeType = NodeType.STANDARD
    
    # Connections
    input_connections: dict[str, tuple[str, str]] = field(default_factory=dict)
    output_connections: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
```

### Special Return Types

For control flow, nodes can return special types:

```python
@dataclass
class ConditionalReturn:
    """Boolean branching"""
    condition: bool
    value: Any = None
    true_branch: str | None = None
    false_branch: str | None = None

@dataclass
class SelectReturn:
    """Multi-way branching"""
    branch: str
    value: Any = None

@dataclass
class FSMReturn:
    """State machine transitions"""
    next_state: str | None = None
    value: Any = None
    stop: bool = False
```

### Auto-Introspection

Nodes automatically extract metadata from functions:

```python
def process_data(input_data: dict, threshold: float = 0.5) -> dict:
    """Process data with configurable threshold"""
    return {"result": input_data, "threshold": threshold}

# Auto-inferred:
# - name: "process_data"
# - inputs: ["input_data", "threshold"]
# - outputs: ["result"]
# - description: "Process data with configurable threshold"
```

## Context

Context carries data through workflow execution, storing results and metadata.

### Core Design

```python
@dataclass
class Context:
    """Execution context flowing through workflow"""
    results: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    
    def __getitem__(self, key: str) -> Any:
        """Dict-like access to results"""
        return self.results[key]
    
    def get(self, key: str, default: Any = None) -> Any:
        """Safe result access"""
        return self.results.get(key, default)
```

### FSM Context Extension

For state machines, we extend Context with cycle tracking:

```python
@dataclass
class FSMContext(Context):
    """Extended context for state machines"""
    state_history: list[str] = field(default_factory=list)
    cycle_count: int = 0
    cycle_results: dict[str, list[Any]] = field(default_factory=dict)
    
    def get_latest(self, node_name: str) -> Any:
        """Get most recent result for a node"""
        if node_name in self.cycle_results:
            return self.cycle_results[node_name][-1]
        return self.results.get(node_name)
    
    def get_cycle(self, node_name: str, cycle: int) -> Any:
        """Get result from specific cycle"""
        if node_name in self.cycle_results:
            if 0 <= cycle < len(self.cycle_results[node_name]):
                return self.cycle_results[node_name][cycle]
        return None
```

### Context Injection

Nodes can optionally receive context as a parameter:

```python
# Without context
@workflow.node
def simple_node(data: str) -> str:
    return data.upper()

# With context
@workflow.node
def context_aware_node(data: str, context: Context) -> str:
    previous = context.get("previous_result", "")
    return f"{previous} -> {data}"
```

## DAG (Directed Acyclic Graph)

DAG represents a workflow where data flows from inputs through processing nodes to outputs without cycles.

### Core Structure

```python
@dataclass
class DAG:
    """Directed Acyclic Graph workflow"""
    name: str
    nodes: dict[str, Node] = field(default_factory=dict)
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    
    # Computed properties
    _entry_points: list[str] | None = None
    _execution_order: list[str] | None = None
    _is_validated: bool = False
```

### Key Features

1. **Validation**: Ensures no cycles and proper connections
2. **Topological Sort**: Determines execution order
3. **Parallel Execution**: Identifies independent nodes
4. **Entry Points**: Automatic or manual specification
5. **Step-by-Step Execution**: apart from a `run` method there is also a `step` method that executes a single next step (by getting the context and returning the next context and results) - this should work for both DAG and FSM (for FSM this is esp useful and might be main operating mode)

### Connection Patterns

```python
# Operator overloading
load_data >> process_data >> save_results

# Method chaining
load_data.connect_to(process_data).connect_to(save_results)

# Explicit connection
dag.connect("load_data", "process_data", output="data", input="raw_data")

# Parallel branches
load_data >> [process_a, process_b] >> merge_results
```

It is worth noting here that easiest case is when node has single input and output. Then it is clear what the operator does.
Otherwise the `>>` operator will automatically assume a list of named results (based on output_names provided)

Example:

```python
@workflow.node(outputs=("msg", "result"))
def send_notification(result: dict) -> tuple[str, dict]:
    """Send customer notification"""
    if result["status"] == "success":
        msg = f"Order confirmed! Transaction: {result['transaction_id']}"
    else:
        msg = f"Order failed: {result['reason']}"
    return msg, result
```

Then you can connect it like this:
```python
send_notification.msg >> process_payment
send_notification.result >> send_notification
[send_notification.result, reject_order.result] >> other_node
```

The list operator here combines multiple outputs. If there is a full node with multiple outputs in the list, that will be automatically expanded.

### Execution Modes

1. **Sequential**: Execute nodes in topological order
2. **Parallel**: Execute independent nodes concurrently
3. **Async**: Full asynchronous execution with asyncio

## FSM (Finite State Machine)

FSM extends DAG to support cycles and state-based execution.

### Core Design

```python
@dataclass
class FSM(DAG):
    """Finite State Machine workflow"""
    initial_state: str | None = None
    terminal_states: set[str] = field(default_factory=set)
    max_cycles: int = 1000
    
    # Runtime state
    current_state: str | None = None
    state_transitions: dict[str, dict[str, str]] = field(default_factory=dict)
```

### State Transitions

States transition based on node return values:

```python
@fsm.state
def processing_state(data: dict, context: FSMContext) -> FSMReturn:
    """Process data and decide next state"""
    if data["complete"]:
        return FSMReturn(next_state="final", value=data)
    elif data["error"]:
        return FSMReturn(next_state="error_handler", value=data)
    else:
        return FSMReturn(next_state="processing_state", value=data)
```

### Termination Conditions

FSM execution stops when:
1. Reaching a terminal state
2. Explicit stop signal (`FSMReturn(stop=True)`)
3. Maximum cycles reached
4. Convergence detected (optional)

### Cycle Management

```python
# Access latest result
result = fsm["processing_state"]  # Latest execution

# Access specific cycle
result = fsm["processing_state.5"]  # 5th execution

# Access history
history = context.cycle_results["processing_state"]  # All executions
```

## Relationships

### Node ↔ Context
- Nodes read inputs from context
- Nodes write results to context
- Context optionally injected into nodes

### Node ↔ DAG/FSM
- Workflows contain and manage nodes
- Nodes connected within workflows
- Workflows validate node connections

### DAG ↔ FSM
- FSM extends DAG with cycle support
- Shared execution infrastructure
- FSM adds state tracking

### Context ↔ Execution
- Context created per execution
- Carries results between nodes
- Tracks metrics and metadata

## Type System

All components use modern Python typing:

```python
# Modern compact syntax
str | None              # not Optional[str]
list[str]              # not List[str]
dict[str, Any]         # not Dict[str, Any]
tuple[str, ...]        # not Tuple[str, ...]

# Function types
Callable[[str], dict]  # Function taking str, returning dict
Callable[..., Any]     # Any function signature

# Generic types
T = TypeVar('T')
NodeFunc = Callable[..., T | ConditionalReturn | SelectReturn | FSMReturn]
```

## Performance Considerations

1. **Dataclasses**: Zero overhead vs custom classes
2. **Lazy Evaluation**: Compute properties on demand
3. **Result Caching**: Avoid recomputation
4. **Memory Management**: Clean up intermediate results
5. **Async First**: Built for concurrent execution

## Extensibility Points

1. **Custom Node Types**: Extend NodeType enum
2. **Context Extensions**: Subclass Context for domain needs
3. **Validation Plugins**: Add custom validation rules
4. **Execution Strategies**: Implement custom runners
5. **Serialization Adapters**: Support different formats