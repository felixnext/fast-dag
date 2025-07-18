# API Reference

Complete API documentation for fast-dag classes, methods, and functions.

## Table of Contents

- [Core Classes](#core-classes)
  - [DAG](#dag)
  - [FSM](#fsm)
  - [Node](#node)
  - [Context](#context)
- [Node Types](#node-types)
  - [Standard Node](#standard-node)
  - [Conditional Node](#conditional-node)
  - [Select Node](#select-node)
  - [Multi-Input Nodes](#multi-input-nodes)
- [Return Types](#return-types)
- [Decorators](#decorators)
- [Execution](#execution)
- [Serialization](#serialization)
- [Visualization](#visualization)
- [Utilities](#utilities)

## Core Classes

### DAG

The main class for creating Directed Acyclic Graph workflows.

```python
class DAG(name: str, description: str = None, metadata: dict = None)
```

#### Parameters
- `name` (str): Unique name for the DAG
- `description` (str, optional): Human-readable description
- `metadata` (dict, optional): Additional metadata

#### Properties
- `nodes` (dict[str, Node]): All nodes in the DAG
- `context` (Context): Execution context
- `is_valid` (bool): Whether DAG structure is valid
- `execution_order` (list[str]): Topological sort of nodes

#### Methods

##### add_node
```python
def add_node(
    node: Node | str,
    func: Callable = None,
    *,
    inputs: list[str] = None,
    outputs: list[str] = None,
    description: str = None,
    node_type: NodeType = NodeType.STANDARD,
    retry: int = None,
    retry_delay: float = None,
    timeout: float = None,
    metadata: dict[str, Any] = None,
) -> DAG
```

Add a node to the DAG.

##### connect
```python
def connect(
    from_node: str,
    to_node: str,
    *,
    output: str = None,
    input: str = None
) -> DAG
```

Connect two nodes with optional port specification.

##### run
```python
def run(
    *,
    inputs: dict[str, Any] = None,
    context: Context = None,
    mode: str = "sequential",
    max_workers: int = None,
    error_strategy: str = "fail_fast",
    **kwargs
) -> Any
```

Execute the DAG.

**Parameters:**
- `inputs`: Initial input values
- `context`: Execution context (created if not provided)
- `mode`: Execution mode ("sequential", "parallel", "async")
- `max_workers`: Maximum parallel workers
- `error_strategy`: How to handle errors ("fail_fast", "continue")

##### validate
```python
def validate() -> list[str]
```

Validate DAG structure. Returns list of validation errors.

##### visualize
```python
def visualize(
    backend: str = "mermaid",
    filename: str = None,
    format: str = "png",
    **kwargs
) -> str | None
```

Generate visualization of the DAG.

##### get_node
```python
def get_node(name: str) -> Node
```

Get a node by name.

##### get_entry_points
```python
def get_entry_points() -> list[str]
```

Get nodes with no dependencies.

##### get_execution_order
```python
def get_execution_order() -> list[str]
```

Get topological ordering of nodes.

#### Decorators

##### node
```python
@dag.node(name: str = None, **kwargs)
def my_function(...): ...
```

Convert a function to a DAG node.

##### condition
```python
@dag.condition(name: str = None, **kwargs)
def my_condition(...) -> bool | ConditionalReturn: ...
```

Create a conditional node.

##### select
```python
@dag.select(name: str = None, **kwargs)
def my_router(...) -> SelectReturn: ...
```

Create a select/router node.

##### any
```python
@dag.any(name: str = None, **kwargs)
def process_any(...) -> Any: ...
```

Create a node that processes any available input.

##### all
```python
@dag.all(name: str = None, **kwargs)
def process_all(...) -> Any: ...
```

Create a node that waits for all inputs.

##### cached_node
```python
@dag.cached_node(ttl: int, cache_key: list[str] = None, **kwargs)
def expensive_op(...) -> Any: ...
```

Create a cached node.

#### Special Methods

##### __getitem__
```python
dag["node_name"]  # Get result of node_name
```

Access node results after execution.

##### __len__
```python
len(dag)  # Number of nodes
```

Get number of nodes in DAG.

##### __iter__
```python
for node_name in dag:
    # Iterate over node names
```

Iterate over node names.

### FSM

Finite State Machine for stateful workflows.

```python
class FSM(
    name: str,
    initial_state: str = None,
    terminal_states: set[str] = None,
    max_cycles: int = 100,
    description: str = None,
    metadata: dict = None
)
```

#### Parameters
- `name` (str): FSM name
- `initial_state` (str, optional): Starting state
- `terminal_states` (set[str], optional): States that end execution
- `max_cycles` (int): Maximum execution cycles
- `description` (str, optional): Description
- `metadata` (dict, optional): Additional metadata

#### Properties
- `current_state` (str): Current execution state
- `state_history` (list[str]): History of state transitions
- `is_terminated` (bool): Whether FSM has reached terminal state
- `cycle_count` (int): Number of cycles executed

#### Methods

##### state
```python
@fsm.state(initial: bool = False, terminal: bool = False)
def state_name(...) -> FSMReturn: ...
```

Define a state function.

##### add_state
```python
def add_state(
    name: str,
    func: Callable,
    initial: bool = False,
    terminal: bool = False,
    metadata: dict = None
) -> FSM
```

Add a state to the FSM.

##### add_terminal_state
```python
def add_terminal_state(state_name: str) -> FSM
```

Mark a state as terminal.

##### run
```python
def run(
    *,
    context: FSMContext = None,
    max_cycles: int = None,
    **initial_inputs
) -> Any
```

Execute the FSM.

##### get_result
```python
def get_result(state_cycle: str) -> Any
```

Get result from specific state and cycle (e.g., "process.2" for 3rd cycle).

### Node

Base class for workflow nodes.

```python
class Node(
    func: Callable,
    name: str,
    inputs: list[str] = None,
    outputs: list[str] = None,
    description: str = None,
    node_type: NodeType = NodeType.STANDARD,
    retry: int = None,
    retry_delay: float = None,
    timeout: float = None,
    metadata: dict[str, Any] = None
)
```

#### Properties
- `func` (Callable): Function to execute
- `name` (str): Unique node name
- `inputs` (list[str]): Expected input parameter names
- `outputs` (list[str]): Output names
- `node_type` (NodeType): Type of node
- `description` (str): Node description
- `retry` (int): Number of retry attempts
- `retry_delay` (float): Delay between retries
- `timeout` (float): Execution timeout
- `metadata` (dict): Additional metadata

#### Methods

##### execute
```python
def execute(inputs: dict[str, Any], context: Context) -> Any
```

Execute the node function.

##### validate_inputs
```python
def validate_inputs(inputs: dict[str, Any]) -> list[str]
```

Validate inputs match expected parameters.

##### get_connections
```python
def get_connections() -> dict[str, list[Connection]]
```

Get incoming and outgoing connections.

### Context

Execution context that flows through the workflow.

```python
class Context(
    results: dict[str, Any] = None,
    metadata: dict[str, Any] = None,
    metrics: dict[str, Any] = None
)
```

#### Properties
- `results` (dict[str, Any]): Node execution results
- `metadata` (dict[str, Any]): Workflow metadata
- `metrics` (dict[str, Any]): Execution metrics

#### Methods

##### get
```python
def get(node_name: str, default: Any = None) -> Any
```

Get result from a node.

##### set
```python
def set(node_name: str, value: Any) -> None
```

Set result for a node.

##### update_metrics
```python
def update_metrics(node_name: str, metrics: dict) -> None
```

Update metrics for a node.

##### to_dict
```python
def to_dict() -> dict[str, Any]
```

Convert context to dictionary.

## Node Types

### Standard Node

Basic computation node.

```python
@dag.node
def process(data: dict) -> dict:
    return transform(data)
```

### Conditional Node

Node that branches based on boolean condition.

```python
@dag.condition
def check_threshold(value: float) -> bool:
    return value > 100

# Or with ConditionalReturn
@dag.condition
def check_with_data(value: float) -> ConditionalReturn:
    return ConditionalReturn(
        condition=value > 100,
        value={"original": value, "threshold": 100}
    )
```

**Properties:**
- `on_true`: Connection point for true branch
- `on_false`: Connection point for false branch

### Select Node

Node that routes to multiple branches.

```python
@dag.select
def route_by_type(message: dict) -> SelectReturn:
    return SelectReturn(
        branch=message["type"],
        value=message["payload"]
    )
```

**Properties:**
- `branches`: Dictionary of branch connections

### Multi-Input Nodes

#### ANY Node

Processes first available input.

```python
@dag.any()
def process_first(data: dict) -> Any:
    # data contains {"source_name": source_value}
    source = list(data.keys())[0]
    return process(data[source])
```

#### ALL Node

Waits for all connected inputs.

```python
@dag.all()
def combine_all(data: dict) -> Any:
    # data contains all inputs: {"input1": value1, "input2": value2, ...}
    return merge_data(data)
```

## Return Types

### ConditionalReturn

Return type for conditional nodes.

```python
class ConditionalReturn:
    condition: bool  # Branch decision
    value: Any      # Value to pass forward
```

### SelectReturn

Return type for select nodes.

```python
class SelectReturn:
    branch: str     # Branch name to route to
    value: Any      # Value to pass forward
```

### FSMReturn

Return type for FSM states.

```python
class FSMReturn:
    next_state: str = None  # Next state to transition to
    stop: bool = False      # Whether to stop execution
    value: Any = None       # Value to pass forward
```

## Decorators

### @node

```python
@dag.node(
    name: str = None,
    inputs: list[str] = None,
    outputs: list[str] = None,
    description: str = None,
    retry: int = None,
    retry_delay: float = None,
    timeout: float = None,
    metadata: dict = None
)
```

Convert function to DAG node.

### @condition

```python
@dag.condition(name: str = None, **kwargs)
```

Create conditional branching node.

### @select

```python
@dag.select(name: str = None, **kwargs)
```

Create multi-branch routing node.

### @any

```python
@dag.any(name: str = None, **kwargs)
```

Create node that processes any available input.

### @all

```python
@dag.all(name: str = None, **kwargs)
```

Create node that waits for all inputs.

### @cached_node

```python
@dag.cached_node(
    ttl: int,
    cache_key: list[str] = None,
    backend: CacheBackend = None,
    **kwargs
)
```

Create cached node with TTL.

### @state

```python
@fsm.state(
    initial: bool = False,
    terminal: bool = False,
    metadata: dict = None
)
```

Define FSM state.

## Execution

### Execution Modes

#### Sequential
```python
result = dag.run(mode="sequential")
```

Execute nodes one at a time in order.

#### Parallel
```python
result = dag.run(mode="parallel", max_workers=4)
```

Execute independent nodes concurrently.

#### Async
```python
result = await dag.run_async()
```

Execute with asyncio for I/O-bound operations.

### Error Strategies

#### Fail Fast (default)
```python
result = dag.run(error_strategy="fail_fast")
```

Stop on first error.

#### Continue
```python
result = dag.run(error_strategy="continue")
```

Continue execution despite errors.

### Partial Execution

```python
# Run from specific node
result = dag.run_from("node_name")

# Run until specific node
result = dag.run_until("node_name")

# Run between nodes
result = dag.run_between("start_node", "end_node")
```

## Serialization

### JSON

```python
# Save
dag.to_json("workflow.json")
json_str = dag.to_json()

# Load
dag = DAG.from_json("workflow.json")
dag = DAG.from_json(json_str)
```

### YAML

```python
# Save
dag.to_yaml("workflow.yaml")
yaml_str = dag.to_yaml()

# Load
dag = DAG.from_yaml("workflow.yaml")
dag = DAG.from_yaml(yaml_str)
```

### MsgSpec (Binary)

```python
# Save
dag.to_msgspec("workflow.msgspec")
bytes_data = dag.to_msgspec()

# Load
dag = DAG.from_msgspec("workflow.msgspec")
dag = DAG.from_msgspec(bytes_data)
```

### Dictionary

```python
# Convert to dict
dag_dict = dag.to_dict()

# Create from dict
dag = DAG.from_dict(dag_dict)
```

## Visualization

### Mermaid

```python
mermaid_code = dag.visualize(
    backend="mermaid",
    show_results=True,
    show_execution_time=True,
    node_style={"process": "fill:#f9f"},
    edge_style={("load", "process"): "stroke:#ff3"}
)
```

### Graphviz

```python
dag.visualize(
    backend="graphviz",
    filename="workflow",
    format="png",  # png, pdf, svg, dot
    engine="dot",  # dot, neato, fdp, sfdp, twopi, circo
    graph_attr={"rankdir": "LR"},
    node_attr={"shape": "box"},
    edge_attr={"color": "gray"}
)
```

### Custom Visualization

```python
# Get visualization data
viz_data = dag.get_visualization_data()

# Contains:
# - nodes: List of node information
# - edges: List of connections
# - metrics: Execution metrics (if available)
# - layout: Suggested layout information
```

## Utilities

### Validation

```python
# Validate structure
errors = dag.validate()
if errors:
    for error in errors:
        print(f"Validation error: {error}")

# Validate and raise
try:
    dag.validate_or_raise()
except ValidationError as e:
    print(f"Invalid DAG: {e}")
```

### Metrics

```python
# Get execution metrics
metrics = dag.get_metrics()
print(f"Total time: {metrics['total_time']}")
print(f"Node times: {metrics['node_times']}")

# Get node-specific metrics
node_metrics = dag.get_node_metrics("process_data")
print(f"Execution time: {node_metrics['execution_time']}")
print(f"Memory usage: {node_metrics['memory_usage']}")
```

### Hooks

```python
# Set node hooks
dag.set_node_hooks(
    "node_name",
    pre_execute=lambda node, inputs: print(f"Starting {node.name}"),
    post_execute=lambda node, inputs, result: result,
    on_error=lambda node, inputs, error: print(f"Error: {error}")
)

# Set global hooks
dag.set_global_hooks(
    pre_execute=pre_execute_func,
    post_execute=post_execute_func,
    on_error=error_handler_func
)
```

### Type Checking

```python
# Enable type checking
dag.enable_type_checking()

# Type mismatches will raise TypeValidationError
try:
    dag.run(inputs={"number": "not a number"})  # Expects int
except TypeValidationError as e:
    print(f"Type error: {e}")
```

### Debugging

```python
# Enable debug mode
dag.set_debug(True)

# Debug information includes:
# - Detailed execution logs
# - Input/output values for each node
# - Connection resolution details
# - Performance profiling

# Get debug info after execution
debug_info = dag.get_debug_info()
```

## Type Definitions

### NodeType Enum

```python
class NodeType(Enum):
    STANDARD = "standard"
    CONDITIONAL = "conditional"
    SELECT = "select"
    ANY = "any"
    ALL = "all"
    DAG = "dag"
    FSM = "fsm"
```

### Connection

```python
@dataclass
class Connection:
    from_node: str
    to_node: str
    from_output: str = None
    to_input: str = None
```

### ExecutionMode

```python
class ExecutionMode(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ASYNC = "async"
```

### ErrorStrategy

```python
class ErrorStrategy(Enum):
    FAIL_FAST = "fail_fast"
    CONTINUE = "continue"
    RETRY = "retry"
```

## Exceptions

### ValidationError

Raised when DAG/FSM structure is invalid.

```python
class ValidationError(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors
```

### ExecutionError

Raised during execution failures.

```python
class ExecutionError(Exception):
    def __init__(self, node_name: str, original_error: Exception):
        self.node_name = node_name
        self.original_error = original_error
```

### CircularDependencyError

Raised when circular dependencies detected.

```python
class CircularDependencyError(ValidationError):
    def __init__(self, cycle: list[str]):
        self.cycle = cycle
```

### TypeValidationError

Raised when type checking fails.

```python
class TypeValidationError(Exception):
    def __init__(self, node_name: str, param: str, expected: type, actual: type):
        self.node_name = node_name
        self.param = param
        self.expected = expected
        self.actual = actual
```

## Advanced Usage

### Custom Executors

```python
from fast_dag.execution import Executor

class CustomExecutor(Executor):
    def execute_node(self, node: Node, inputs: dict, context: Context) -> Any:
        # Custom execution logic
        return super().execute_node(node, inputs, context)

# Use custom executor
result = dag.run(executor=CustomExecutor())
```

### Custom Cache Backends

```python
from fast_dag.caching import CacheBackend

class RedisCache(CacheBackend):
    def get(self, key: str) -> Any:
        # Implement get
        pass
    
    def set(self, key: str, value: Any, ttl: int = None):
        # Implement set
        pass
    
    def invalidate(self, key: str):
        # Implement invalidate
        pass
```

### Custom Serializers

```python
from fast_dag.serialization import register_serializer

def serialize_custom(obj: CustomType) -> dict:
    return {"data": obj.data, "_type": "CustomType"}

def deserialize_custom(data: dict) -> CustomType:
    return CustomType(data["data"])

register_serializer(CustomType, serialize_custom, deserialize_custom)
```

---

For more examples and patterns, see the [Examples Gallery](examples/README.md).