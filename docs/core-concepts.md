# Core Concepts

This guide provides an in-depth look at the fundamental building blocks of fast-dag. Understanding these concepts will help you build more powerful and efficient workflows.

## Table of Contents
- [Overview](#overview)
- [Node](#node)
- [Context](#context)
- [DAG (Directed Acyclic Graph)](#dag-directed-acyclic-graph)
- [FSM (Finite State Machine)](#fsm-finite-state-machine)
- [Connections](#connections)
- [Return Types](#return-types)
- [Execution Flow](#execution-flow)

## Overview

fast-dag is built on four core concepts:

1. **Node**: A unit of work that executes a function
2. **Context**: Shared state and metadata during execution
3. **DAG**: A workflow where data flows forward without cycles
4. **FSM**: A stateful workflow that can have cycles

These concepts work together to create flexible, powerful workflows.

## Node

A Node is the fundamental unit of work in fast-dag. It wraps a Python function and manages its execution, inputs, outputs, and metadata.

### Node Anatomy

```python
from fast_dag.core.node import Node
from fast_dag.core.types import NodeType

# A node consists of:
node = Node(
    func=my_function,           # The function to execute
    name="process_data",        # Unique identifier
    inputs=["data", "config"],  # Expected input parameters
    outputs=["result"],         # Output names
    description="Process data", # Human-readable description
    node_type=NodeType.STANDARD,# Type of node
    retry=3,                    # Retry attempts on failure
    retry_delay=1.0,           # Delay between retries
    timeout=30.0,              # Execution timeout
    metadata={"version": "1.0"} # Custom metadata
)
```

### Node Types

fast-dag supports several node types:

```python
class NodeType(Enum):
    STANDARD = "standard"       # Regular computation node
    CONDITIONAL = "conditional" # Boolean branching node
    SELECT = "select"          # Multi-way branching node
    ANY = "any"               # Waits for any input
    ALL = "all"               # Waits for all inputs
    DAG = "dag"               # Nested DAG node
    FSM = "fsm"               # Nested FSM node
```

### Creating Nodes

There are multiple ways to create nodes:

```python
# 1. Using decorators (recommended)
@dag.node
def process_data(data: list) -> dict:
    return {"count": len(data)}

# 2. Manual creation
node = Node(
    func=process_data,
    name="processor"
)
dag.add_node(node)

# 3. Special node types
@dag.condition
def check_value(value: int) -> bool:
    return value > 100

@dag.any()
def first_ready(data: dict) -> str:
    return f"Got: {data}"

@dag.all()
def wait_for_all(data: dict) -> str:
    return f"All inputs: {data}"
```

### Node Lifecycle

During execution, nodes go through these phases:

1. **Validation**: Input requirements checked
2. **Pre-execution**: Optional hook called
3. **Execution**: Function runs with inputs
4. **Post-execution**: Optional hook called, result processed
5. **Error handling**: On failure, retry logic or error hook

```python
def pre_hook(node, inputs):
    print(f"Starting {node.name}")

def post_hook(node, inputs, result):
    print(f"Finished {node.name}")
    return result  # Can modify result

def error_hook(node, inputs, error):
    print(f"Error in {node.name}: {error}")

dag.set_node_hooks(
    "my_node",
    pre_execute=pre_hook,
    post_execute=post_hook,
    on_error=error_hook
)
```

## Context

Context is the shared state that flows through your workflow during execution. It stores results, metadata, and metrics.

### Context Structure

```python
from fast_dag import Context

context = Context()

# Main components:
context.results     # Dict of node results
context.metadata    # Custom metadata
context.metrics     # Execution metrics
```

### Using Context in Nodes

Nodes can access and modify context:

```python
@dag.node
def process_with_context(data: list, context: Context) -> dict:
    # Access previous results
    previous = context.get("previous_node")
    
    # Add metadata
    context.metadata["processed_count"] = len(data)
    
    # Access metrics
    execution_time = context.metrics.get("node_times", {})
    
    return {"processed": data, "previous": previous}
```

### FSMContext

FSMs use an extended context that tracks state history:

```python
from fast_dag import FSMContext

fsm_context = FSMContext()

# Additional FSM-specific attributes:
fsm_context.state_history    # List of visited states
fsm_context.cycle_count      # Number of cycles executed
fsm_context.cycle_results    # Results by cycle
```

### Context Best Practices

1. **Don't Overuse**: Pass data through connections when possible
2. **Metadata Only**: Use context.metadata for cross-cutting concerns
3. **Immutable Values**: Avoid modifying shared data structures
4. **Clear Naming**: Use descriptive keys for metadata

## DAG (Directed Acyclic Graph)

A DAG represents a workflow where:
- Data flows in one direction (no cycles)
- Each node executes at most once
- Dependencies are explicit

### DAG Properties

```python
from fast_dag import DAG

dag = DAG(
    name="etl_pipeline",
    description="Extract, transform, load pipeline",
    metadata={"owner": "data_team", "sla": "1h"}
)

# Key properties:
dag.nodes           # Dictionary of all nodes
dag.entry_points    # Nodes with no dependencies
dag.execution_order # Topological sort of nodes
dag.is_valid        # Check if DAG is valid
```

### Building DAGs

```python
# Create a DAG
dag = DAG("my_workflow")

# Add nodes
@dag.node
def extract():
    return fetch_data()

@dag.node
def transform(data):
    return clean_data(data)

@dag.node
def load(clean_data):
    save_to_db(clean_data)

# Connect nodes
dag.connect("extract", "transform", input="data")
dag.connect("transform", "load", input="clean_data")

# Or use operators
dag.nodes["extract"] >> dag.nodes["transform"] >> dag.nodes["load"]
```

### DAG Validation

DAGs are validated for:
- **No cycles**: Would create infinite loops
- **Connected components**: All nodes reachable (unless allowed)
- **Input satisfaction**: All required inputs have sources
- **Type compatibility**: Output/input types match (optional)

```python
# Validate
errors = dag.validate()
if errors:
    raise ValidationError(f"DAG invalid: {errors}")

# Or validate and raise
dag.validate_or_raise()
```

### Execution Order

DAGs use topological sorting to determine execution order:

```python
# Get execution order
order = dag.get_execution_order()
print(order)  # ['extract', 'transform', 'load']

# Nodes at the same level can run in parallel
```

## FSM (Finite State Machine)

An FSM represents a stateful workflow where:
- States can be revisited (cycles allowed)
- Execution continues until a terminal state
- State transitions are explicit

### FSM Properties

```python
from fast_dag import FSM

fsm = FSM(
    name="order_state_machine",
    initial_state="pending",          # Starting state
    terminal_states={"completed", "cancelled"},  # End states
    max_cycles=100,                   # Maximum iterations
    metadata={"version": "2.0"}
)

# Key properties:
fsm.current_state    # Current execution state
fsm.state_history    # History of state transitions
fsm.is_terminated    # Whether FSM has stopped
```

### Building FSMs

```python
# Create an FSM
fsm = FSM("traffic_light")

# Define states
@fsm.state(initial=True)
def red() -> FSMReturn:
    print("Red light")
    return FSMReturn(next_state="green")

@fsm.state
def green() -> FSMReturn:
    print("Green light")
    return FSMReturn(next_state="yellow")

@fsm.state
def yellow() -> FSMReturn:
    print("Yellow light")
    return FSMReturn(next_state="red")

# No explicit connections needed - FSMReturn handles transitions
```

### State Transitions

States control their transitions through FSMReturn:

```python
@fsm.state
def processing(data: dict) -> FSMReturn:
    if data["status"] == "complete":
        return FSMReturn(
            next_state="success",
            value={"result": data["result"]}
        )
    elif data["retries"] > 3:
        return FSMReturn(
            next_state="failed",
            value={"error": "Max retries exceeded"}
        )
    else:
        return FSMReturn(
            next_state="processing",  # Stay in same state
            value={**data, "retries": data["retries"] + 1}
        )
```

### Terminal States

Terminal states end the FSM execution:

```python
@fsm.state(terminal=True)
def completed(data: dict) -> FSMReturn:
    print(f"Process completed: {data}")
    return FSMReturn(stop=True, value="Success")

# Or mark terminal after definition
fsm.add_terminal_state("failed")
```

## Connections

Connections define how data flows between nodes.

### Connection Types

```python
# 1. Simple connection (auto-detect input)
dag.connect("source", "target")

# 2. Explicit input mapping
dag.connect("source", "target", input="data")

# 3. Output selection
dag.connect("source", "target", output="result", input="data")

# 4. Operator syntax
source_node >> target_node

# 5. Conditional connections
condition_node.on_true >> true_branch
condition_node.on_false >> false_branch

# 6. Multi-input connections (for ANY/ALL nodes)
node_a >> collector
node_b >> collector
node_c >> collector
```

### Connection Rules

1. **Single Input**: Standard nodes accept one connection per input
2. **Multiple Outputs**: Nodes can connect to multiple targets
3. **Type Safety**: Connections can be validated for type compatibility
4. **Named Ports**: Use specific input/output names for clarity

```python
# Example with named ports
@dag.node
def split_data(data: list) -> dict:
    return {
        "train": data[:80],
        "test": data[80:]
    }

@dag.node
def train_model(train_data: list) -> Model:
    return fit_model(train_data)

@dag.node
def test_model(test_data: list, model: Model) -> float:
    return evaluate(model, test_data)

# Connect specific outputs
dag.connect("split_data", "train_model", output="train", input="train_data")
dag.connect("split_data", "test_model", output="test", input="test_data")
dag.connect("train_model", "test_model", input="model")
```

## Return Types

Special return types control workflow behavior:

### ConditionalReturn

For boolean branching:

```python
from fast_dag import ConditionalReturn

@dag.condition
def check_quality(data: dict) -> ConditionalReturn:
    quality_score = calculate_quality(data)
    return ConditionalReturn(
        condition=quality_score > 0.8,
        value={"score": quality_score, "data": data}
    )
```

### SelectReturn

For multi-way branching:

```python
from fast_dag import SelectReturn

@dag.select
def route_request(request: dict) -> SelectReturn:
    return SelectReturn(
        branch=request["type"],  # "email", "sms", "push"
        value=request["payload"]
    )

# Connect branches
dag.nodes["route_request"].branches["email"] >> dag.nodes["send_email"]
dag.nodes["route_request"].branches["sms"] >> dag.nodes["send_sms"]
dag.nodes["route_request"].branches["push"] >> dag.nodes["send_push"]
```

### FSMReturn

For state transitions:

```python
from fast_dag import FSMReturn

@fsm.state
def process_payment(order: dict) -> FSMReturn:
    result = payment_api.charge(order["amount"])
    
    if result.success:
        return FSMReturn(
            next_state="fulfillment",
            value={"order": order, "payment_id": result.id}
        )
    else:
        return FSMReturn(
            next_state="payment_retry",
            value={"order": order, "error": result.error}
        )
```

## Execution Flow

Understanding how fast-dag executes workflows:

### DAG Execution Flow

1. **Validation**: Structure and connections checked
2. **Initialization**: Context created, metrics initialized
3. **Topological Sort**: Execution order determined
4. **Node Execution**:
   - Entry points execute first
   - Nodes execute when dependencies are ready
   - Results stored in context
5. **Completion**: Final result returned

```python
# Execution timeline
# Time  Nodes
# T0:   [load_data]           # Entry point
# T1:   [validate, enrich]    # Parallel execution
# T2:   [combine]             # Waits for both
# T3:   [save]                # Final node
```

### FSM Execution Flow

1. **Validation**: Initial state and terminals checked
2. **Initialization**: FSMContext created with state tracking
3. **State Execution**:
   - Start from initial state
   - Execute state function
   - Transition based on FSMReturn
   - Record in state history
4. **Termination**: Stop on terminal state or max cycles

```python
# State progression
# Cycle  State        Next
# 0:     pending   -> processing
# 1:     processing -> processing  # Retry
# 2:     processing -> validated
# 3:     validated -> completed    # Terminal
```

### Parallel Execution

In parallel mode, independent nodes run concurrently:

```python
# These nodes have no dependencies on each other
@dag.node
def fetch_api():
    return api_call()

@dag.node  
def fetch_db():
    return db_query()

@dag.node
def fetch_file():
    return read_file()

# They'll run in parallel when:
result = dag.run(mode="parallel", max_workers=4)
```

### Async Execution

For I/O-bound operations:

```python
@dag.node
async def async_fetch(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

# Run asynchronously
result = await dag.run_async()
```

## Summary

These core concepts work together to create powerful workflows:

- **Nodes** encapsulate work and can be composed
- **Context** provides shared state and metrics
- **DAGs** ensure forward-only data flow
- **FSMs** handle stateful, cyclic processes
- **Connections** define explicit data dependencies
- **Return Types** control execution flow

Understanding these concepts deeply will help you:
- Design better workflows
- Debug issues more easily
- Optimize performance
- Build reusable components

Next, explore [Building Workflows](building-workflows.md) to see these concepts in action!