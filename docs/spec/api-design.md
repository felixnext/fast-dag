# API Design

This document specifies the public APIs for constructing, connecting, and executing workflows in fast-dag.

## Design Principles

1. **Multiple Paradigms**: Support decorators, operators, and explicit APIs
2. **Progressive Disclosure**: Simple things simple, complex things possible
3. **Type Safety**: Full typing for IDE support and early error detection
4. **Intuitive Naming**: Inspired by FastAPI, Keras, and NetworkX

## Workflow Construction

### Decorator-Based API

The primary API uses decorators for elegant workflow definition:

```python
from fast_dag import DAG, FSM

# Create workflow
dag = DAG("data_pipeline")

# Add nodes with decorator
@dag.node
def load_data(file_path: str) -> dict:
    """Load data from file"""
    return {"data": "content"}

@dag.node(inputs=["data"], outputs=["processed"])
def process_data(data: dict) -> dict:
    """Process the data"""
    return {"processed": transform(data)}

# Conditional node
@dag.condition()
def check_quality(processed: dict) -> ConditionalReturn:
    """Branch based on quality check"""
    quality = calculate_quality(processed)
    return ConditionalReturn(
        condition=quality > 0.8,
        value=processed
    )
```

### Manual Construction

For programmatic workflow generation:

```python
# Create nodes manually
node1 = Node(
    func=load_func,
    name="loader",
    inputs=["path"],
    outputs=["data"]
)

node2 = Node(
    func=process_func,
    name="processor",
    inputs=["data"],
    outputs=["result"]
)

# Add to workflow
dag.add_node(node1)
dag.add_node(node2)
dag.connect("loader", "processor")
```

### Builder Pattern

For complex workflows:

```python
dag = (DAG("pipeline")
    .add_node("load", load_data)
    .add_node("validate", validate_data)
    .add_node("process", process_data)
    .connect("load", "validate")
    .connect("validate", "process")
    .set_entry_point("load")
)
```

## Connection APIs

### Operator Overloading

Intuitive connection syntax using operators:

```python
# Sequential connection
load >> validate >> process >> save

# Parallel branching
load >> [process_a, process_b] >> merge

# Conditional branching
validate >> check_condition
check_condition.true >> process_valid
check_condition.false >> handle_error

# Alternative operators
load | validate | process  # Pipe style
```

### Method Chaining

Fluent interface for connections:

```python
load.connect_to(validate).connect_to(process)

# With specific outputs/inputs
load.output("data").connect_to(process.input("raw_data"))
```

### Explicit Connection

Full control over connections:

```python
# Simple connection
dag.connect("load", "process")

# Specific input/output mapping
dag.connect(
    from_node="load",
    to_node="process",
    output="data",
    input="raw_data"
)

# Multiple connections
dag.connect_many([
    ("load", "validate"),
    ("validate", "process"),
    ("process", "save")
])
```

## Execution APIs

### Direct Execution

Simple execution with automatic runner:

```python
# Synchronous execution
result = dag.run(file_path="data.csv")

# Async execution
result = await dag.run_async(file_path="data.csv")

# With options
result = dag.run(
    inputs={"file_path": "data.csv"},
    mode="parallel",
    timeout=60.0
)
```

### Runner-Based Execution

Fine-grained control with Runner:

```python
from fast_dag import Runner, ExecutionMode

# Create runner
runner = Runner(dag)

# Configure execution
runner.configure(
    mode=ExecutionMode.PARALLEL,
    max_workers=4,
    timeout=300,
    error_strategy="continue"
)

# Execute with context
context = Context(metadata={"run_id": "123"})
metrics = runner.run(
    inputs={"file_path": "data.csv"},
    context=context
)

# Access results
print(metrics.duration)
print(metrics.node_times)
print(context.results)
```

### Async Execution

Native async support:

```python
# Async node definition
@dag.node
async def fetch_data(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

# Async execution
async def main():
    result = await dag.run_async(url="https://api.example.com")
    
# Parallel async execution
async def parallel_runs():
    tasks = [
        dag.run_async(url=url) 
        for url in urls
    ]
    results = await asyncio.gather(*tasks)
```

## Result Access

### Dictionary-Style Access

Intuitive result retrieval:

```python
# After execution
result = dag.run(inputs={...})

# Access by node name
data = dag["load_data"]
processed = dag["process_data"]

# Safe access
data = dag.get("load_data", default=None)

# Check existence
if "process_data" in dag:
    result = dag["process_data"]
```

### Context Access

Direct context manipulation:

```python
# Get context after execution
context = dag.context

# Access results
all_results = context.results
specific = context.get("node_name")

# Access metadata
metrics = context.metrics
metadata = context.metadata
```

### FSM-Specific Access

State machine result patterns:

```python
# Latest result
latest = fsm["process_state"]

# Specific cycle
cycle_5 = fsm["process_state.5"]

# All cycles
history = fsm.get_history("process_state")

# State info
current = fsm.current_state
history = fsm.state_history
```

## Validation APIs

### Pre-Execution Validation

```python
# Validate before execution
errors = dag.validate()
if errors:
    for error in errors:
        print(f"Validation error: {error}")

# Validate and raise
dag.validate_or_raise()  # Raises ValidationError

# Check specific aspects
is_acyclic = dag.is_acyclic()
has_entry = dag.has_entry_points()
is_connected = dag.is_fully_connected()
```

### Connection Validation

```python
# Check if connection is valid
can_connect = dag.can_connect("node1", "node2")

# Get connection issues
issues = dag.check_connection("node1", "node2")

# Validate node compatibility
compatible = dag.check_compatibility("node1", "output1", "node2", "input1")
```

## Advanced APIs

### Subworkflow Composition

```python
# Use workflow as node
sub_dag = DAG("preprocessing")
# ... define sub_dag

main_dag = DAG("main_pipeline")

# Add as node
@main_dag.node
def preprocess(data: dict) -> dict:
    return sub_dag.run(data=data)

# Or explicitly
main_dag.add_subworkflow("preprocess", sub_dag)
```

### Dynamic Node Creation

```python
# Create nodes dynamically
for i in range(5):
    @dag.node(name=f"processor_{i}")
    def process(data: dict) -> dict:
        return transform(data, step=i)

# Connect dynamically
for i in range(4):
    dag.connect(f"processor_{i}", f"processor_{i+1}")
```

### Middleware/Hooks

```python
# Add execution hooks
@dag.before_node
def log_start(node_name: str, inputs: dict):
    logger.info(f"Starting {node_name}")

@dag.after_node
def log_complete(node_name: str, result: Any, duration: float):
    logger.info(f"Completed {node_name} in {duration}s")

# Error handling
@dag.on_error
def handle_error(node_name: str, error: Exception):
    logger.error(f"Error in {node_name}: {error}")
    # Return value determines if execution continues
    return "continue"  # or "stop" or "retry"
```

### Visualization

```python
# Generate diagrams - basic
mermaid_code = dag.to_mermaid()
dag.save_mermaid("workflow.mmd")

# Generate diagrams with execution context
context = dag.run(inputs={...})
mermaid_code = dag.to_mermaid(context=context)
dag.save_mermaid("workflow_executed.mmd", context=context)

# Graphviz
dot = dag.to_graphviz()
dag.save_graphviz("workflow.png", format="png")

# Graphviz with execution results
dag.save_graphviz("workflow_executed.png", context=context, format="png")

# HTML export with execution results
dag.export_html("workflow_results.html", context=context, title="Execution Results")

# Visualize with options
dag.visualize(
    context=context,
    show_results=True,
    show_timing=True,
    highlight_errors=True
)
```

## Configuration APIs

### Workflow Configuration

```python
# Configure workflow behavior
dag.configure(
    max_parallel=4,
    timeout=300,
    retry_policy={
        "max_attempts": 3,
        "backoff": "exponential"
    },
    error_handling="continue"
)

# FSM-specific
fsm.configure(
    max_cycles=1000,
    convergence_check=True,
    state_timeout=60
)
```

### Node Configuration

```python
# Configure individual nodes
@dag.node(
    retry=3,
    timeout=30,
    cache=True,
    tags=["io", "slow"]
)
def fetch_data(url: str) -> dict:
    ...

# Runtime configuration
dag.configure_node(
    "fetch_data",
    retry=5,
    timeout=60
)
```

## Error Handling

### Error Strategies

```python
from fast_dag import ErrorStrategy

# Global error handling
dag.run(
    inputs={...},
    error_strategy=ErrorStrategy.CONTINUE  # STOP, RETRY, IGNORE
)

# Per-node error handling
@dag.node(on_error="retry")
def flaky_operation(data: dict) -> dict:
    ...

# Custom error handlers
def custom_handler(error: Exception, context: Context) -> str:
    if isinstance(error, NetworkError):
        return "retry"
    return "stop"

dag.set_error_handler(custom_handler)
```

## Type Hints

All APIs use modern Python type hints:

```python
from typing import Any, TypeVar, Protocol

T = TypeVar('T')

class NodeProtocol(Protocol):
    """Protocol for node functions"""
    def __call__(self, *args: Any, **kwargs: Any) -> T: ...

class ExecutionResult:
    """Result of workflow execution"""
    success: bool
    results: dict[str, Any]
    errors: list[tuple[str, Exception]]
    metrics: ExecutionMetrics
```

## API Stability

### Stable APIs
- Core decorators (`@dag.node`, `@fsm.state`)
- Connection operators (`>>`, `|`)
- Execution methods (`run`, `run_async`)
- Result access patterns

### Experimental APIs
- Distributed execution
- Advanced caching strategies
- Custom execution engines

### Deprecation Policy
- 2 version warning period
- Clear migration guides
- Compatibility shims when possible