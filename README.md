# fast-dag

<p align="center">
  <a href="https://pypi.org/project/fast-dag/"><img src="https://img.shields.io/pypi/v/fast-dag.svg" alt="PyPI version"></a>
  <a href="https://pypi.org/project/fast-dag/"><img src="https://img.shields.io/pypi/pyversions/fast-dag.svg" alt="Python versions"></a>
  <a href="https://github.com/felixnext/fast-dag/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
  <a href="https://github.com/felixnext/fast-dag/actions"><img src="https://github.com/felixnext/fast-dag/workflows/CI/badge.svg" alt="CI"></a>
</p>

**fast-dag** is a lightweight, high-performance Python library for building and executing Directed Acyclic Graph (DAG) and Finite State Machine (FSM) workflows. It's designed with simplicity and speed in mind, making it perfect for data pipelines, task orchestration, and complex workflow automation.

## ✨ Key Features

- 🚀 **Fast & Lightweight** - Minimal dependencies, optimized for performance
- 🔄 **DAG & FSM Support** - Build both directed acyclic graphs and state machines
- 🎯 **Type-Safe** - Full type hints and mypy support
- 🔌 **Extensible** - Easy to add custom nodes and behaviors
- 📊 **Visualization** - Built-in Mermaid and Graphviz support
- ⚡ **Multiple Execution Modes** - Sequential, parallel, and async execution
- 🛡️ **Robust Error Handling** - Comprehensive error strategies and retry logic
- 🔍 **Runtime Validation** - Automatic cycle detection and dependency validation

## 📦 Installation

### Basic Installation

```bash
pip install fast-dag
```

### With Visualization Support

```bash
pip install fast-dag[viz]
```

### With All Features

```bash
pip install fast-dag[all]
```

### Development Installation

```bash
git clone https://github.com/felixnext/fast-dag.git
cd fast-dag
pip install -e ".[dev]"
```

## 🚀 Quick Start

### Simple DAG Example

```python
from fast_dag import DAG

# Create a DAG
dag = DAG("data_pipeline")

# Define nodes using decorators
@dag.node
def fetch_data() -> dict:
    return {"data": [1, 2, 3, 4, 5]}

@dag.node
def process_data(data: dict) -> list:
    return [x * 2 for x in data["data"]]

@dag.node
def save_results(processed: list) -> str:
    return f"Saved {len(processed)} items"

# Connect nodes
dag.connect("fetch_data", "process_data", input="data")
dag.connect("process_data", "save_results", input="processed")

# Execute the DAG
result = dag.run()
print(result)  # "Saved 5 items"
```

### FSM Example

```python
from fast_dag import FSM
from fast_dag.core.types import FSMReturn

# Create a finite state machine
fsm = FSM("traffic_light")

@fsm.state(initial=True)
def red() -> FSMReturn:
    print("Red light - STOP")
    return FSMReturn(next_state="green")

@fsm.state
def green() -> FSMReturn:
    print("Green light - GO")
    return FSMReturn(next_state="yellow")

@fsm.state
def yellow() -> FSMReturn:
    print("Yellow light - SLOW DOWN")
    return FSMReturn(next_state="red")

# Run for 5 cycles
fsm.max_cycles = 5
fsm.run()
```

## 🎯 Core Concepts

### Nodes

Nodes are the basic units of work in fast-dag:

```python
@dag.node
def my_task(x: int) -> int:
    return x * 2

# Or create nodes manually
from fast_dag.core.node import Node

node = Node(
    func=my_function,
    name="my_node",
    description="Process data"
)
```

### Connections

Connect nodes to define data flow:

```python
# Simple connection
dag.connect("source", "target")

# Specify input/output names
dag.connect("source", "target", output="result", input="data")

# Use operator syntax
dag.nodes["a"] >> dag.nodes["b"] >> dag.nodes["c"]
```

### Conditional Flows

Build dynamic workflows with conditions:

```python
@dag.condition
def check_threshold(value: int) -> bool:
    return value > 100

@dag.node
def process_high(value: int) -> str:
    return f"High value: {value}"

@dag.node
def process_low(value: int) -> str:
    return f"Low value: {value}"

# Connect conditional branches
dag.nodes["check_threshold"].on_true >> dag.nodes["process_high"]
dag.nodes["check_threshold"].on_false >> dag.nodes["process_low"]
```

### Advanced Node Types

```python
# Multi-input convergence
@dag.any()  # Waits for ANY input to be ready
def merge_first(data: dict) -> str:
    return f"Received: {data}"

@dag.all()  # Waits for ALL inputs to be ready
def merge_all(data: dict) -> str:
    return f"All data: {data}"

# Multi-way branching
@dag.select
def router(request: dict) -> SelectReturn:
    category = request.get("category", "default")
    return SelectReturn(branch=category, value=request)
```

## 🔧 Advanced Features

### Execution Modes

```python
# Sequential execution (default)
result = dag.run()

# Parallel execution
result = dag.run(mode="parallel")

# Async execution
result = await dag.run_async()

# Custom runner configuration
from fast_dag.runner import DAGRunner

runner = DAGRunner(dag)
runner.configure(
    max_workers=4,
    timeout=300,
    error_strategy="continue"
)
result = runner.run()
```

### Error Handling

```python
# Retry with exponential backoff
@dag.node(retry=3, retry_delay=1.0)
def flaky_operation() -> dict:
    # Will retry up to 3 times with exponential backoff
    return fetch_external_data()

# Error strategies
result = dag.run(error_strategy="continue")  # Continue on error
result = dag.run(error_strategy="stop")      # Stop on first error (default)
```

### Node Lifecycle Hooks

```python
def before_node(node, inputs):
    print(f"Starting {node.name}")

def after_node(node, inputs, result):
    print(f"Finished {node.name}: {result}")
    return result  # Can modify result

def on_error(node, inputs, error):
    print(f"Error in {node.name}: {error}")

dag.set_node_hooks(
    "my_node",
    pre_execute=before_node,
    post_execute=after_node,
    on_error=on_error
)
```

### Visualization

```python
# Generate Mermaid diagram
mermaid_code = dag.visualize(backend="mermaid")

# Generate Graphviz DOT
dot_code = dag.visualize(backend="graphviz")

# Visualize with execution results
dag.run()
from fast_dag.visualization import VisualizationOptions

options = VisualizationOptions(
    show_results=True,
    direction="LR",
    success_color="#90EE90",
    error_color="#FFB6C1"
)
dag.visualize(options=options, filename="pipeline", format="png")
```

### Context and Metrics

```python
# Access context during execution
@dag.node
def process_with_context(data: dict, context: Context) -> dict:
    # Access previous results
    previous = context.get("previous_node")
    
    # Store metadata
    context.metadata["process_time"] = time.time()
    
    return {"combined": [data, previous]}

# After execution, access metrics
dag.run()
print(dag.context.metrics["execution_order"])
print(dag.context.metrics["node_times"])
print(dag.context.metadata)
```

## 📋 More Examples

### Data Processing Pipeline

```python
dag = DAG("etl_pipeline")

@dag.node
def extract_data() -> pd.DataFrame:
    return pd.read_csv("data.csv")

@dag.node
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna().reset_index(drop=True)

@dag.node
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    assert len(df) > 0, "No data after transformation"
    return df

@dag.node
def load_data(df: pd.DataFrame) -> str:
    df.to_parquet("output.parquet")
    return f"Loaded {len(df)} rows"

# Connect the pipeline
(
    dag.nodes["extract_data"] 
    >> dag.nodes["transform_data"] 
    >> dag.nodes["validate_data"] 
    >> dag.nodes["load_data"]
)

result = dag.run()
```

### State Machine with Conditions

```python
fsm = FSM("order_processor")

@fsm.state(initial=True)
def pending(order_id: str) -> FSMReturn:
    # Process payment
    if payment_successful():
        return FSMReturn(next_state="confirmed", value={"order_id": order_id})
    else:
        return FSMReturn(next_state="failed", value={"reason": "payment_failed"})

@fsm.state
def confirmed(data: dict) -> FSMReturn:
    # Ship order
    tracking = ship_order(data["order_id"])
    return FSMReturn(next_state="shipped", value={"tracking": tracking})

@fsm.state(terminal=True)
def shipped(data: dict) -> FSMReturn:
    notify_customer(data["tracking"])
    return FSMReturn(stop=True, value="Order delivered")

@fsm.state(terminal=True)
def failed(data: dict) -> FSMReturn:
    log_failure(data["reason"])
    return FSMReturn(stop=True, value="Order failed")

result = fsm.run(order_id="12345")
```

## 🛠️ Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=fast-dag

# Run specific test file
uv run pytest tests/unit/test_dag.py
```

### Code Quality

```bash
# Format code
uv run ruff format .

# Lint code
uv run ruff check . --fix

# Type checking
uv run mypy fast-dag
```

### Pre-commit Hooks

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run manually
uv run pre-commit run --all-files
```

### Building and Publishing

```bash
# Build the package
uv run python -m build

# Check the distribution
uv run twine check dist/*

# Upload to TestPyPI (for testing)
uv run twine upload --repository testpypi dist/*

# Upload to PyPI
uv run twine upload dist/*
```

## 📚 Documentation

For more detailed documentation, examples, and API reference, visit our [documentation site](https://github.com/felixnext/fast-dag/wiki).

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [UV](https://github.com/astral-sh/uv) for fast, reliable Python package management
- Inspired by [Airflow](https://airflow.apache.org/), [Prefect](https://www.prefect.io/), and [Dagster](https://dagster.io/)
- Special thanks to all contributors and users

## 📧 Contact

- **Author**: Felix Geilert
- **GitHub**: [@felixnext](https://github.com/felixnext)
- **PyPI**: [fast-dag](https://pypi.org/project/fast-dag/)
