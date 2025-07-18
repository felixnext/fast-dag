# fast-dag Documentation

Welcome to the **fast-dag** documentation! This guide will help you understand and use fast-dag to build powerful, efficient workflows in Python.

## What is fast-dag?

fast-dag is a lightweight, high-performance Python library for building and executing Directed Acyclic Graph (DAG) and Finite State Machine (FSM) workflows. It's designed to be:

- **Simple**: Intuitive API with decorator-based workflow definition
- **Fast**: Minimal overhead with support for parallel and async execution
- **Flexible**: From simple pipelines to complex state machines
- **Type-Safe**: Full type hints for better IDE support and fewer bugs
- **Extensible**: Easy to add custom behaviors and integrations

## Quick Navigation

### 🚀 Getting Started
- [Installation & Setup](getting-started.md#installation)
- [Your First DAG](getting-started.md#your-first-dag)
- [Your First FSM](getting-started.md#your-first-fsm)
- [Understanding the Basics](getting-started.md#understanding-the-basics)

### 📚 Core Documentation
- [Core Concepts](core-concepts.md) - Understand Nodes, Context, DAG, and FSM
- [Building Workflows](building-workflows.md) - Learn all the ways to construct workflows
- [Execution Guide](execution.md) - Master execution modes and runtime behavior
- [Advanced Features](advanced-features.md) - Caching, serialization, visualization, and more

### 📖 Reference
- [API Reference](api-reference.md) - Complete API documentation
- [Examples Gallery](examples/README.md) - Learn by example
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [Migration Guide](migration-guide.md) - Moving from other workflow tools

## Quick Install

```bash
# Basic installation
pip install fast-dag

# With visualization support
pip install fast-dag[viz]

# With all features
pip install fast-dag[all]
```

## Quick Example

Here's a taste of what fast-dag can do:

```python
from fast_dag import DAG

# Create a workflow
dag = DAG("data_pipeline")

@dag.node
def fetch_data() -> dict:
    return {"sales": [100, 200, 300]}

@dag.node
def process_data(data: dict) -> float:
    return sum(data["sales"]) / len(data["sales"])

@dag.node
def save_result(average: float) -> str:
    return f"Average sales: ${average:.2f}"

# Connect the nodes
dag.connect("fetch_data", "process_data", input="data")
dag.connect("process_data", "save_result", input="average")

# Run it!
result = dag.run()
print(result)  # "Average sales: $200.00"
```

## Why fast-dag?

### For Data Engineers
- Build complex ETL pipelines with ease
- Handle failures gracefully with retry logic
- Monitor execution with built-in metrics
- Scale with parallel execution

### For ML Engineers
- Create reproducible ML pipelines
- Cache expensive computations
- Visualize model training workflows
- Integrate with any ML framework

### For Backend Developers
- Orchestrate microservices
- Build state machines for business logic
- Handle async operations naturally
- Type-safe workflow definitions

## Key Features

✅ **Multiple Workflow Types**
- Directed Acyclic Graphs (DAG) for pipelines
- Finite State Machines (FSM) for stateful workflows
- Nested workflows for complex orchestration

✅ **Flexible Execution**
- Sequential execution for simple flows
- Parallel execution for independent tasks
- Async execution for I/O-bound operations

✅ **Developer Experience**
- Intuitive decorator-based API
- Full type hints and IDE support
- Excellent error messages
- Comprehensive documentation

✅ **Production Ready**
- Robust error handling
- Caching support
- Serialization for persistence
- Visualization for debugging

## Getting Help

- 📖 Browse the [full documentation](core-concepts.md)
- 💡 Check out [examples](examples/README.md)
- 🐛 Report issues on [GitHub](https://github.com/felixnext/fast-dag/issues)
- 💬 Ask questions in [Discussions](https://github.com/felixnext/fast-dag/discussions)

## Next Steps

Ready to dive in? Start with our [Getting Started Guide](getting-started.md) to build your first workflow!

---

*fast-dag is open source and available under the MIT license. Contributions are welcome!*