# Fast-DAG: Technical Specification

## Executive Summary

Fast-DAG is a high-performance Python library for building and executing Directed Acyclic Graph (DAG) workflows and Finite State Machine (FSM) workflows. Built with modern Python (3.10+), it prioritizes runtime performance through dataclasses while providing rich functionality including async/parallel execution, visualization, and optional high-performance serialization.

## Key Features

- **Performance First**: Built on dataclasses for minimal overhead
- **Modern Python**: Full type hints with compact syntax (`str | None`, `list[str]`)
- **Multiple Execution Modes**: Synchronous, asynchronous, and parallel
- **Flexible API**: Decorators, operators, and manual construction
- **Optional Serialization**: High-performance msgspec with pydantic compatibility
- **Rich Visualization**: Mermaid and Graphviz support
- **Extensible**: Plugin architecture for custom functionality

## Specification Structure

This specification is organized into focused documents:

### Core Specifications
- [**Core Concepts**](core-concepts.md) - Node, Context, DAG, and FSM fundamentals
- [**API Design**](api-design.md) - Decorators, connections, and execution patterns
- [**Implementation**](implementation.md) - Technical architecture and algorithms

### Feature Specifications
- [**Serialization**](serialization.md) - msgspec integration and data persistence
- [**Visualization**](visualization.md) - Mermaid and Graphviz diagram generation
- [**Examples**](examples.md) - Practical usage patterns and recipes

## Design Philosophy

### 1. Performance Without Compromise
- Dataclasses for zero-overhead execution
- Validation only at boundaries (construction/serialization)
- Efficient dependency resolution algorithms
- Memory-conscious result storage

### 2. Developer Experience
- Intuitive APIs inspired by FastAPI and Keras
- Strong typing for IDE support
- Clear error messages with actionable solutions
- Progressive disclosure of complexity

### 3. Production Ready
- Comprehensive error handling
- Performance metrics and observability
- Robust testing and validation
- Clear migration paths

## Quick Example

```python
from fast_dag import DAG

# Create workflow
workflow = DAG("data_pipeline")

# Define nodes with decorators
@workflow.node
def load_data(file_path: str) -> int:
    """Load data from file"""
    return 1

@workflow.node(name="process_data")
def process_data(data: int) -> dict:
    """Process the data"""
    return {"processed": data}

@workflow.node(description="Save the data")
def save_data(data: dict) -> str:
    """Save the data"""
    return str(data["processed"])

# Connect nodes
load_data >> process_data >> save_data

# Execute
result = workflow.run(file_path="data.csv")
```

Note that on creation of the workflow this will run validations in terms of inputs and outputs of the nodes.
This include type checking and checking if the inputs and outputs are compatible.

## Installation

```bash
# Core functionality
pip install fast-dag

# With serialization support
pip install fast-dag[serialize]

# With visualization
pip install fast-dag[viz]

# Everything
pip install fast-dag[all]
```

## Architecture Overview

```
fast_dag/
├── core/
│   ├── node.py         # Node abstraction
│   ├── context.py      # Execution context
│   ├── dag.py          # DAG implementation
│   └── fsm.py          # FSM implementation
├── execution/
│   ├── runner.py       # Execution strategies
│   └── scheduler.py    # Dependency resolution
├── serialization/      # Optional msgspec/pydantic
├── visualization/      # Optional graphviz/mermaid
└── utils/             # Helpers and utilities
```

## Core Concepts Summary

### Node
- Wraps Python functions with metadata
- Automatic signature introspection
- Support for conditional logic
- Context injection when needed

### Context
- Lightweight result container
- Execution metrics tracking
- History for FSM cycles
- Extensible metadata storage

### DAG
- Acyclic workflow definition
- Topological execution order
- Parallel execution support
- Comprehensive validation

### FSM
- Stateful workflow execution
- Cycle tracking and limits
- Transition conditions
- State history access

## Next Steps

1. Review [Core Concepts](core-concepts.md) for detailed component descriptions
2. Explore [API Design](api-design.md) for usage patterns
3. Check [Examples](examples.md) for practical applications
4. See [Implementation](implementation.md) for technical details