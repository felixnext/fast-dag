# Fast-DAG Test Suite

This directory contains the comprehensive test suite for the fast-dag library, following Test-Driven Development (TDD) principles.

## Test Structure

```
tests/
├── conftest.py          # Shared fixtures and test functions
├── unit/                # Unit tests for individual components
│   ├── test_context.py  # Tests for Context and FSMContext
│   ├── test_node.py     # Tests for Node class
│   ├── test_dag.py      # Tests for DAG creation and validation
│   ├── test_dag_execution.py  # Tests for DAG execution
│   └── test_fsm.py      # Tests for FSM functionality
└── integration/         # Integration tests (to be added)
```

## Test Coverage

### 1. Context Tests (`test_context.py`)
- **Context**: Basic data storage, dict-like access, metadata, metrics
- **FSMContext**: State history, cycle tracking, cycle results, latest/specific cycle access

### 2. Node Tests (`test_node.py`)
- **Creation**: Basic creation, custom names, explicit inputs/outputs
- **Introspection**: Automatic input/output detection, context parameter exclusion
- **Validation**: Signature validation, async detection
- **Connections**: Input/output connection tracking
- **Special Types**: Conditional nodes, FSM state nodes
- **Execution**: Sync/async execution, conditional returns

### 3. DAG Tests (`test_dag.py`)
- **Creation**: Basic DAG, with description/metadata
- **Node Addition**: Decorator pattern, manual addition, builder pattern
- **Validation**: Cycle detection, disconnected nodes, missing branches
- **Connections**: Simple connections, explicit ports, type checking
- **Properties**: Entry points, execution order, cached properties

### 4. DAG Execution Tests (`test_dag_execution.py`)
- **Basic Execution**: Linear workflows, multiple inputs, custom context
- **Result Access**: Dict-style access, get method, contains operator
- **Conditional**: True/false branch execution
- **Parallel**: Parallel branch execution
- **Async**: Async nodes, mixed sync/async
- **Error Handling**: Node errors, continue strategy, timeouts
- **Metrics**: Execution timing, parallel groups

### 5. FSM Tests (`test_fsm.py`)
- **Creation**: Basic FSM, max cycles setting
- **States**: State definition, initial/terminal marking
- **Transitions**: Simple transitions, conditional transitions
- **Step Execution**: Single step, multiple steps, cycles
- **Full Execution**: Run to completion, max cycles limit, explicit stop
- **Result Access**: Latest state, specific cycle, state history
- **Validation**: Missing initial state, unreachable states

## Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=fast_dag --cov-report=term-missing

# Run specific test file
uv run pytest tests/unit/test_context.py

# Run specific test
uv run pytest tests/unit/test_dag.py::TestDAGCreation::test_dag_creation

# Run with verbose output
uv run pytest -v

# Run only unit tests
uv run pytest tests/unit/

# Run tests matching pattern
uv run pytest -k "execution"
```

## Test Markers

Tests use pytest markers defined in `pyproject.toml`:

```bash
# Run only unit tests
uv run pytest -m unit

# Run only integration tests  
uv run pytest -m integration

# Skip slow tests
uv run pytest -m "not slow"
```

## Writing New Tests

1. Follow the existing pattern in test files
2. Use descriptive test names that explain what is being tested
3. Include docstrings explaining the test purpose
4. Use fixtures from `conftest.py` for common test data
5. Group related tests in classes
6. Test both success and failure cases
7. Use `pytest.raises` for expected exceptions

## TDD Workflow

1. Write failing tests for new functionality
2. Implement minimal code to make tests pass
3. Refactor while keeping tests green
4. Add edge case tests
5. Ensure all tests pass before committing

## Next Steps

With these comprehensive tests in place, the next step is to implement the actual fast-dag library components to make all tests pass. The tests serve as both specification and validation for the implementation.