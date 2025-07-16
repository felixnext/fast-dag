# Fast-DAG Examples

This directory contains example scripts demonstrating various features of the fast-dag library.

## Examples

### 1. Simple Sequential DAG (`01_simple_sequential.py`)
A basic data processing pipeline demonstrating:
- Decorator-based node creation
- Sequential connections using `>>` operator
- Workflow validation
- Visualization (Mermaid and Graphviz)
- Different execution modes (sync/async/parallel)

```bash
# Run with default sync mode
python 01_simple_sequential.py

# Run with async mode
python 01_simple_sequential.py --mode async

# Run with parallel mode and visualization
python 01_simple_sequential.py --mode parallel --visualize
```

### 2. Complex Branching DAG (`02_complex_branching.py`)
An order processing workflow demonstrating:
- Conditional nodes (if/else branching)
- Parallel branches (one node with multiple children)
- AND nodes (multiple inputs converging)
- Complex workflow validation

```bash
# Run with sync mode
python 02_complex_branching.py

# Run with parallel mode (executes parallel branches concurrently)
python 02_complex_branching.py --mode parallel
```

### 3. FSM State Machine (`03_fsm_state_machine.py`)
A traffic light state machine demonstrating:
- Multiple states (red, green, yellow, emergency)
- State transitions with conditions
- Terminal states and stop conditions
- Step-by-step execution
- Cycle tracking and history

```bash
# Run with default settings
python 03_fsm_state_machine.py

# Run with custom max cycles
python 03_fsm_state_machine.py --max-cycles 20

# Run in async mode
python 03_fsm_state_machine.py --mode async
```

### 4. Invalid DAG Examples (`04_invalid_dag.py`)
Examples of various validation errors:
- Example 1: Cyclic dependencies
- Example 2: Disconnected nodes
- Example 3: Type mismatches
- Example 4: Missing conditional branches

```bash
# Run cyclic dependency example
python 04_invalid_dag.py --example 1

# Run disconnected nodes example
python 04_invalid_dag.py --example 2

# Run type mismatch example
python 04_invalid_dag.py --example 3

# Run missing connections example
python 04_invalid_dag.py --example 4
```

## Common Patterns

### Execution Modes
All examples support different execution modes via the `--mode` parameter:
- `sync` (default): Sequential execution
- `async`: Asynchronous execution using asyncio
- `parallel`: Parallel execution of independent nodes

### Visualization
Examples that support `--visualize` will generate:
- `.mmd` files: Mermaid diagrams (viewable in many markdown editors)
- `.png` files: Graphviz-rendered images

### Validation
All examples demonstrate workflow validation before execution, showing how fast-dag catches configuration errors early.

## Requirements

These examples assume fast-dag is installed with optional dependencies:

```bash
# Core + visualization
pip install fast-dag[viz]

# Or all features
pip install fast-dag[all]
```