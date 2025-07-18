# Getting Started with fast-dag

This guide will walk you through installing fast-dag and building your first workflows. By the end, you'll understand the basics and be ready to create your own DAGs and FSMs.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Your First DAG](#your-first-dag)
- [Your First FSM](#your-first-fsm)
- [Understanding the Basics](#understanding-the-basics)
- [Common Patterns](#common-patterns)
- [Next Steps](#next-steps)

## Prerequisites

Before you begin, ensure you have:
- Python 3.10 or higher installed
- Basic knowledge of Python functions and decorators
- (Optional) Graphviz installed if you want to generate workflow diagrams

## Installation

### Basic Installation

The simplest way to install fast-dag:

```bash
pip install fast-dag
```

This gives you the core functionality for building and executing workflows.

### Installation with Extras

fast-dag offers optional features through extras:

```bash
# For visualization support (Mermaid and Graphviz)
pip install fast-dag[viz]

# For serialization support (msgspec, YAML)
pip install fast-dag[serialize]

# For all features
pip install fast-dag[all]
```

### Development Installation

If you want to contribute or need the latest features:

```bash
git clone https://github.com/felixnext/fast-dag.git
cd fast-dag
pip install -e ".[dev]"
```

### Verify Installation

```python
import fast_dag
print(fast_dag.__version__)
# Output: 0.2.0
```

## Your First DAG

Let's build a simple data processing pipeline step by step.

### Step 1: Import and Create a DAG

```python
from fast_dag import DAG

# Create a DAG with a descriptive name
pipeline = DAG("customer_analysis")
```

### Step 2: Define Nodes

Nodes are the building blocks of your workflow. Use the `@dag.node` decorator to convert functions into nodes:

```python
@pipeline.node
def load_customers():
    """Load customer data from database"""
    # In real life, this would query a database
    return [
        {"id": 1, "name": "Alice", "purchases": 5, "total_spent": 250.00},
        {"id": 2, "name": "Bob", "purchases": 3, "total_spent": 150.00},
        {"id": 3, "name": "Charlie", "purchases": 8, "total_spent": 480.00},
    ]

@pipeline.node
def calculate_averages(customers):
    """Calculate average purchase value per customer"""
    return [
        {
            "id": c["id"],
            "name": c["name"],
            "avg_purchase": c["total_spent"] / c["purchases"]
        }
        for c in customers
    ]

@pipeline.node
def identify_vips(customers_with_avg):
    """Identify VIP customers (avg purchase > $50)"""
    vips = [c for c in customers_with_avg if c["avg_purchase"] > 50]
    return {
        "vip_count": len(vips),
        "vip_names": [c["name"] for c in vips],
        "message": f"Found {len(vips)} VIP customers!"
    }
```

### Step 3: Connect Nodes

Define how data flows between nodes:

```python
# Method 1: Using connect()
pipeline.connect("load_customers", "calculate_averages", input="customers")
pipeline.connect("calculate_averages", "identify_vips", input="customers_with_avg")

# Method 2: Using operator syntax (alternative)
# pipeline.nodes["load_customers"] >> pipeline.nodes["calculate_averages"] >> pipeline.nodes["identify_vips"]
```

### Step 4: Validate and Run

```python
# Validate the DAG structure
errors = pipeline.validate()
if errors:
    print("Validation errors:", errors)
else:
    print("DAG is valid!")

# Execute the pipeline
result = pipeline.run()
print(result)
# Output: {'vip_count': 2, 'vip_names': ['Alice', 'Charlie'], 'message': 'Found 2 VIP customers!'}

# Access individual node results
print(pipeline["load_customers"])  # Returns the customer list
print(pipeline["calculate_averages"])  # Returns customers with averages
```

### Step 5: Visualize Your DAG

If you installed the visualization extras:

```python
# Generate a Mermaid diagram
mermaid_code = pipeline.visualize(backend="mermaid")
print(mermaid_code)

# Save as an image (requires Graphviz)
pipeline.visualize(backend="graphviz", filename="customer_pipeline", format="png")
```

## Your First FSM

Now let's build a Finite State Machine for an order processing system.

### Step 1: Import and Create an FSM

```python
from fast_dag import FSM
from fast_dag.core.types import FSMReturn

# Create an FSM
order_fsm = FSM("order_processor", max_cycles=10)
```

### Step 2: Define States

States are defined using the `@fsm.state` decorator:

```python
@order_fsm.state(initial=True)
def pending(order_id: str = None):
    """Initial state - order received"""
    print(f"Processing order {order_id}")
    # Simulate payment processing
    payment_success = True  # In real life, call payment API
    
    if payment_success:
        return FSMReturn(next_state="paid", value={"order_id": order_id, "status": "payment_received"})
    else:
        return FSMReturn(next_state="failed", value={"order_id": order_id, "error": "payment_failed"})

@order_fsm.state
def paid(order_data: dict):
    """Order has been paid"""
    print(f"Order {order_data['order_id']} paid, preparing shipment")
    # Simulate inventory check
    in_stock = True
    
    if in_stock:
        return FSMReturn(next_state="shipped", value={**order_data, "tracking": "TRK123"})
    else:
        return FSMReturn(next_state="refunded", value={**order_data, "reason": "out_of_stock"})

@order_fsm.state
def shipped(order_data: dict):
    """Order has been shipped"""
    print(f"Order shipped with tracking {order_data['tracking']}")
    return FSMReturn(next_state="delivered", value=order_data)

@order_fsm.state(terminal=True)
def delivered(order_data: dict):
    """Order delivered - terminal state"""
    print(f"Order {order_data['order_id']} delivered successfully!")
    return FSMReturn(stop=True, value="Order completed")

@order_fsm.state(terminal=True)
def failed(order_data: dict):
    """Order failed - terminal state"""
    print(f"Order {order_data['order_id']} failed: {order_data.get('error', 'Unknown error')}")
    return FSMReturn(stop=True, value="Order failed")

@order_fsm.state(terminal=True)
def refunded(order_data: dict):
    """Order refunded - terminal state"""
    print(f"Order {order_data['order_id']} refunded: {order_data['reason']}")
    return FSMReturn(stop=True, value="Order refunded")
```

### Step 3: Run the FSM

```python
# Execute the FSM
result = order_fsm.run(order_id="ORD-12345")

# Access state history
print(order_fsm.state_history)
# Output: ['pending', 'paid', 'shipped', 'delivered']

# Get results from specific cycles
print(order_fsm["pending"])  # First state result
print(order_fsm["delivered"])  # Final state result
```

## Understanding the Basics

### Nodes vs States

- **Nodes** (in DAGs): Execute once, pass data forward, no cycles allowed
- **States** (in FSMs): Can be revisited, maintain state history, cycles allowed

### Data Flow

1. **In DAGs**: Data flows from node to node through connections
2. **In FSMs**: Data flows through FSMReturn objects between states

### Context

Both DAGs and FSMs maintain a context that stores:
- Results from each node/state
- Metadata about the execution
- Performance metrics

Access context during execution:

```python
@pipeline.node
def process_with_context(data: dict, context):
    # Access previous results
    previous_result = context.get("previous_node")
    # Add metadata
    context.metadata["processed_at"] = time.time()
    return processed_data
```

### Validation

Always validate before running:

```python
# For DAGs
errors = dag.validate()
if errors:
    for error in errors:
        print(f"Error: {error}")

# For FSMs
errors = fsm.validate()
if errors:
    for error in errors:
        print(f"Error: {error}")
```

## Common Patterns

### Pattern 1: Conditional Execution

```python
@pipeline.condition
def check_threshold(value: float) -> bool:
    return value > 100

@pipeline.node
def process_high(value: float):
    return f"High value: {value}"

@pipeline.node
def process_low(value: float):
    return f"Low value: {value}"

# Connect conditional branches
pipeline.nodes["check_threshold"].on_true >> pipeline.nodes["process_high"]
pipeline.nodes["check_threshold"].on_false >> pipeline.nodes["process_low"]
```

### Pattern 2: Parallel Processing

```python
# Nodes that don't depend on each other run in parallel
@pipeline.node
def fetch_from_api():
    return api_data

@pipeline.node
def fetch_from_database():
    return db_data

@pipeline.node
def fetch_from_file():
    return file_data

@pipeline.all()  # Wait for all inputs
def combine_data(api: dict, db: dict, file: dict):
    return {**api, **db, **file}

# Connect all sources to combine_data
pipeline.connect("fetch_from_api", "combine_data", input="api")
pipeline.connect("fetch_from_database", "combine_data", input="db")
pipeline.connect("fetch_from_file", "combine_data", input="file")

# Run in parallel mode
result = pipeline.run(mode="parallel")
```

### Pattern 3: Error Handling

```python
@pipeline.node(retry=3, retry_delay=1.0, timeout=30.0)
def risky_operation():
    # This will retry up to 3 times with exponential backoff
    return fetch_external_data()

# Run with error strategy
result = pipeline.run(error_strategy="continue")  # Continue on errors
```

### Pattern 4: Caching Results

```python
@pipeline.cached_node(ttl=3600)  # Cache for 1 hour
def expensive_computation(data):
    # This result will be cached
    return complex_calculation(data)
```

## Next Steps

Now that you understand the basics:

1. 📚 Learn about [Core Concepts](core-concepts.md) in detail
2. 🔨 Explore [Building Workflows](building-workflows.md) for advanced patterns
3. ⚡ Master [Execution Modes](execution.md) for performance
4. 🎯 Check out [Examples](examples/README.md) for real-world use cases

### Quick Tips

- **Start Simple**: Begin with sequential DAGs before moving to complex patterns
- **Use Type Hints**: They help with IDE support and catch errors early
- **Validate Early**: Always validate your workflows before running
- **Name Clearly**: Use descriptive names for nodes and workflows
- **Monitor Execution**: Use context and metrics to understand performance

### Getting Help

- Check the [Troubleshooting Guide](troubleshooting.md) for common issues
- Browse the [API Reference](api-reference.md) for detailed documentation
- Ask questions in [GitHub Discussions](https://github.com/felixnext/fast-dag/discussions)

Happy workflow building! 🚀