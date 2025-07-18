# Building Workflows

This guide covers all the ways to construct workflows in fast-dag, from simple pipelines to complex nested systems.

## Table of Contents
- [Decorator Patterns](#decorator-patterns)
- [Manual Node Creation](#manual-node-creation)
- [Connection Methods](#connection-methods)
- [Conditional Workflows](#conditional-workflows)
- [Multi-Input Patterns](#multi-input-patterns)
- [Select/Switch Patterns](#selectswitch-patterns)
- [Nested Workflows](#nested-workflows)
- [Dynamic Workflows](#dynamic-workflows)
- [Best Practices](#best-practices)

## Decorator Patterns

Decorators are the most intuitive way to build workflows in fast-dag.

### Basic Node Decorator

```python
from fast_dag import DAG

dag = DAG("my_workflow")

@dag.node
def process_data(raw_data: list) -> dict:
    """Basic node with automatic name inference"""
    return {"count": len(raw_data), "data": raw_data}

# Node name is inferred as "process_data"
```

### Custom Node Names and Properties

```python
@dag.node(
    name="data_processor",          # Custom name
    description="Process raw data",  # Description
    retry=3,                        # Retry on failure
    retry_delay=2.0,               # Delay between retries
    timeout=30.0,                  # Timeout in seconds
    metadata={"version": "1.0"}    # Custom metadata
)
def process(data: list) -> dict:
    return {"processed": data}
```

### Conditional Decorators

```python
@dag.condition
def quality_check(data: dict) -> bool:
    """Returns boolean for branching"""
    return data["quality_score"] > 0.8

# Or with explicit return type
@dag.condition
def validate_size(data: list) -> ConditionalReturn:
    is_valid = len(data) > 10
    return ConditionalReturn(
        condition=is_valid,
        value={"size": len(data), "valid": is_valid}
    )
```

### Multi-Input Decorators

```python
# Wait for ANY input to be ready
@dag.any()
def process_first(data: dict) -> str:
    """Processes whichever input arrives first"""
    source = list(data.keys())[0]
    return f"Processed {source}: {data[source]}"

# Wait for ALL inputs to be ready
@dag.all()
def combine_all(data: dict) -> dict:
    """Waits for all connected inputs"""
    return {
        "combined": list(data.values()),
        "sources": list(data.keys())
    }
```

### Select/Router Decorators

```python
@dag.select
def route_by_type(message: dict) -> SelectReturn:
    """Route to different branches based on message type"""
    msg_type = message.get("type", "unknown")
    return SelectReturn(branch=msg_type, value=message["payload"])

# Connect branches
dag.nodes["route_by_type"].branches["email"] >> dag.nodes["send_email"]
dag.nodes["route_by_type"].branches["sms"] >> dag.nodes["send_sms"]
dag.nodes["route_by_type"].branches["unknown"] >> dag.nodes["log_unknown"]
```

### FSM State Decorators

```python
from fast_dag import FSM
from fast_dag.core.types import FSMReturn

fsm = FSM("order_processing")

@fsm.state(initial=True)
def new_order(order_id: str) -> FSMReturn:
    """Initial state for new orders"""
    order = fetch_order(order_id)
    return FSMReturn(next_state="validate", value=order)

@fsm.state
def validate(order: dict) -> FSMReturn:
    """Validate order details"""
    if is_valid(order):
        return FSMReturn(next_state="payment", value=order)
    else:
        return FSMReturn(next_state="rejected", value={**order, "reason": "invalid"})

@fsm.state(terminal=True)
def completed(order: dict) -> FSMReturn:
    """Terminal state - order completed"""
    return FSMReturn(stop=True, value=f"Order {order['id']} completed")
```

### Cached Node Decorator

```python
@dag.cached_node(
    ttl=3600,              # Cache for 1 hour
    cache_key=["user_id"], # Cache key parameters
    backend="redis"        # Cache backend
)
def expensive_calculation(user_id: str, data: dict) -> dict:
    """Results are cached based on user_id"""
    return perform_heavy_computation(user_id, data)
```

## Manual Node Creation

Sometimes you need more control over node creation.

### Creating Nodes Explicitly

```python
from fast_dag.core.node import Node
from fast_dag.core.types import NodeType

# Create a node manually
node = Node(
    func=my_function,
    name="my_node",
    inputs=["data", "config"],
    outputs=["result", "metrics"],
    node_type=NodeType.STANDARD
)

# Add to DAG
dag.add_node(node)
```

### Node Factory Pattern

```python
def create_processor_node(name: str, processor_type: str) -> Node:
    """Factory function to create similar nodes"""
    def process_func(data: dict) -> dict:
        if processor_type == "uppercase":
            return {k: v.upper() if isinstance(v, str) else v 
                    for k, v in data.items()}
        elif processor_type == "lowercase":
            return {k: v.lower() if isinstance(v, str) else v 
                    for k, v in data.items()}
        return data
    
    return Node(
        func=process_func,
        name=name,
        inputs=["data"],
        outputs=["result"],
        metadata={"processor_type": processor_type}
    )

# Create multiple similar nodes
dag.add_node(create_processor_node("upper_1", "uppercase"))
dag.add_node(create_processor_node("upper_2", "uppercase"))
dag.add_node(create_processor_node("lower_1", "lowercase"))
```

### Programmatic DAG Building

```python
# Build DAG from configuration
config = {
    "nodes": [
        {"name": "fetch", "func": fetch_data},
        {"name": "clean", "func": clean_data},
        {"name": "save", "func": save_data}
    ],
    "connections": [
        {"from": "fetch", "to": "clean"},
        {"from": "clean", "to": "save"}
    ]
}

dag = DAG("configured_pipeline")

# Add nodes
for node_config in config["nodes"]:
    dag.add_node(node_config["name"], node_config["func"])

# Add connections
for conn in config["connections"]:
    dag.connect(conn["from"], conn["to"])
```

## Connection Methods

fast-dag provides multiple ways to connect nodes.

### Basic Connection

```python
# Simple connection - auto-detects input parameter
dag.connect("source_node", "target_node")

# Explicit input mapping
dag.connect("source_node", "target_node", input="data")

# With output selection
dag.connect("source_node", "target_node", output="result", input="data")
```

### Operator Syntax

```python
# Sequential chain
dag.nodes["fetch"] >> dag.nodes["process"] >> dag.nodes["save"]

# Branching
source = dag.nodes["source"]
source >> dag.nodes["path_a"]
source >> dag.nodes["path_b"]

# Converging
dag.nodes["input_1"] >> dag.nodes["combine"]
dag.nodes["input_2"] >> dag.nodes["combine"]
```

### Conditional Connections

```python
# Using on_true/on_false properties
condition = dag.nodes["check_condition"]
condition.on_true >> dag.nodes["success_path"]
condition.on_false >> dag.nodes["failure_path"]

# Multiple conditions
quality_check = dag.nodes["quality_check"]
quality_check.on_true >> dag.nodes["high_quality_process"]
quality_check.on_false >> dag.nodes["low_quality_process"]

size_check = dag.nodes["size_check"]
size_check.on_true >> dag.nodes["large_batch_process"]
size_check.on_false >> dag.nodes["small_batch_process"]
```

### Multi-Output Connections

```python
@dag.node
def split_data(data: list) -> dict:
    """Split data into train/test sets"""
    split_idx = int(len(data) * 0.8)
    return {
        "train": data[:split_idx],
        "test": data[split_idx:],
        "metadata": {"total": len(data), "split": 0.8}
    }

# Connect different outputs to different nodes
dag.connect("split_data", "train_model", output="train", input="training_data")
dag.connect("split_data", "prepare_test", output="test", input="test_data")
dag.connect("split_data", "log_metadata", output="metadata", input="info")
```

### Dynamic Connections

```python
# Connect nodes based on conditions
def build_pipeline(include_validation: bool = True):
    dag = DAG("dynamic_pipeline")
    
    # Always include these
    dag.add_node("fetch", fetch_data)
    dag.add_node("process", process_data)
    dag.add_node("save", save_data)
    
    # Basic flow
    dag.connect("fetch", "process")
    
    # Conditionally add validation
    if include_validation:
        dag.add_node("validate", validate_data)
        dag.connect("process", "validate")
        dag.connect("validate", "save")
    else:
        dag.connect("process", "save")
    
    return dag
```

## Conditional Workflows

Build workflows that branch based on runtime conditions.

### Simple If-Then-Else

```python
@dag.condition
def check_threshold(value: float) -> bool:
    return value > 100

@dag.node
def high_value_process(value: float) -> str:
    return f"Premium processing for ${value}"

@dag.node
def standard_process(value: float) -> str:
    return f"Standard processing for ${value}"

# Connect branches
check = dag.nodes["check_threshold"]
check.on_true >> dag.nodes["high_value_process"]
check.on_false >> dag.nodes["standard_process"]
```

### Nested Conditions

```python
@dag.condition
def check_region(data: dict) -> bool:
    return data["region"] == "US"

@dag.condition
def check_premium(data: dict) -> bool:
    return data["tier"] == "premium"

@dag.node
def us_premium_process(data: dict) -> dict:
    return {"processed": data, "path": "us_premium"}

@dag.node
def us_standard_process(data: dict) -> dict:
    return {"processed": data, "path": "us_standard"}

@dag.node
def intl_process(data: dict) -> dict:
    return {"processed": data, "path": "international"}

# Build nested condition tree
region_check = dag.nodes["check_region"]
premium_check = dag.nodes["check_premium"]

# US path branches to premium check
region_check.on_true >> premium_check
premium_check.on_true >> dag.nodes["us_premium_process"]
premium_check.on_false >> dag.nodes["us_standard_process"]

# International path
region_check.on_false >> dag.nodes["intl_process"]
```

### Condition with Side Effects

```python
@dag.condition
def validate_and_log(data: dict) -> ConditionalReturn:
    """Condition that also produces output"""
    is_valid = validate_data(data)
    
    # Log validation result
    log_entry = {
        "timestamp": datetime.now(),
        "valid": is_valid,
        "data_size": len(data),
        "issues": get_validation_issues(data) if not is_valid else []
    }
    
    return ConditionalReturn(
        condition=is_valid,
        value={"data": data, "validation_log": log_entry}
    )
```

## Multi-Input Patterns

Handle workflows where nodes need multiple inputs.

### ANY Pattern - Race Condition

```python
@dag.any()
def process_first_ready(data: dict) -> dict:
    """Process whichever input is ready first"""
    # data contains: {"source_name": source_data}
    source = list(data.keys())[0]
    value = data[source]
    
    return {
        "winner": source,
        "result": process_data(value),
        "timestamp": time.time()
    }

# Multiple sources race to provide input
dag.nodes["fast_api"] >> dag.nodes["process_first_ready"]
dag.nodes["slow_api"] >> dag.nodes["process_first_ready"]
dag.nodes["cache_lookup"] >> dag.nodes["process_first_ready"]
```

### ALL Pattern - Synchronization

```python
@dag.all()
def combine_sources(data: dict) -> dict:
    """Wait for all inputs before processing"""
    # data contains all inputs: {"input_name": input_data}
    return {
        "user_data": data["user_api"],
        "transaction_data": data["transaction_api"],
        "analytics_data": data["analytics_api"],
        "combined_at": datetime.now()
    }

# Connect all required sources
dag.connect("fetch_user", "combine_sources", input="user_api")
dag.connect("fetch_transactions", "combine_sources", input="transaction_api")
dag.connect("fetch_analytics", "combine_sources", input="analytics_api")
```

### Partial Inputs Pattern

```python
@dag.node
def merge_optional(
    required_data: dict,
    optional_data: dict = None,
    config: dict = None
) -> dict:
    """Node with required and optional inputs"""
    result = {"base": required_data}
    
    if optional_data:
        result["optional"] = optional_data
    
    if config:
        result["configured"] = apply_config(required_data, config)
    
    return result

# Required connection
dag.connect("source", "merge_optional", input="required_data")

# Optional connections (may or may not exist)
if has_optional_source:
    dag.connect("optional_source", "merge_optional", input="optional_data")
```

## Select/Switch Patterns

Route execution to different branches based on values.

### Basic Switch

```python
@dag.select
def route_by_type(request: dict) -> SelectReturn:
    """Route based on request type"""
    request_type = request.get("type", "unknown")
    return SelectReturn(branch=request_type, value=request)

# Define handlers for each type
@dag.node
def handle_create(request: dict) -> dict:
    return {"action": "created", "id": create_resource(request)}

@dag.node
def handle_update(request: dict) -> dict:
    return {"action": "updated", "id": update_resource(request)}

@dag.node
def handle_delete(request: dict) -> dict:
    return {"action": "deleted", "id": delete_resource(request)}

@dag.node
def handle_unknown(request: dict) -> dict:
    return {"action": "error", "message": "Unknown request type"}

# Connect branches
router = dag.nodes["route_by_type"]
router.branches["create"] >> dag.nodes["handle_create"]
router.branches["update"] >> dag.nodes["handle_update"]
router.branches["delete"] >> dag.nodes["handle_delete"]
router.branches["unknown"] >> dag.nodes["handle_unknown"]
```

### Dynamic Branch Selection

```python
@dag.select
def dynamic_router(data: dict) -> SelectReturn:
    """Route based on computed value"""
    # Complex routing logic
    score = calculate_score(data)
    
    if score > 90:
        branch = "premium"
    elif score > 70:
        branch = "standard"
    elif score > 50:
        branch = "basic"
    else:
        branch = "reject"
    
    return SelectReturn(
        branch=branch,
        value={"data": data, "score": score}
    )
```

### Multi-Level Routing

```python
@dag.select
def route_by_region(data: dict) -> SelectReturn:
    return SelectReturn(branch=data["region"], value=data)

@dag.select
def route_us_by_state(data: dict) -> SelectReturn:
    return SelectReturn(branch=data["state"], value=data)

@dag.select
def route_eu_by_country(data: dict) -> SelectReturn:
    return SelectReturn(branch=data["country"], value=data)

# Build routing tree
region_router = dag.nodes["route_by_region"]

# US routes to state router
region_router.branches["US"] >> dag.nodes["route_us_by_state"]
us_router = dag.nodes["route_us_by_state"]
us_router.branches["CA"] >> dag.nodes["process_california"]
us_router.branches["NY"] >> dag.nodes["process_newyork"]

# EU routes to country router
region_router.branches["EU"] >> dag.nodes["route_eu_by_country"]
eu_router = dag.nodes["route_eu_by_country"]
eu_router.branches["DE"] >> dag.nodes["process_germany"]
eu_router.branches["FR"] >> dag.nodes["process_france"]
```

## Nested Workflows

Compose complex workflows from simpler ones.

### DAG as a Node

```python
# Create a sub-workflow
sub_dag = DAG("data_validation")

@sub_dag.node
def check_format(data: dict) -> dict:
    return {"valid": is_valid_format(data), "data": data}

@sub_dag.node
def check_completeness(data: dict) -> dict:
    return {"complete": is_complete(data), "data": data}

@sub_dag.node
def validation_report(format_check: dict, completeness: dict) -> dict:
    return {
        "valid": format_check["valid"] and completeness["complete"],
        "report": generate_report(format_check, completeness)
    }

# Connect sub-DAG nodes
sub_dag.connect("check_format", "validation_report", input="format_check")
sub_dag.connect("check_completeness", "validation_report", input="completeness")

# Add sub-DAG to main DAG
main_dag = DAG("main_pipeline")

@main_dag.node
def load_data() -> dict:
    return load_from_source()

@main_dag.node
def process_valid_data(validation_result: dict) -> dict:
    if validation_result["valid"]:
        return process_data(validation_result["data"])
    else:
        raise ValueError(f"Invalid data: {validation_result['report']}")

# Add sub-DAG as a node
main_dag.add_dag(
    name="validate",
    dag=sub_dag,
    inputs=["data"],            # Map to sub-DAG inputs
    outputs=["validation_result"] # Map from sub-DAG outputs
)

# Connect
main_dag.connect("load_data", "validate", input="data")
main_dag.connect("validate", "process_valid_data", input="validation_result")
```

### FSM as a Node

```python
# Create a retry FSM
retry_fsm = FSM("retry_logic", max_cycles=5)

@retry_fsm.state(initial=True)
def attempt(data: dict) -> FSMReturn:
    try:
        result = external_api_call(data)
        return FSMReturn(next_state="success", value=result)
    except Exception as e:
        return FSMReturn(
            next_state="retry_wait",
            value={"data": data, "error": str(e), "attempts": 1}
        )

@retry_fsm.state
def retry_wait(state: dict) -> FSMReturn:
    time.sleep(2 ** state["attempts"])  # Exponential backoff
    return FSMReturn(next_state="retry_attempt", value=state)

@retry_fsm.state
def retry_attempt(state: dict) -> FSMReturn:
    try:
        result = external_api_call(state["data"])
        return FSMReturn(next_state="success", value=result)
    except Exception as e:
        state["attempts"] += 1
        if state["attempts"] >= 3:
            return FSMReturn(next_state="failed", value=state)
        else:
            state["error"] = str(e)
            return FSMReturn(next_state="retry_wait", value=state)

@retry_fsm.state(terminal=True)
def success(result: dict) -> FSMReturn:
    return FSMReturn(stop=True, value=result)

@retry_fsm.state(terminal=True)
def failed(state: dict) -> FSMReturn:
    return FSMReturn(stop=True, value={"error": "Max retries exceeded"})

# Add FSM to DAG
main_dag.add_fsm(
    name="reliable_api_call",
    fsm=retry_fsm,
    inputs=["api_request"],
    outputs=["api_response"]
)
```

### Recursive Workflows

```python
def create_recursive_processor(max_depth: int = 3):
    """Create a DAG that can process nested structures"""
    dag = DAG(f"recursive_processor_depth_{max_depth}")
    
    @dag.node
    def process_item(item: dict, depth: int = 0) -> dict:
        # Base processing
        result = {"processed": transform(item), "depth": depth}
        
        # Check for nested items
        if "children" in item and depth < max_depth:
            # Create child processor
            child_dag = create_recursive_processor(max_depth - depth - 1)
            
            # Process children
            child_results = []
            for child in item["children"]:
                child_result = child_dag.run(item=child, depth=depth + 1)
                child_results.append(child_result)
            
            result["children"] = child_results
        
        return result
    
    return dag
```

## Dynamic Workflows

Build workflows that adapt at runtime.

### Configuration-Driven Workflows

```python
def build_pipeline_from_config(config: dict) -> DAG:
    """Build DAG from configuration"""
    dag = DAG(config["name"])
    
    # Register available operations
    operations = {
        "filter": lambda data, criteria: filter_data(data, criteria),
        "transform": lambda data, mapping: transform_data(data, mapping),
        "aggregate": lambda data, group_by: aggregate_data(data, group_by),
        "join": lambda data, other, on: join_data(data, other, on)
    }
    
    # Create nodes from config
    for step in config["steps"]:
        operation = operations[step["operation"]]
        
        @dag.node(name=step["name"])
        def dynamic_node(data, config=step["config"]):
            return operation(data, **config)
    
    # Create connections from config
    for connection in config["connections"]:
        dag.connect(
            connection["from"],
            connection["to"],
            input=connection.get("input", "data")
        )
    
    return dag

# Example config
pipeline_config = {
    "name": "data_pipeline",
    "steps": [
        {"name": "filter_active", "operation": "filter", "config": {"criteria": {"active": True}}},
        {"name": "transform_names", "operation": "transform", "config": {"mapping": {"name": "upper"}}},
        {"name": "group_by_category", "operation": "aggregate", "config": {"group_by": "category"}}
    ],
    "connections": [
        {"from": "filter_active", "to": "transform_names"},
        {"from": "transform_names", "to": "group_by_category"}
    ]
}

pipeline = build_pipeline_from_config(pipeline_config)
```

### Runtime Node Generation

```python
@dag.node
def analyze_data(data: list) -> dict:
    """Analyze data and determine required processing"""
    analysis = {
        "has_nulls": any(d is None for d in data),
        "has_duplicates": len(data) != len(set(data)),
        "needs_normalization": check_normalization(data)
    }
    
    # Dynamically create processing nodes based on analysis
    if analysis["has_nulls"]:
        dag.add_node("handle_nulls", handle_null_values)
        dag.connect("analyze_data", "handle_nulls")
    
    if analysis["has_duplicates"]:
        dag.add_node("remove_duplicates", deduplicate_data)
        if "handle_nulls" in dag.nodes:
            dag.connect("handle_nulls", "remove_duplicates")
        else:
            dag.connect("analyze_data", "remove_duplicates")
    
    return analysis
```

### Plugin-Based Workflows

```python
class ProcessorPlugin:
    """Base class for processor plugins"""
    def process(self, data: dict) -> dict:
        raise NotImplementedError

def load_plugins(plugin_dir: str) -> dict[str, ProcessorPlugin]:
    """Load processor plugins from directory"""
    plugins = {}
    # Load plugin implementations
    return plugins

def build_plugin_workflow(plugin_names: list[str]) -> DAG:
    """Build workflow from plugins"""
    dag = DAG("plugin_workflow")
    plugins = load_plugins("./plugins")
    
    previous = None
    for i, plugin_name in enumerate(plugin_names):
        plugin = plugins[plugin_name]
        
        @dag.node(name=f"{plugin_name}_{i}")
        def plugin_node(data: dict, p=plugin):
            return p.process(data)
        
        if previous:
            dag.connect(previous, f"{plugin_name}_{i}")
        previous = f"{plugin_name}_{i}"
    
    return dag
```

## Best Practices

### 1. Node Design

```python
# ✅ Good: Single responsibility
@dag.node
def validate_email(email: str) -> bool:
    return "@" in email and "." in email.split("@")[1]

# ❌ Bad: Multiple responsibilities
@dag.node
def process_user(user: dict) -> dict:
    # Validates, transforms, and saves - too much!
    if not validate_user(user):
        raise ValueError("Invalid user")
    user = transform_user(user)
    save_to_db(user)
    return user
```

### 2. Error Handling

```python
# ✅ Good: Explicit error handling
@dag.node(retry=3, retry_delay=1.0)
def fetch_data(url: str) -> dict:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        # Specific error handling
        raise ValueError(f"Failed to fetch from {url}: {e}")

# ❌ Bad: Silent failures
@dag.node
def fetch_data_bad(url: str) -> dict:
    try:
        return requests.get(url).json()
    except:
        return {}  # Silently returns empty dict
```

### 3. Type Hints

```python
# ✅ Good: Clear type hints
@dag.node
def calculate_average(numbers: list[float]) -> float:
    return sum(numbers) / len(numbers) if numbers else 0.0

# ❌ Bad: No type hints
@dag.node
def calculate_average_bad(numbers):
    return sum(numbers) / len(numbers)
```

### 4. Connection Clarity

```python
# ✅ Good: Explicit connections
dag.connect("load_users", "validate_users", input="user_list")
dag.connect("validate_users", "enrich_users", input="valid_users")

# ❌ Bad: Ambiguous connections
dag.connect("node1", "node2")  # What data is flowing?
```

### 5. Workflow Organization

```python
# ✅ Good: Logical grouping
# Data ingestion phase
@dag.node
def fetch_source_a(): ...

@dag.node
def fetch_source_b(): ...

# Validation phase
@dag.node
def validate_source_a(data_a): ...

@dag.node
def validate_source_b(data_b): ...

# Processing phase
@dag.all()
def combine_validated(data): ...
```

### 6. Reusable Components

```python
# ✅ Good: Reusable node factory
def create_validator(field_name: str, validator_func: Callable):
    @dag.node(name=f"validate_{field_name}")
    def validate(data: dict) -> dict:
        if not validator_func(data.get(field_name)):
            raise ValueError(f"Invalid {field_name}")
        return data
    return validate

# Create specific validators
dag.add_node(create_validator("email", is_valid_email))
dag.add_node(create_validator("phone", is_valid_phone))
```

## Summary

fast-dag provides flexible workflow building through:

- **Decorators** for intuitive workflow definition
- **Manual creation** for programmatic control
- **Multiple connection** methods for different use cases
- **Conditional branching** for dynamic flows
- **Multi-input patterns** for convergence
- **Nested workflows** for composition
- **Dynamic building** for runtime adaptation

Choose the patterns that best fit your use case, and don't be afraid to mix approaches for maximum flexibility!

Next, explore [Execution](execution.md) to learn how to run your workflows efficiently.