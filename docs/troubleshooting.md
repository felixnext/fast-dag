# Troubleshooting Guide

This guide helps you diagnose and fix common issues when using fast-dag.

## Table of Contents

- [Installation Issues](#installation-issues)
- [Common Errors](#common-errors)
- [Performance Issues](#performance-issues)
- [Workflow Issues](#workflow-issues)
- [Execution Problems](#execution-problems)
- [Serialization Issues](#serialization-issues)
- [Visualization Problems](#visualization-problems)
- [Debugging Techniques](#debugging-techniques)
- [FAQ](#faq)

## Installation Issues

### ImportError: No module named 'fast_dag'

**Problem:** Python can't find the fast-dag module.

**Solutions:**
1. Ensure fast-dag is installed:
   ```bash
   pip install fast-dag
   ```

2. Check if you're in the correct virtual environment:
   ```bash
   which python
   pip list | grep fast-dag
   ```

3. If using conda:
   ```bash
   conda activate your-env
   pip install fast-dag
   ```

### Installation fails with compilation errors

**Problem:** Binary dependencies fail to compile.

**Solutions:**
1. Update pip and setuptools:
   ```bash
   pip install --upgrade pip setuptools wheel
   ```

2. Install system dependencies:
   ```bash
   # Ubuntu/Debian
   sudo apt-get install python3-dev build-essential

   # macOS
   xcode-select --install

   # Windows
   # Install Visual C++ Build Tools
   ```

3. Use pre-built wheels:
   ```bash
   pip install fast-dag --only-binary :all:
   ```

### Version conflicts

**Problem:** Dependency version conflicts with other packages.

**Solutions:**
1. Create a fresh virtual environment:
   ```bash
   python -m venv fresh_env
   source fresh_env/bin/activate  # or fresh_env\Scripts\activate on Windows
   pip install fast-dag
   ```

2. Use pip-tools to resolve conflicts:
   ```bash
   pip install pip-tools
   echo "fast-dag" > requirements.in
   pip-compile requirements.in
   pip install -r requirements.txt
   ```

## Common Errors

### ValidationError: Circular dependency detected

**Problem:** Your DAG has a cycle.

**Example:**
```python
dag.connect("A", "B")
dag.connect("B", "C")
dag.connect("C", "A")  # Creates cycle!
```

**Solutions:**
1. Visualize your DAG to find the cycle:
   ```python
   print(dag.visualize(backend="mermaid"))
   ```

2. Check for cycles programmatically:
   ```python
   errors = dag.validate()
   for error in errors:
       if "circular" in error.lower():
           print(f"Cycle found: {error}")
   ```

3. Redesign your workflow to eliminate cycles:
   ```python
   # Instead of A -> B -> C -> A
   # Consider: A -> B -> C, and C produces output
   ```

### TypeError: Node function missing required parameters

**Problem:** Node inputs don't match function parameters.

**Example:**
```python
@dag.node
def process(data: dict, config: dict) -> dict:  # Expects 2 params
    return data

dag.connect("source", "process", input="data")  # Only provides 1
```

**Solutions:**
1. Provide all required inputs:
   ```python
   dag.connect("source", "process", input="data")
   dag.connect("config_loader", "process", input="config")
   ```

2. Use default parameters:
   ```python
   @dag.node
   def process(data: dict, config: dict = None) -> dict:
       config = config or {"default": "config"}
       return data
   ```

3. Use keyword arguments:
   ```python
   result = dag.run(inputs={
       "data": my_data,
       "config": my_config
   })
   ```

### AttributeError: 'NoneType' object has no attribute 'x'

**Problem:** Trying to access attributes on None values.

**Solutions:**
1. Add null checks in your nodes:
   ```python
   @dag.node
   def safe_process(data: dict | None) -> dict:
       if data is None:
           return {"error": "No data provided"}
       return {"processed": data}
   ```

2. Use optional chaining pattern:
   ```python
   @dag.node
   def get_nested_value(data: dict) -> str:
       # Safe navigation
       return data.get("user", {}).get("name", "Unknown")
   ```

3. Validate inputs early:
   ```python
   @dag.node
   def validate_and_process(data: dict) -> dict:
       if not data or "required_field" not in data:
           raise ValueError("Invalid input data")
       return process(data)
   ```

## Performance Issues

### Slow execution

**Problem:** Workflow takes too long to execute.

**Diagnosis:**
```python
# Enable profiling
dag.enable_profiling()
result = dag.run()
profile = dag.get_profile()

# Find bottlenecks
print(f"Slowest nodes:")
for node, time in sorted(profile.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"  {node}: {time:.2f}s")
```

**Solutions:**

1. **Use parallel execution:**
   ```python
   # Instead of sequential
   result = dag.run(mode="parallel", max_workers=4)
   ```

2. **Add caching:**
   ```python
   @dag.cached_node(ttl=3600)
   def expensive_computation(data: dict) -> dict:
       return heavy_processing(data)
   ```

3. **Optimize node functions:**
   ```python
   # Bad: Loading data repeatedly
   @dag.node
   def process_item(item_id: str) -> dict:
       all_data = load_entire_database()  # Loads everything!
       return all_data[item_id]

   # Good: Load once, process many
   @dag.node
   def load_data() -> dict:
       return load_entire_database()

   @dag.node
   def process_items(data: dict, item_ids: list) -> list:
       return [data[id] for id in item_ids]
   ```

4. **Use async for I/O operations:**
   ```python
   @dag.node
   async def fetch_data(urls: list) -> list:
       async with aiohttp.ClientSession() as session:
           tasks = [fetch_url(session, url) for url in urls]
           return await asyncio.gather(*tasks)
   ```

### High memory usage

**Problem:** Workflow consumes too much memory.

**Solutions:**

1. **Stream large data:**
   ```python
   @dag.node
   def process_large_file(filename: str) -> dict:
       counts = {}
       # Stream file instead of loading all
       with open(filename) as f:
           for line in f:
               # Process line by line
               update_counts(counts, line)
       return counts
   ```

2. **Clear intermediate results:**
   ```python
   @dag.node
   def process_and_clear(data: dict, context: Context) -> dict:
       result = expensive_transformation(data)
       
       # Clear previous large results
       if "large_intermediate" in context.results:
           del context.results["large_intermediate"]
       
       return result
   ```

3. **Use generators:**
   ```python
   @dag.node
   def generate_batches(data: list, batch_size: int = 100) -> Iterator[list]:
       for i in range(0, len(data), batch_size):
           yield data[i:i + batch_size]
   ```

## Workflow Issues

### Nodes not executing in expected order

**Problem:** Execution order seems wrong.

**Diagnosis:**
```python
# Check execution order
print("Execution order:", dag.get_execution_order())

# Visualize dependencies
print(dag.visualize(backend="mermaid"))
```

**Solutions:**

1. **Explicit connections:**
   ```python
   # Unclear order
   dag.add_node("A", func_a)
   dag.add_node("B", func_b)
   dag.add_node("C", func_c)

   # Clear order
   dag.nodes["A"] >> dag.nodes["B"] >> dag.nodes["C"]
   ```

2. **Check parallel execution:**
   ```python
   # Nodes at same level run in parallel
   # A -> B
   # A -> C
   # B and C run simultaneously in parallel mode
   ```

### Missing node results

**Problem:** Can't access node results after execution.

**Solutions:**

1. **Access results correctly:**
   ```python
   # Run workflow first
   result = dag.run()

   # Then access results
   node_result = dag["node_name"]
   # or
   node_result = dag.get_result("node_name")
   ```

2. **Check node actually executed:**
   ```python
   # Check if node was reached
   if "node_name" in dag.context.results:
       result = dag.context.results["node_name"]
   else:
       print("Node didn't execute - check dependencies")
   ```

3. **Handle conditional execution:**
   ```python
   @dag.condition
   def check_condition(value: int) -> bool:
       return value > 10

   # Only one branch executes
   # Check which branch ran
   if dag.nodes["check_condition"].last_result:
       true_result = dag.get_result("true_branch", None)
   else:
       false_result = dag.get_result("false_branch", None)
   ```

## Execution Problems

### Async execution errors

**Problem:** Async execution fails or hangs.

**Solutions:**

1. **Use proper async context:**
   ```python
   # Wrong
   result = dag.run_async()  # Returns coroutine!

   # Right
   import asyncio
   result = asyncio.run(dag.run_async())

   # Or in async function
   async def main():
       result = await dag.run_async()
   ```

2. **Mix sync and async correctly:**
   ```python
   # fast-dag handles mixed nodes automatically
   @dag.node
   def sync_node(data: dict) -> dict:
       return {"sync": data}

   @dag.node
   async def async_node(data: dict) -> dict:
       await asyncio.sleep(1)
       return {"async": data}

   # Both work together
   result = await dag.run_async()
   ```

### Timeout errors

**Problem:** Nodes timing out.

**Solutions:**

1. **Increase timeout:**
   ```python
   @dag.node(timeout=300.0)  # 5 minutes
   def long_running_task(data: dict) -> dict:
       return process_large_dataset(data)
   ```

2. **Break into smaller tasks:**
   ```python
   # Instead of one big task
   @dag.node
   def process_all(items: list) -> list:
       return [expensive_process(item) for item in items]

   # Break into chunks
   @dag.node
   def chunk_items(items: list) -> list[list]:
       return [items[i:i+10] for i in range(0, len(items), 10)]

   @dag.node
   def process_chunk(chunk: list) -> list:
       return [expensive_process(item) for item in chunk]
   ```

### Retry failures

**Problem:** Retries not working as expected.

**Solutions:**

1. **Configure retry properly:**
   ```python
   @dag.node(
       retry=3,
       retry_delay=5.0,    # Initial delay
       retry_backoff=2.0   # Exponential backoff
   )
   def flaky_operation() -> dict:
       if random.random() < 0.7:  # 70% failure rate
           raise Exception("Random failure")
       return {"success": True}
   ```

2. **Handle specific exceptions:**
   ```python
   @dag.node(retry=3)
   def api_call() -> dict:
       try:
           return external_api.get()
       except requests.Timeout:
           raise  # Will retry
       except requests.HTTPError as e:
           if e.response.status_code >= 500:
               raise  # Server error, retry
           else:
               # Client error, don't retry
               return {"error": str(e)}
   ```

## Serialization Issues

### Can't serialize custom objects

**Problem:** Serialization fails with custom classes.

**Solutions:**

1. **Register custom serializers:**
   ```python
   from fast_dag.serialization import register_serializer

   class CustomModel:
       def __init__(self, data):
           self.data = data

   def serialize_custom(obj: CustomModel) -> dict:
       return {"data": obj.data, "_type": "CustomModel"}

   def deserialize_custom(data: dict) -> CustomModel:
       return CustomModel(data["data"])

   register_serializer(CustomModel, serialize_custom, deserialize_custom)
   ```

2. **Use dataclasses:**
   ```python
   from dataclasses import dataclass, asdict

   @dataclass
   class Model:
       id: str
       value: float

   # Automatically serializable
   ```

3. **Convert to primitive types:**
   ```python
   @dag.node
   def process_model(model: CustomModel) -> dict:
       # Return serializable dict instead of custom object
       return {
           "id": model.id,
           "result": model.compute()
       }
   ```

### Large workflow serialization

**Problem:** Serialized workflows are too large.

**Solutions:**

1. **Exclude runtime data:**
   ```python
   # Save only structure, not results
   dag.to_json("workflow.json", include_results=False)
   ```

2. **Use binary format:**
   ```python
   # More compact than JSON
   dag.to_msgspec("workflow.msgspec")
   ```

3. **Compress large workflows:**
   ```python
   import gzip
   import json

   # Compress JSON
   workflow_json = dag.to_json()
   with gzip.open("workflow.json.gz", "wt") as f:
       f.write(workflow_json)
   ```

## Visualization Problems

### Visualization too complex

**Problem:** Large workflows are hard to visualize.

**Solutions:**

1. **Simplify visualization:**
   ```python
   # Show only main flow
   dag.visualize(
       backend="mermaid",
       max_depth=2,  # Limit nesting depth
       hide_util_nodes=True  # Hide utility nodes
   )
   ```

2. **Focus on subgraphs:**
   ```python
   # Visualize from specific node
   subgraph = dag.get_subgraph("interesting_node", depth=3)
   subgraph.visualize()
   ```

3. **Use hierarchical layout:**
   ```python
   dag.visualize(
       backend="graphviz",
       engine="dot",
       graph_attr={"rankdir": "TB", "ranksep": "2.0"}
   )
   ```

### Graphviz not working

**Problem:** Graphviz visualization fails.

**Solutions:**

1. **Install graphviz system package:**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install graphviz

   # macOS
   brew install graphviz

   # Windows
   # Download from https://graphviz.org/download/
   ```

2. **Use Mermaid instead:**
   ```python
   # No system dependencies
   mermaid_code = dag.visualize(backend="mermaid")
   # Paste into https://mermaid.live
   ```

## Debugging Techniques

### Enable debug mode

```python
# Global debug mode
import logging
logging.basicConfig(level=logging.DEBUG)

# DAG-specific debug
dag.set_debug(True)

# Run with debug output
result = dag.run()

# Get debug information
debug_info = dag.get_debug_info()
print(debug_info)
```

### Add logging to nodes

```python
import logging
logger = logging.getLogger(__name__)

@dag.node
def debug_node(data: dict) -> dict:
    logger.debug(f"Input data: {data}")
    logger.debug(f"Data type: {type(data)}")
    
    result = process(data)
    
    logger.debug(f"Output: {result}")
    return result
```

### Interactive debugging

```python
@dag.node
def debug_with_breakpoint(data: dict) -> dict:
    # Set breakpoint
    import pdb; pdb.set_trace()
    
    # Or use IDE breakpoint
    result = process(data)
    
    return result
```

### Execution tracing

```python
class ExecutionTracer:
    def __init__(self):
        self.trace = []
    
    def pre_execute(self, node, inputs):
        self.trace.append({
            "event": "pre_execute",
            "node": node.name,
            "inputs": list(inputs.keys()),
            "time": time.time()
        })
    
    def post_execute(self, node, inputs, result):
        self.trace.append({
            "event": "post_execute",
            "node": node.name,
            "result_type": type(result).__name__,
            "time": time.time()
        })

tracer = ExecutionTracer()
dag.set_global_hooks(
    pre_execute=tracer.pre_execute,
    post_execute=tracer.post_execute
)

result = dag.run()
print(json.dumps(tracer.trace, indent=2))
```

## FAQ

### Q: How do I pass data between non-connected nodes?

**A:** Use Context for shared state:
```python
@dag.node
def store_metadata(data: dict, context: Context) -> dict:
    context.metadata["shared_value"] = compute_value(data)
    return data

@dag.node
def use_metadata(other_data: dict, context: Context) -> dict:
    shared = context.metadata.get("shared_value", "default")
    return {"result": process(other_data, shared)}
```

### Q: Can I modify a DAG after creation?

**A:** Yes, DAGs are mutable until execution:
```python
# Add nodes dynamically
dag.add_node("new_node", new_function)

# Remove nodes
dag.remove_node("old_node")

# Modify connections
dag.disconnect("A", "B")
dag.connect("A", "C")
```

### Q: How do I handle optional dependencies?

**A:** Use conditional imports and fallbacks:
```python
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

@dag.node
def process_data(data: list) -> dict:
    if HAS_PANDAS:
        df = pd.DataFrame(data)
        return df.describe().to_dict()
    else:
        # Fallback implementation
        return {"count": len(data), "mean": sum(data) / len(data)}
```

### Q: How do I test workflows?

**A:** Use pytest and mocking:
```python
import pytest
from unittest.mock import Mock

def test_workflow():
    # Create test DAG
    test_dag = DAG("test")
    
    # Mock external dependencies
    mock_api = Mock(return_value={"test": "data"})
    
    @test_dag.node
    def fetch_data():
        return mock_api()
    
    @test_dag.node
    def process(data):
        return {"processed": data}
    
    test_dag.connect("fetch_data", "process")
    
    # Run and assert
    result = test_dag.run()
    assert result == {"processed": {"test": "data"}}
    mock_api.assert_called_once()
```

### Q: How do I handle large numbers of similar nodes?

**A:** Use node factories:
```python
def create_processor(node_id: int):
    @dag.node(name=f"processor_{node_id}")
    def processor(data: dict) -> dict:
        return {"id": node_id, "processed": data}
    return processor

# Create 10 processors
for i in range(10):
    create_processor(i)

# Connect them
for i in range(9):
    dag.connect(f"processor_{i}", f"processor_{i+1}")
```

---

Still having issues? Check our [GitHub Issues](https://github.com/felixnext/fast-dag/issues) or ask in [Discussions](https://github.com/felixnext/fast-dag/discussions).