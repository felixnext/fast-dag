# Execution and Runtime

This guide covers everything about executing workflows in fast-dag, from basic runs to advanced performance optimization.

## Table of Contents
- [Execution Modes](#execution-modes)
- [Running DAGs](#running-dags)
- [Running FSMs](#running-fsms)
- [Context and Data Flow](#context-and-data-flow)
- [Error Handling](#error-handling)
- [Performance Optimization](#performance-optimization)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Node Lifecycle](#node-lifecycle)
- [Best Practices](#best-practices)

## Execution Modes

fast-dag supports three execution modes to match your performance needs:

### Sequential Mode (Default)

Nodes execute one at a time in topological order.

```python
# Default mode
result = dag.run()

# Explicit sequential mode
result = dag.run(mode="sequential")
```

**When to use**:
- Simple workflows
- Debugging and development
- When order matters strictly
- Low resource environments

### Parallel Mode

Independent nodes execute concurrently using threads.

```python
# Run with parallel execution
result = dag.run(mode="parallel", max_workers=4)

# Auto-detect optimal workers
result = dag.run(mode="parallel")  # Uses CPU count
```

**When to use**:
- CPU-bound operations
- Independent data processing
- When nodes have no shared state
- Multiple data sources

**Example**:
```python
@dag.node
def process_region_a(data):
    # CPU-intensive processing
    return heavy_computation(data["region_a"])

@dag.node
def process_region_b(data):
    # CPU-intensive processing
    return heavy_computation(data["region_b"])

@dag.node
def process_region_c(data):
    # CPU-intensive processing
    return heavy_computation(data["region_c"])

@dag.all()
def combine_regions(a: dict, b: dict, c: dict):
    return merge_results(a, b, c)

# All three regions process in parallel
result = dag.run(mode="parallel", max_workers=3)
```

### Async Mode

For I/O-bound operations using asyncio.

```python
# Define async nodes
@dag.node
async def fetch_api_data(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

@dag.node
async def fetch_db_data(query: str) -> list:
    async with database.connect() as conn:
        return await conn.fetch(query)

# Run asynchronously
result = await dag.run_async()
```

**When to use**:
- Network requests
- Database queries
- File I/O operations
- Webhook handlers

### Mixed Mode Execution

Combine sync and async nodes:

```python
@dag.node
def prepare_data(raw: dict) -> dict:
    # Synchronous CPU work
    return transform_data(raw)

@dag.node
async def upload_data(data: dict) -> str:
    # Asynchronous I/O
    async with client.post("/api/upload", json=data) as resp:
        return (await resp.json())["id"]

# fast-dag handles the mix automatically
result = await dag.run_async()
```

## Running DAGs

### Basic Execution

```python
# Simple run
result = dag.run()

# With initial inputs
result = dag.run(inputs={"data": my_data, "config": my_config})

# With context
context = Context(metadata={"user_id": "123"})
result = dag.run(context=context)

# All together
result = dag.run(
    inputs={"data": my_data},
    context=context,
    mode="parallel",
    max_workers=8
)
```

### Partial Execution

Run only specific parts of a DAG:

```python
# Run from a specific node
result = dag.run_from("process_data")

# Run up to a specific node
result = dag.run_until("save_results")

# Run between nodes
result = dag.run_between("load_data", "validate_output")
```

### Execution Control

```python
# Set timeout for entire DAG
result = dag.run(timeout=300.0)  # 5 minutes

# Cancel execution
import asyncio

async def controlled_run():
    task = asyncio.create_task(dag.run_async())
    
    # Cancel after 10 seconds
    await asyncio.sleep(10)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        print("DAG execution cancelled")
```

### Execution Results

```python
# Get final result
result = dag.run()
print(result)  # Result from last node

# Access specific node results
dag["load_data"]      # Result from load_data node
dag.get_result("process_data")  # Alternative syntax

# Get all results
all_results = dag.context.results
for node_name, result in all_results.items():
    print(f"{node_name}: {result}")

# Check execution status
if dag.context.metadata.get("execution_status") == "completed":
    print("Success!")
```

## Running FSMs

### Basic FSM Execution

```python
# Simple run
result = fsm.run()

# With initial state data
result = fsm.run(order_id="ORD-123", amount=99.99)

# With context
context = FSMContext()
result = fsm.run(context=context)
```

### FSM-Specific Features

```python
# Set maximum cycles
result = fsm.run(max_cycles=50)

# Access state history
print(fsm.state_history)
# ['pending', 'processing', 'processed', 'completed']

# Get results by cycle
print(fsm["processing"])     # Latest processing result
print(fsm["processing.0"])   # First processing result
print(fsm["processing.2"])   # Third processing result

# Check termination
if fsm.is_terminated:
    print(f"FSM ended in state: {fsm.current_state}")
```

### State Transition Control

```python
# Run with state hooks
def on_state_change(fsm, from_state, to_state, result):
    print(f"Transition: {from_state} -> {to_state}")
    # Can modify or validate transitions
    
fsm.add_state_hook(on_state_change)
result = fsm.run()
```

## Context and Data Flow

### Understanding Context

Context carries shared state through execution:

```python
from fast_dag import Context

# Create context with initial data
context = Context(
    metadata={
        "request_id": "req-123",
        "user_id": "user-456",
        "start_time": time.time()
    }
)

# Nodes can access and modify context
@dag.node
def process_with_context(data: dict, context: Context) -> dict:
    # Read from context
    user_id = context.metadata["user_id"]
    
    # Write to context
    context.metadata["processing_time"] = time.time()
    
    # Access other node results
    previous = context.get("previous_node", default={})
    
    return {"processed": data, "user": user_id}
```

### Data Flow Patterns

#### Forward Data Flow
```python
@dag.node
def step1() -> dict:
    return {"value": 100}

@dag.node
def step2(data: dict) -> dict:
    return {"doubled": data["value"] * 2}

@dag.node
def step3(data: dict) -> str:
    return f"Final: {data['doubled']}"

# Connect in sequence
dag.nodes["step1"] >> dag.nodes["step2"] >> dag.nodes["step3"]
```

#### Branching Data Flow
```python
@dag.node
def source() -> dict:
    return {"data": [1, 2, 3, 4, 5]}

@dag.node
def process_evens(data: dict) -> list:
    return [x for x in data["data"] if x % 2 == 0]

@dag.node
def process_odds(data: dict) -> list:
    return [x for x in data["data"] if x % 2 != 0]

# Data branches to both processors
source_node = dag.nodes["source"]
source_node >> dag.nodes["process_evens"]
source_node >> dag.nodes["process_odds"]
```

#### Merging Data Flow
```python
@dag.all()
def merge_results(evens: list, odds: list) -> dict:
    return {
        "evens": evens,
        "odds": odds,
        "total": len(evens) + len(odds)
    }

# Connect both branches to merger
dag.connect("process_evens", "merge_results", input="evens")
dag.connect("process_odds", "merge_results", input="odds")
```

### Context Best Practices

```python
# ✅ Good: Use context for cross-cutting concerns
@dag.node
def audit_log(data: dict, context: Context) -> dict:
    context.metadata["audit_trail"] = context.metadata.get("audit_trail", [])
    context.metadata["audit_trail"].append({
        "node": "audit_log",
        "timestamp": time.time(),
        "data_size": len(str(data))
    })
    return data

# ❌ Bad: Use context for primary data flow
@dag.node
def bad_pattern(context: Context) -> None:
    # Don't do this - use proper connections
    data = context.results["previous_node"]
    processed = process(data)
    context.results["current_node"] = processed
```

## Error Handling

### Node-Level Error Handling

```python
# Retry configuration
@dag.node(
    retry=3,                    # Retry up to 3 times
    retry_delay=1.0,           # Initial delay between retries
    retry_backoff=2.0,         # Exponential backoff multiplier
    timeout=30.0               # Timeout per attempt
)
def risky_operation(data: dict) -> dict:
    response = external_api_call(data)
    if not response.ok:
        raise ValueError(f"API error: {response.status}")
    return response.json()

# Custom error handler
def handle_error(node, inputs, error):
    print(f"Error in {node.name}: {error}")
    # Can return a default value
    return {"error": str(error), "default": True}

dag.set_node_error_handler("risky_operation", handle_error)
```

### DAG-Level Error Strategies

```python
# Continue on error (default is "fail_fast")
result = dag.run(error_strategy="continue")

# Fail fast - stop on first error
result = dag.run(error_strategy="fail_fast")

# Custom error handling
def dag_error_handler(node_name: str, error: Exception, context: Context):
    # Log error
    logger.error(f"DAG error in {node_name}: {error}")
    
    # Store error in context
    context.metadata.setdefault("errors", []).append({
        "node": node_name,
        "error": str(error),
        "timestamp": time.time()
    })
    
    # Decide whether to continue
    if isinstance(error, CriticalError):
        return "stop"  # Stop execution
    return "continue"  # Continue with other nodes

result = dag.run(error_handler=dag_error_handler)
```

### Error Recovery Patterns

```python
# Checkpoint pattern
@dag.node
def checkpoint_save(data: dict, context: Context) -> dict:
    # Save intermediate state
    checkpoint_file = f"checkpoint_{context.metadata['run_id']}.json"
    with open(checkpoint_file, "w") as f:
        json.dump({"data": data, "context": context.to_dict()}, f)
    return data

# Circuit breaker pattern
class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failures = 0
        self.threshold = failure_threshold
        self.timeout = timeout
        self.last_failure = None
        
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            if self.is_open():
                raise CircuitBreakerOpenError("Circuit breaker is open")
            
            try:
                result = func(*args, **kwargs)
                self.reset()
                return result
            except Exception as e:
                self.record_failure()
                raise
                
        return wrapper
    
    def is_open(self):
        if self.failures >= self.threshold:
            if time.time() - self.last_failure < self.timeout:
                return True
        return False
        
    def record_failure(self):
        self.failures += 1
        self.last_failure = time.time()
        
    def reset(self):
        self.failures = 0
        self.last_failure = None

# Use circuit breaker
@dag.node
@CircuitBreaker(failure_threshold=3, timeout=300)
def protected_api_call(data: dict) -> dict:
    return external_api.process(data)
```

## Performance Optimization

### Caching

```python
# Simple caching
@dag.cached_node(ttl=3600)  # Cache for 1 hour
def expensive_computation(data: dict) -> dict:
    return complex_algorithm(data)

# Cache with custom key
@dag.cached_node(
    ttl=1800,
    cache_key=["user_id", "data_version"]  # Cache key from inputs
)
def user_specific_calc(user_id: str, data_version: int, data: dict) -> dict:
    return process_user_data(user_id, data_version, data)

# LRU cache for memory efficiency
from functools import lru_cache

@dag.node
@lru_cache(maxsize=100)
def cached_transform(data_tuple: tuple) -> dict:
    # Convert tuple back to dict for processing
    data = dict(data_tuple)
    return transform(data)
```

### Lazy Evaluation

```python
# Lazy loading pattern
@dag.node
def lazy_data_loader(config: dict) -> Callable:
    """Returns a function that loads data when called"""
    def load():
        # Expensive loading only happens when accessed
        return load_large_dataset(config["path"])
    return load

@dag.node
def process_if_needed(data_loader: Callable, should_process: bool) -> dict:
    if should_process:
        data = data_loader()  # Load only if needed
        return process(data)
    return {"skipped": True}
```

### Memory Management

```python
# Streaming pattern for large data
@dag.node
def stream_processor(file_path: str) -> Iterator[dict]:
    """Process large file in chunks"""
    with open(file_path) as f:
        for chunk in read_chunks(f, chunk_size=1000):
            yield process_chunk(chunk)

@dag.node
def aggregate_streams(chunks: Iterator[dict]) -> dict:
    """Aggregate streaming results"""
    total = 0
    count = 0
    for chunk in chunks:
        total += chunk["sum"]
        count += chunk["count"]
    return {"average": total / count}

# Clear intermediate results
@dag.node
def memory_intensive_op(data: dict, context: Context) -> dict:
    result = expensive_operation(data)
    
    # Clear previous results to free memory
    if "large_intermediate" in context.results:
        del context.results["large_intermediate"]
    
    return result
```

### Parallel Optimization

```python
# Batch processing pattern
@dag.node
def batch_processor(items: list, batch_size: int = 100) -> list:
    """Process items in batches for efficiency"""
    results = []
    
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        # Process batch in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            batch_results = list(executor.map(process_item, batch))
        results.extend(batch_results)
    
    return results

# Resource pooling
class ResourcePool:
    def __init__(self, create_resource, max_size=10):
        self.create = create_resource
        self.pool = queue.Queue(maxsize=max_size)
        self.size = 0
        
    def acquire(self):
        try:
            return self.pool.get_nowait()
        except queue.Empty:
            if self.size < self.pool.maxsize:
                self.size += 1
                return self.create()
            return self.pool.get()  # Wait for available resource
    
    def release(self, resource):
        self.pool.put(resource)

# Use resource pool
db_pool = ResourcePool(create_db_connection, max_size=5)

@dag.node
def db_operation(query: str) -> list:
    conn = db_pool.acquire()
    try:
        return conn.execute(query)
    finally:
        db_pool.release(conn)
```

## Monitoring and Metrics

### Built-in Metrics

```python
# Access execution metrics
result = dag.run()

# Get timing information
metrics = dag.context.metrics
print(f"Total time: {metrics['execution_time']:.2f}s")
print(f"Node times: {metrics['node_times']}")

# Memory usage (if tracked)
if 'memory_usage' in metrics:
    print(f"Peak memory: {metrics['memory_usage']['peak_mb']:.2f} MB")
```

### Custom Metrics

```python
# Add custom metrics collector
class MetricsCollector:
    def __init__(self):
        self.metrics = defaultdict(list)
    
    def record(self, metric_name: str, value: float, tags: dict = None):
        self.metrics[metric_name].append({
            "value": value,
            "timestamp": time.time(),
            "tags": tags or {}
        })
    
    def get_summary(self, metric_name: str) -> dict:
        values = [m["value"] for m in self.metrics[metric_name]]
        return {
            "count": len(values),
            "mean": statistics.mean(values) if values else 0,
            "min": min(values) if values else 0,
            "max": max(values) if values else 0,
            "stdev": statistics.stdev(values) if len(values) > 1 else 0
        }

# Use in nodes
collector = MetricsCollector()

@dag.node
def monitored_process(data: dict) -> dict:
    start_time = time.time()
    
    # Process data
    result = process_data(data)
    
    # Record metrics
    collector.record("processing_time", time.time() - start_time)
    collector.record("data_size", len(data), {"type": "input"})
    collector.record("result_size", len(result), {"type": "output"})
    
    return result
```

### Progress Tracking

```python
# Progress callback
def progress_callback(node_name: str, status: str, progress: float):
    print(f"[{progress*100:.1f}%] {node_name}: {status}")

# Long-running node with progress
@dag.node
def long_process(items: list, context: Context) -> list:
    results = []
    total = len(items)
    
    for i, item in enumerate(items):
        # Update progress
        progress = i / total
        context.metadata["progress"] = {
            "node": "long_process",
            "percent": progress,
            "items_processed": i,
            "total_items": total
        }
        
        # Process item
        results.append(process_item(item))
        
        # Call progress callback if set
        if hasattr(dag, 'progress_callback'):
            dag.progress_callback("long_process", f"Processing item {i+1}/{total}", progress)
    
    return results

# Set progress callback
dag.progress_callback = progress_callback
```

## Node Lifecycle

### Lifecycle Phases

1. **Pre-validation**: Check input availability
2. **Pre-execution**: Prepare for execution
3. **Execution**: Run the node function
4. **Post-execution**: Process results
5. **Cleanup**: Release resources

### Lifecycle Hooks

```python
# Define lifecycle hooks
def pre_execute_hook(node, inputs, context):
    print(f"Starting {node.name} with inputs: {list(inputs.keys())}")
    # Can modify inputs
    return inputs

def post_execute_hook(node, inputs, result, context):
    print(f"Completed {node.name}, result type: {type(result)}")
    # Can modify result
    return result

def cleanup_hook(node, context):
    print(f"Cleaning up {node.name}")
    # Release resources, close connections, etc.

# Set hooks on specific node
dag.set_node_hooks(
    "process_data",
    pre_execute=pre_execute_hook,
    post_execute=post_execute_hook,
    cleanup=cleanup_hook
)

# Set default hooks for all nodes
dag.set_default_hooks(
    pre_execute=lambda n, i, c: print(f"Executing {n.name}"),
    post_execute=lambda n, i, r, c: r,
    cleanup=lambda n, c: None
)
```

### Resource Management

```python
# Context manager pattern
class ManagedResource:
    def __init__(self, node_name):
        self.node_name = node_name
        self.resource = None
    
    def __enter__(self):
        print(f"Acquiring resource for {self.node_name}")
        self.resource = acquire_resource()
        return self.resource
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Releasing resource for {self.node_name}")
        if self.resource:
            release_resource(self.resource)

@dag.node
def managed_operation(data: dict) -> dict:
    with ManagedResource("managed_operation") as resource:
        return resource.process(data)

# Automatic cleanup with decorators
def with_cleanup(cleanup_func):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            finally:
                cleanup_func()
        return wrapper
    return decorator

@dag.node
@with_cleanup(lambda: print("Cleaning up temp files"))
def temp_file_processor(data: dict) -> dict:
    temp_file = create_temp_file(data)
    return process_temp_file(temp_file)
```

## Best Practices

### 1. Choose the Right Execution Mode

```python
# ✅ Good: Parallel for independent I/O
@dag.node
async def fetch_user(user_id: str) -> dict:
    return await api.get_user(user_id)

@dag.node
async def fetch_orders(user_id: str) -> list:
    return await api.get_orders(user_id)

@dag.node
async def fetch_preferences(user_id: str) -> dict:
    return await api.get_preferences(user_id)

# Run async for I/O operations
result = await dag.run_async()

# ❌ Bad: Sequential for independent operations
# This would be 3x slower!
result = dag.run(mode="sequential")
```

### 2. Handle Errors Gracefully

```python
# ✅ Good: Specific error handling with fallbacks
@dag.node(retry=2, retry_delay=1.0)
def fetch_primary_data(source: str) -> dict:
    try:
        return fetch_from_primary(source)
    except PrimarySourceError:
        # Fallback to secondary
        return fetch_from_secondary(source)

# ❌ Bad: Catch-all with no recovery
@dag.node
def bad_fetch(source: str) -> dict:
    try:
        return fetch_data(source)
    except:
        return {}  # Silently fails
```

### 3. Optimize Resource Usage

```python
# ✅ Good: Stream large data
@dag.node
def process_large_file(file_path: str) -> dict:
    stats = {"lines": 0, "words": 0}
    with open(file_path) as f:
        for line in f:  # Stream line by line
            stats["lines"] += 1
            stats["words"] += len(line.split())
    return stats

# ❌ Bad: Load everything into memory
@dag.node
def bad_process_file(file_path: str) -> dict:
    with open(file_path) as f:
        content = f.read()  # Could be gigabytes!
    return {"lines": len(content.splitlines())}
```

### 4. Use Context Appropriately

```python
# ✅ Good: Context for metadata
@dag.node
def track_processing(data: dict, context: Context) -> dict:
    start = time.time()
    result = process(data)
    
    # Track performance metadata
    context.metadata["processing_times"] = context.metadata.get("processing_times", [])
    context.metadata["processing_times"].append(time.time() - start)
    
    return result

# ❌ Bad: Context for primary data flow
@dag.node
def bad_data_passing(context: Context) -> None:
    # Don't use context as a data bus
    data = context.results["previous"]  # Use connections instead!
    context.results["current"] = process(data)
```

### 5. Monitor and Profile

```python
# ✅ Good: Add monitoring to critical paths
import cProfile
import pstats

@dag.node
def critical_operation(data: dict) -> dict:
    if PROFILING_ENABLED:
        profiler = cProfile.Profile()
        profiler.enable()
    
    try:
        result = expensive_algorithm(data)
        return result
    finally:
        if PROFILING_ENABLED:
            profiler.disable()
            stats = pstats.Stats(profiler)
            stats.sort_stats('cumulative')
            stats.print_stats(10)  # Top 10 functions
```

## Summary

Effective execution in fast-dag involves:

- **Choosing the right mode**: Sequential, parallel, or async based on workload
- **Managing data flow**: Through connections and context appropriately
- **Handling errors**: With retries, fallbacks, and recovery strategies
- **Optimizing performance**: Via caching, streaming, and resource pooling
- **Monitoring execution**: With metrics, progress tracking, and profiling
- **Managing lifecycle**: Using hooks and proper resource cleanup

Master these concepts to build robust, performant workflows that scale!

Next, explore [Advanced Features](advanced-features.md) for even more capabilities.