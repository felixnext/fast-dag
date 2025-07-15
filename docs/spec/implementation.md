# Implementation Details

This document covers the technical implementation of fast-dag's core algorithms, performance optimizations, and architectural decisions.

## Architecture Overview

### Package Structure

```
fast_dag/
├── __init__.py          # Public API exports
├── core/
│   ├── __init__.py
│   ├── node.py          # Node class and utilities
│   ├── context.py       # Context implementations
│   ├── dag.py           # DAG class
│   └── fsm.py           # FSM class
├── execution/
│   ├── __init__.py
│   ├── runner.py        # Execution strategies
│   ├── scheduler.py     # Dependency resolution
│   └── parallel.py      # Parallel execution
├── validation/
│   ├── __init__.py
│   ├── graph.py         # Graph algorithms
│   └── validators.py    # Validation rules
├── serialization/       # Optional [serialize]
│   ├── __init__.py
│   ├── msgspec_impl.py  # msgspec serialization
│   └── adapters.py      # Future adapters (pydantic)
├── visualization/       # Optional [viz]
│   ├── __init__.py
│   ├── mermaid.py       # Mermaid diagrams
│   └── graphviz.py      # Graphviz diagrams
└── utils/
    ├── __init__.py
    ├── introspection.py # Function analysis
    └── typing.py        # Type utilities
```

### Core Algorithms

#### Topological Sort (DAG Execution Order)

```python
def topological_sort(nodes: dict[str, Node]) -> list[str]:
    """Kahn's algorithm for topological sorting"""
    # Build adjacency lists and in-degree counts
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    
    for node_name, node in nodes.items():
        for output_conns in node.output_connections.values():
            for target_node, _ in output_conns:
                graph[node_name].append(target_node)
                in_degree[target_node] += 1
    
    # Find nodes with no dependencies
    queue = deque([
        node for node in nodes 
        if in_degree[node] == 0
    ])
    
    result = []
    while queue:
        node = queue.popleft()
        result.append(node)
        
        # Remove node from graph
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # Check for cycles
    if len(result) != len(nodes):
        raise CycleError("Graph contains cycles")
    
    return result
```

#### Cycle Detection (Validation)

```python
def detect_cycles(nodes: dict[str, Node]) -> list[list[str]]:
    """Tarjan's algorithm for finding strongly connected components"""
    index = 0
    stack = []
    indices = {}
    lowlinks = {}
    on_stack = set()
    cycles = []
    
    def strongconnect(node: str):
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        
        # Check successors
        for output_conns in nodes[node].output_connections.values():
            for successor, _ in output_conns:
                if successor not in indices:
                    strongconnect(successor)
                    lowlinks[node] = min(lowlinks[node], lowlinks[successor])
                elif successor in on_stack:
                    lowlinks[node] = min(lowlinks[node], indices[successor])
        
        # Found SCC root
        if lowlinks[node] == indices[node]:
            component = []
            while True:
                w = stack.pop()
                on_stack.remove(w)
                component.append(w)
                if w == node:
                    break
            
            if len(component) > 1:
                cycles.append(component)
    
    for node in nodes:
        if node not in indices:
            strongconnect(node)
    
    return cycles
```

#### Parallel Execution Scheduling

```python
def schedule_parallel_execution(
    nodes: dict[str, Node],
    execution_order: list[str]
) -> list[list[str]]:
    """Group nodes into parallel execution levels"""
    levels = []
    scheduled = set()
    remaining = set(execution_order)
    
    while remaining:
        # Find nodes that can execute now
        current_level = []
        for node in remaining:
            # Check if all dependencies are satisfied
            dependencies_met = True
            for input_conn in nodes[node].input_connections.values():
                source_node, _ = input_conn
                if source_node not in scheduled:
                    dependencies_met = False
                    break
            
            if dependencies_met:
                current_level.append(node)
        
        if not current_level:
            raise SchedulingError("Cannot schedule remaining nodes")
        
        levels.append(current_level)
        scheduled.update(current_level)
        remaining.difference_update(current_level)
    
    return levels
```

### Performance Optimizations

#### 1. Lazy Property Computation

```python
class DAG:
    @cached_property
    def execution_order(self) -> list[str]:
        """Compute execution order only when needed"""
        return topological_sort(self.nodes)
    
    @cached_property
    def entry_points(self) -> list[str]:
        """Find entry points lazily"""
        entries = []
        for name, node in self.nodes.items():
            if not node.input_connections:
                entries.append(name)
        return entries
```

#### 2. Result Caching

```python
class CachedNode:
    def __init__(self, node: Node):
        self.node = node
        self._cache = {}
        self._cache_key = None
    
    def execute(self, inputs: dict[str, Any]) -> Any:
        # Generate cache key
        cache_key = self._generate_cache_key(inputs)
        
        if cache_key == self._cache_key:
            return self._cache.get("result")
        
        # Execute and cache
        result = self.node.func(**inputs)
        self._cache = {"result": result}
        self._cache_key = cache_key
        
        return result
```

#### 3. Memory-Efficient Context

```python
class StreamingContext(Context):
    """Context that can offload results to disk"""
    
    def __init__(self, memory_limit: int = 1_000_000_000):  # 1GB
        super().__init__()
        self.memory_limit = memory_limit
        self.memory_used = 0
        self.offloaded = {}
    
    def set_result(self, node_name: str, value: Any) -> None:
        size = sys.getsizeof(value)
        
        if self.memory_used + size > self.memory_limit:
            # Offload least recently used results
            self._offload_results()
        
        super().set_result(node_name, value)
        self.memory_used += size
```

### Execution Strategies

#### Sequential Execution

```python
async def execute_sequential(
    nodes: dict[str, Node],
    order: list[str],
    context: Context
) -> None:
    """Execute nodes one by one"""
    for node_name in order:
        node = nodes[node_name]
        inputs = gather_inputs(node, context)
        
        try:
            if asyncio.iscoroutinefunction(node.func):
                result = await node.func(**inputs)
            else:
                result = node.func(**inputs)
            
            process_result(node, result, context)
            
        except Exception as e:
            handle_node_error(node_name, e, context)
```

#### Parallel Execution

```python
async def execute_parallel(
    nodes: dict[str, Node],
    levels: list[list[str]],
    context: Context,
    max_workers: int = 4
) -> None:
    """Execute independent nodes in parallel"""
    semaphore = asyncio.Semaphore(max_workers)
    
    for level in levels:
        tasks = []
        for node_name in level:
            task = execute_node_with_semaphore(
                nodes[node_name],
                context,
                semaphore
            )
            tasks.append(task)
        
        # Wait for all nodes in level
        await asyncio.gather(*tasks, return_exceptions=True)
```

### Function Introspection

#### Automatic Input/Output Detection

```python
def introspect_function(func: Callable) -> dict[str, Any]:
    """Extract metadata from function"""
    sig = inspect.signature(func)
    
    # Extract inputs (excluding 'context')
    inputs = []
    for param_name, param in sig.parameters.items():
        if param_name != 'context':
            inputs.append(param_name)
    
    # Extract outputs from return annotation
    outputs = []
    if sig.return_annotation != inspect.Signature.empty:
        # Handle different return type patterns
        return_type = sig.return_annotation
        
        if hasattr(return_type, '__origin__'):
            # Handle generic types like dict[str, Any]
            if return_type.__origin__ is dict:
                outputs = ["result"]  # Default
        elif isinstance(return_type, type):
            outputs = ["result"]
    
    # Extract description from docstring
    description = inspect.getdoc(func)
    
    return {
        "inputs": inputs,
        "outputs": outputs,
        "description": description,
        "is_async": asyncio.iscoroutinefunction(func)
    }
```

### State Machine Implementation

#### State Tracking

```python
class FSMExecutor:
    def __init__(self, fsm: FSM):
        self.fsm = fsm
        self.state_history = []
        self.cycle_count = 0
    
    async def execute(self, context: FSMContext) -> None:
        """Execute FSM with cycle management"""
        current_state = self.fsm.initial_state
        
        while not self._should_stop(current_state):
            # Record state
            self.state_history.append(current_state)
            context.state_history.append(current_state)
            
            # Execute state node
            node = self.fsm.nodes[current_state]
            result = await self._execute_node(node, context)
            
            # Store cycle result
            if current_state not in context.cycle_results:
                context.cycle_results[current_state] = []
            context.cycle_results[current_state].append(result)
            
            # Determine next state
            if isinstance(result, FSMReturn):
                if result.stop:
                    break
                current_state = result.next_state or current_state
            else:
                # Use transition table
                current_state = self._get_next_state(current_state, result)
            
            self.cycle_count += 1
            context.cycle_count = self.cycle_count
    
    def _should_stop(self, state: str) -> bool:
        """Check termination conditions"""
        return (
            state in self.fsm.terminal_states or
            self.cycle_count >= self.fsm.max_cycles
        )
```

### Error Handling Architecture

#### Error Propagation

```python
class NodeExecutionError(Exception):
    """Enhanced error with context"""
    def __init__(
        self,
        node_name: str,
        original_error: Exception,
        inputs: dict[str, Any],
        partial_results: dict[str, Any]
    ):
        self.node_name = node_name
        self.original_error = original_error
        self.inputs = inputs
        self.partial_results = partial_results
        
        super().__init__(
            f"Error in node '{node_name}': {original_error}"
        )

class ErrorHandler:
    def __init__(self, strategy: str = "stop"):
        self.strategy = strategy
        self.error_log = []
    
    def handle_error(
        self,
        error: NodeExecutionError,
        context: Context
    ) -> str:
        """Decide how to handle error"""
        self.error_log.append(error)
        
        if self.strategy == "stop":
            raise error
        elif self.strategy == "continue":
            # Mark node as failed
            context.set_result(
                error.node_name,
                ErrorResult(error)
            )
            return "continue"
        elif self.strategy == "retry":
            return "retry"
```

### Serialization Architecture

#### Pluggable Serialization

```python
from typing import Protocol

class Serializer(Protocol):
    """Protocol for serialization implementations"""
    def serialize(self, obj: Any) -> bytes: ...
    def deserialize(self, data: bytes, type_: type[T]) -> T: ...

class MsgspecSerializer:
    """High-performance msgspec implementation"""
    def __init__(self):
        import msgspec
        self.encoder = msgspec.msgpack.Encoder()
        self.decoder = msgspec.msgpack.Decoder()
    
    def serialize(self, obj: Any) -> bytes:
        return self.encoder.encode(obj)
    
    def deserialize(self, data: bytes, type_: type[T]) -> T:
        return self.decoder.decode(data, type=type_)

# Future: PydanticSerializer for compatibility
```

### Performance Metrics

#### Metric Collection

```python
@dataclass
class NodeMetrics:
    """Per-node execution metrics"""
    start_time: float
    end_time: float
    duration: float
    memory_before: int
    memory_after: int
    cache_hit: bool = False
    retry_count: int = 0

class MetricsCollector:
    def __init__(self):
        self.node_metrics: dict[str, NodeMetrics] = {}
        self.total_start = None
        self.total_end = None
    
    @contextmanager
    def measure_node(self, node_name: str):
        """Context manager for node metrics"""
        start = time.perf_counter()
        mem_before = self._get_memory_usage()
        
        try:
            yield
        finally:
            end = time.perf_counter()
            mem_after = self._get_memory_usage()
            
            self.node_metrics[node_name] = NodeMetrics(
                start_time=start,
                end_time=end,
                duration=end - start,
                memory_before=mem_before,
                memory_after=mem_after
            )
```

## Threading and Concurrency

### Thread Safety

All core classes are designed to be thread-safe for read operations but require external synchronization for writes:

```python
class ThreadSafeDAG:
    def __init__(self, dag: DAG):
        self.dag = dag
        self._lock = threading.RLock()
    
    def add_node(self, node: Node) -> None:
        with self._lock:
            self.dag.add_node(node)
    
    def run(self, **kwargs) -> Any:
        # Execution creates new context, so it's thread-safe
        return self.dag.run(**kwargs)
```

### Async Context Vars

For async execution, we use contextvars for proper context isolation:

```python
from contextvars import ContextVar

current_execution_context: ContextVar[Context] = ContextVar(
    'current_execution_context',
    default=None
)

def get_current_context() -> Context | None:
    """Get context for current async task"""
    return current_execution_context.get()
```

## Future Optimizations

1. **JIT Compilation**: Use Numba for numeric workflows
2. **Distributed Execution**: Ray/Dask integration
3. **GPU Acceleration**: CuPy for array operations
4. **Incremental Execution**: Only re-run changed nodes
5. **Workflow Compilation**: Pre-compile to optimized execution plan