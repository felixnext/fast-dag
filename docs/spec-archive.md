# Workflow Library Technical Specification

## Overview

A Python library for building and executing both Directed Acyclic Graph (DAG) workflows and Finite State Machine (FSM) workflows with support for synchronous, asynchronous, and parallel execution.

## Core Architecture

### Base Classes

> For each of the dataclasses we want to have serialization and deserialization
> so that we can store them in DB, json or yaml.

```python
from dataclasses import dataclass, field
from typing import Any, Callable
from abc import ABC, abstractmethod
import asyncio

@dataclass
class Context:
    """Base context object passed between nodes."""
    results: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    # exec info might need to be updated - it should include execution info of previous nodes
    # each node has a exec order (parallel = multiple nodes with same exec order)
    # then there should be detailed datatype of execution info (e.g. time, memory, etc.)
    execution_info: dict[str, Any] = field(default_factory=dict)
    
    def get_result(self, node_name: str) -> Any:
        """Get result by node name"""
        return self.results.get(node_name)
    
    def set_result(self, node_name: str, value: Any) -> None:
        """Set result for node"""
        self.results[node_name] = value

# NOTE: FSM needs special context - as it requires tracking of cycles and states.
    
@dataclass
class ConditionalReturn:
    """Special return type for conditional nodes
    
    `condition` is the boolean eval. `value` is the value that is passed on.
    The conditional class then needs to define two outputs: true and false, for the branches.
    """
    condition: bool
    value: Any = None

@dataclass
class SelectReturn:
    """Special return type for select nodes"""
    value: Any = None
    branch: str = None

@dataclass
class FSMReturn:
    """Special return type for FSM state transitions"""
    next_state: str | None = None
    value: Any = None
    stop_condition: bool = False

class Node:
    """Core node class representing a unit of work"""
    
    def __init__(
        self,
        func: Callable,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None,
        node_type: str = "standard"
    ):
        self.func = func
        self.name = name or func.__name__
        self.inputs = inputs or self._infer_inputs()
        self.outputs = outputs or self._infer_outputs()
        self.description = description or func.__doc__
        self.node_type = node_type  # "standard", "condition", "fsm_state"
        
        # Connection tracking
        self.input_connections: Dict[str, 'Node'] = {}
        self.output_connections: Dict[str, List['Node']] = {}
        
        # Validation
        self._validate_signature()
    
    def _infer_inputs(self) -> list[str]:
        """Infer inputs from function signature"""
        import inspect
        sig = inspect.signature(self.func)
        return [p.name for p in sig.parameters.values() 
                if p.name != 'context']
    
    def _infer_outputs(self) -> list[str]:
        """Infer outputs from function signature/return annotation"""
        import inspect
        sig = inspect.signature(self.func)
        if sig.return_annotation != inspect.Signature.empty:
            # Handle Union, Tuple annotations, etc.
            return ['result']  # Simplified for now
        return ['result']
    
    def _validate_signature(self) -> None:
        """Validate function signature matches declared inputs/outputs"""
        # Implementation would check parameter names, types, etc.
        pass
    
    async def execute(self, context: Context, **kwargs) -> Any:
        """Execute the node function"""
        import inspect
        sig = inspect.signature(self.func)
        
        # Prepare arguments
        func_args = {}
        for param_name in sig.parameters:
            if param_name == 'context':
                func_args['context'] = context
            elif param_name in kwargs:
                func_args[param_name] = kwargs[param_name]
        
        # Execute function
        if asyncio.iscoroutinefunction(self.func):
            result = await self.func(**func_args)
        else:
            result = self.func(**func_args)
        
        return result
    
    # Operator overloading for connections
    def __rshift__(self, other: 'Node') -> 'Node':
        """>> operator for connecting nodes"""
        return self.connect_to(other)
    
    def __or__(self, other: 'Node') -> 'Node':
        """| operator for connecting nodes"""
        return self.connect_to(other)
    
    def connect_to(
        self, 
        other: 'Node', 
        output_name: str = None, 
        input_name: str = None
    ) -> 'Node':
        """Connect this node's output to another node's input"""
        output_name = output_name or self.outputs[0]
        input_name = input_name or other.inputs[0]
        
        # Track connections
        if output_name not in self.output_connections:
            self.output_connections[output_name] = []
        self.output_connections[output_name].append(other)
        other.input_connections[input_name] = self
        
        return other  # Enable chaining
```

### Workflow Base Classes

```python
class WorkflowBase(ABC):
    """Base class for DAG and FSM workflows"""
    
    def __init__(self, name: str = "workflow"):
        self.name = name
        self.nodes: dict[str, Node] = {}
        self.entrypoints: list[str] = []
        self.context = Context()
        
    def node(
        self,
        name: str | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        description: str | None = None
    ):
        """Decorator to create and add nodes to workflow"""
        def decorator(func: Callable) -> Node:
            node = Node(
                func=func,
                name=name,
                inputs=inputs,
                outputs=outputs,
                description=description
            )
            self.add_node(node)
            return node
        return decorator
    
    def condition(
        self,
        name: Optional[str] = None,
        true_branch: Optional[str] = None,
        false_branch: Optional[str] = None
    ):
        """Decorator for conditional nodes"""
        def decorator(func: Callable) -> Node:
            node = Node(
                func=func,
                name=name,
                node_type="condition"
            )
            node.true_branch = true_branch
            node.false_branch = false_branch
            self.add_node(node)
            return node
        return decorator
    
    def add_node(self, node: Node) -> None:
        """Add node to workflow"""
        if node.name in self.nodes:
            raise ValueError(f"Node '{node.name}' already exists")
        self.nodes[node.name] = node
    
    def set_entrypoint(self, *node_names: str) -> None:
        """Set workflow entrypoints"""
        self.entrypoints = list(node_names)
    
    def validate(self) -> List[str]:
        """Validate workflow structure and return errors"""
        errors = []
        
        # Check for unconnected inputs
        for node in self.nodes.values():
            for input_name in node.inputs:
                if input_name not in node.input_connections:
                    if node.name not in self.entrypoints:
                        errors.append(f"Node '{node.name}' input '{input_name}' not connected")
        
        # Check for cycles in DAG (if applicable)
        if isinstance(self, DAG):
            if self._has_cycles():
                errors.append("DAG contains cycles")
        
        return errors
    
    def get_dependencies(self, node_name: str) -> Set[str]:
        """Get all upstream dependencies for a node"""
        dependencies = set()
        
        def _collect_deps(name: str):
            node = self.nodes[name]
            for input_conn in node.input_connections.values():
                if input_conn.name not in dependencies:
                    dependencies.add(input_conn.name)
                    _collect_deps(input_conn.name)
        
        _collect_deps(node_name)
        return dependencies
    
    def get_downstream(self, node_name: str) -> Set[str]:
        """Get all downstream nodes for a node"""
        downstream = set()
        
        def _collect_downstream(name: str):
            node = self.nodes[name]
            for output_conns in node.output_connections.values():
                for conn_node in output_conns:
                    if conn_node.name not in downstream:
                        downstream.add(conn_node.name)
                        _collect_downstream(conn_node.name)
        
        _collect_downstream(node_name)
        return downstream
    
    # Result access methods
    def __getitem__(self, node_name: str) -> Any:
        """Access results via workflow['node_name']"""
        return self.get_result(node_name)
    
    def get_result(self, node_name: str) -> Any:
        """Get result by node name"""
        return self.context.get_result(node_name)
    
    @property
    def results(self) -> Dict[str, Any]:
        """Access all results"""
        return self.context.results

class DAG(WorkflowBase):
    """Directed Acyclic Graph workflow"""
    
    def _has_cycles(self) -> bool:
        """Check for cycles using DFS"""
        visited = set()
        rec_stack = set()
        
        def _has_cycle_util(node_name: str) -> bool:
            visited.add(node_name)
            rec_stack.add(node_name)
            
            node = self.nodes[node_name]
            for output_conns in node.output_connections.values():
                for conn_node in output_conns:
                    if conn_node.name not in visited:
                        if _has_cycle_util(conn_node.name):
                            return True
                    elif conn_node.name in rec_stack:
                        return True
            
            rec_stack.remove(node_name)
            return False
        
        for node_name in self.nodes:
            if node_name not in visited:
                if _has_cycle_util(node_name):
                    return True
        return False

class FSM(WorkflowBase):
    """Finite State Machine workflow"""
    
    def __init__(self, name: str = "fsm"):
        super().__init__(name)
        self.current_state: Optional[str] = None
        self.state_history: List[str] = []
        self.cycle_count: int = 0
        self.max_cycles: int = 1000  # Safety limit
        
    def state(
        self,
        name: Optional[str] = None,
        is_initial: bool = False,
        is_final: bool = False
    ):
        """Decorator for FSM state nodes"""
        def decorator(func: Callable) -> Node:
            node = Node(
                func=func,
                name=name,
                node_type="fsm_state"
            )
            node.is_initial = is_initial
            node.is_final = is_final
            self.add_node(node)
            
            if is_initial:
                self.set_entrypoint(node.name)
                
            return node
        return decorator
    
    def get_result(self, node_name: str) -> Any:
        """Get latest result for FSM node (supports .X cycle notation)"""
        if '.' in node_name:
            base_name, cycle_str = node_name.split('.', 1)
            cycle_num = int(cycle_str)
            result_key = f"{base_name}.{cycle_num}"
        else:
            # Find latest cycle for this node
            base_name = node_name
            latest_cycle = -1
            for key in self.context.results.keys():
                if key.startswith(f"{base_name}."):
                    cycle_num = int(key.split('.')[1])
                    latest_cycle = max(latest_cycle, cycle_num)
            
            if latest_cycle >= 0:
                result_key = f"{base_name}.{latest_cycle}"
            else:
                result_key = base_name
        
        return self.context.get_result(result_key)
```

### Execution Engine

```python
@dataclass
class ExecutionMetrics:
    """Performance metrics for workflow execution"""
    total_time: float = 0.0
    node_times: Dict[str, float] = field(default_factory=dict)
    nodes_executed: int = 0
    errors: List[str] = field(default_factory=list)
    execution_order: List[str] = field(default_factory=list)

class Runner:
    """Workflow execution engine"""
    
    def __init__(self, workflow: WorkflowBase):
        self.workflow = workflow
        self.metrics = ExecutionMetrics()
    
    def run(
        self,
        inputs: Optional[Dict[str, Any]] = None,
        mode: str = "sequential"  # "sequential", "parallel"
    ) -> ExecutionMetrics:
        """Run workflow synchronously"""
        return asyncio.run(self.run_async(inputs, mode))
    
    async def run_async(
        self,
        inputs: Optional[Dict[str, Any]] = None,
        mode: str = "sequential"
    ) -> ExecutionMetrics:
        """Run workflow asynchronously"""
        import time
        start_time = time.time()
        
        try:
            if isinstance(self.workflow, DAG):
                await self._execute_dag(inputs or {}, mode)
            elif isinstance(self.workflow, FSM):
                await self._execute_fsm(inputs or {}, mode)
            else:
                raise ValueError(f"Unknown workflow type: {type(self.workflow)}")
                
        except Exception as e:
            self.metrics.errors.append(str(e))
            raise
        finally:
            self.metrics.total_time = time.time() - start_time
        
        return self.metrics
    
    async def _execute_dag(
        self,
        inputs: Dict[str, Any],
        mode: str
    ) -> None:
        """Execute DAG workflow"""
        # Initialize context with inputs
        for key, value in inputs.items():
            self.workflow.context.set_result(key, value)
        
        # Topological sort for execution order
        execution_order = self._topological_sort()
        
        if mode == "parallel":
            await self._execute_parallel(execution_order)
        else:
            await self._execute_sequential(execution_order)
    
    async def _execute_fsm(
        self,
        inputs: Dict[str, Any],
        mode: str
    ) -> None:
        """Execute FSM workflow"""
        # Initialize context
        for key, value in inputs.items():
            self.workflow.context.set_result(key, value)
        
        current_state = self.workflow.entrypoints[0]
        self.workflow.current_state = current_state
        cycle = 0
        
        while current_state and cycle < self.workflow.max_cycles:
            cycle += 1
            self.workflow.cycle_count = cycle
            
            # Execute current state
            node = self.workflow.nodes[current_state]
            result = await self._execute_node(node, cycle)
            
            # Handle FSM result
            if isinstance(result, FSMReturn):
                if result.stop_condition:
                    break
                current_state = result.next_state
                if not result.continue_execution:
                    break
            else:
                # Determine next state from connections
                current_state = self._get_next_state(node, result)
            
            self.workflow.state_history.append(current_state or "END")
    
    async def _execute_node(self, node: Node, cycle: int = 0) -> Any:
        """Execute a single node"""
        import time
        
        start_time = time.time()
        
        try:
            # Prepare inputs
            inputs = {}
            for input_name, source_node in node.input_connections.items():
                inputs[input_name] = self.workflow.get_result(source_node.name)
            
            # Execute
            result = await node.execute(self.workflow.context, **inputs)
            
            # Store result
            result_key = f"{node.name}.{cycle}" if cycle > 0 else node.name
            self.workflow.context.set_result(result_key, result)
            
            # Update metrics
            execution_time = time.time() - start_time
            self.metrics.node_times[node.name] = execution_time
            self.metrics.nodes_executed += 1
            self.metrics.execution_order.append(node.name)
            
            return result
            
        except Exception as e:
            error_msg = f"Error in node '{node.name}': {str(e)}"
            self.metrics.errors.append(error_msg)
            raise RuntimeError(error_msg) from e
    
    def _topological_sort(self) -> List[str]:
        """Topological sort for DAG execution order"""
        in_degree = {name: 0 for name in self.workflow.nodes}
        
        # Calculate in-degrees
        for node in self.workflow.nodes.values():
            for output_conns in node.output_connections.values():
                for conn_node in output_conns:
                    in_degree[conn_node.name] += 1
        
        # Start with nodes that have no dependencies
        queue = [name for name, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            current = queue.pop(0)
            result.append(current)
            
            # Update in-degrees of connected nodes
            node = self.workflow.nodes[current]
            for output_conns in node.output_connections.values():
                for conn_node in output_conns:
                    in_degree[conn_node.name] -= 1
                    if in_degree[conn_node.name] == 0:
                        queue.append(conn_node.name)
        
        return result
    
    async def _execute_parallel(self, execution_order: List[str]) -> None:
        """Execute nodes in parallel where possible"""
        executed = set()
        
        while len(executed) < len(execution_order):
            # Find nodes ready to execute
            ready_nodes = []
            for node_name in execution_order:
                if node_name in executed:
                    continue
                
                node = self.workflow.nodes[node_name]
                dependencies = set(conn.name for conn in node.input_connections.values())
                
                if dependencies.issubset(executed):
                    ready_nodes.append(node)
            
            if not ready_nodes:
                break
            
            # Execute ready nodes in parallel
            tasks = [self._execute_node(node) for node in ready_nodes]
            await asyncio.gather(*tasks)
            
            executed.update(node.name for node in ready_nodes)
    
    async def _execute_sequential(self, execution_order: List[str]) -> None:
        """Execute nodes sequentially"""
        for node_name in execution_order:
            node = self.workflow.nodes[node_name]
            await self._execute_node(node)
```

## Enhanced Parallel Execution Engine

### Advanced Parallel Execution

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Dict, List, Set, Tuple, Optional
import multiprocessing as mp
from dataclasses import dataclass

@dataclass
class ExecutionConfig:
    """Configuration for workflow execution"""
    mode: str = "sequential"  # "sequential", "parallel", "distributed"
    max_workers: int = mp.cpu_count()
    use_process_pool: bool = False  # For CPU-bound tasks
    memory_limit_mb: Optional[int] = None
    timeout_seconds: Optional[float] = None
    retry_on_failure: bool = True
    enable_checkpointing: bool = False
    checkpoint_interval: int = 10  # Checkpoint every N nodes

class ParallelExecutor:
    """Advanced parallel execution engine"""
    
    def __init__(self, config: ExecutionConfig = None):
        self.config = config or ExecutionConfig()
        self.thread_pool = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self.process_pool = ProcessPoolExecutor(max_workers=self.config.max_workers) if self.config.use_process_pool else None
        self.semaphore = asyncio.Semaphore(self.config.max_workers)
        
    async def execute_dag(self, dag: DAG, inputs: Dict[str, Any]) -> ExecutionMetrics:
        """Execute DAG with advanced parallel strategies"""
        # Analyze DAG structure
        analysis = self._analyze_dag_structure(dag)
        
        # Choose execution strategy
        if analysis.max_parallelism == 1:
            return await self._execute_sequential(dag, inputs)
        elif analysis.has_heavy_computation:
            return await self._execute_hybrid(dag, inputs, analysis)
        else:
            return await self._execute_parallel_async(dag, inputs, analysis)
    
    def _analyze_dag_structure(self, dag: DAG) -> 'DAGAnalysis':
        """Analyze DAG for optimal execution strategy"""
        analysis = DAGAnalysis()
        
        # Calculate parallelism levels
        levels = self._calculate_levels(dag)
        analysis.levels = levels
        analysis.max_parallelism = max(len(level) for level in levels.values())
        
        # Identify critical path
        analysis.critical_path = self._find_critical_path(dag)
        
        # Check for heavy computation nodes
        for node in dag.nodes.values():
            if hasattr(node.func, '__annotations__'):
                # Check for CPU-bound indicators
                if 'numpy' in str(node.func.__annotations__) or \
                   'pandas' in str(node.func.__annotations__):
                    analysis.has_heavy_computation = True
                    analysis.heavy_nodes.add(node.name)
        
        return analysis
    
    def _calculate_levels(self, dag: DAG) -> Dict[int, List[str]]:
        """Calculate execution levels for parallel execution"""
        levels = {}
        visited = set()
        
        def assign_level(node_name: str, level: int = 0):
            if node_name in visited:
                return
            
            visited.add(node_name)
            node = dag.nodes[node_name]
            
            # Calculate max level from dependencies
            max_dep_level = -1
            for dep_node in node.input_connections.values():
                if dep_node.name not in visited:
                    assign_level(dep_node.name, level)
                max_dep_level = max(max_dep_level, node_levels.get(dep_node.name, -1))
            
            current_level = max_dep_level + 1
            node_levels[node_name] = current_level
            
            if current_level not in levels:
                levels[current_level] = []
            levels[current_level].append(node_name)
        
        node_levels = {}
        
        # Start from entry points
        for entry in dag.entrypoints:
            assign_level(entry)
        
        # Process any unvisited nodes
        for node_name in dag.nodes:
            if node_name not in visited:
                assign_level(node_name)
        
        return levels
    
    async def _execute_parallel_async(
        self, 
        dag: DAG, 
        inputs: Dict[str, Any], 
        analysis: 'DAGAnalysis'
    ) -> ExecutionMetrics:
        """Execute DAG with async parallel execution"""
        metrics = ExecutionMetrics()
        context = dag.context
        
        # Initialize inputs
        for key, value in inputs.items():
            context.set_result(key, value)
        
        # Execute by levels
        for level, node_names in sorted(analysis.levels.items()):
            # Execute all nodes at this level in parallel
            tasks = []
            for node_name in node_names:
                node = dag.nodes[node_name]
                task = self._execute_node_async(node, context, metrics)
                tasks.append(task)
            
            # Wait for all nodes at this level to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle any errors
            for result, node_name in zip(results, node_names):
                if isinstance(result, Exception):
                    metrics.errors.append(f"Error in {node_name}: {str(result)}")
                    if not self.config.retry_on_failure:
                        raise result
        
        return metrics
    
    async def _execute_node_async(
        self, 
        node: Node, 
        context: Context, 
        metrics: ExecutionMetrics
    ) -> Any:
        """Execute single node with resource management"""
        async with self.semaphore:  # Limit concurrent executions
            import time
            start_time = time.time()
            
            try:
                # Prepare inputs
                inputs = {}
                for input_name, source_node in node.input_connections.items():
                    inputs[input_name] = context.get_result(source_node.name)
                
                # Execute with timeout if configured
                if self.config.timeout_seconds:
                    result = await asyncio.wait_for(
                        node.execute(context, **inputs),
                        timeout=self.config.timeout_seconds
                    )
                else:
                    result = await node.execute(context, **inputs)
                
                # Store result
                context.set_result(node.name, result)
                
                # Update metrics
                execution_time = time.time() - start_time
                metrics.node_times[node.name] = execution_time
                metrics.nodes_executed += 1
                metrics.execution_order.append(node.name)
                
                return result
                
            except asyncio.TimeoutError:
                error_msg = f"Node '{node.name}' timed out after {self.config.timeout_seconds}s"
                metrics.errors.append(error_msg)
                raise
            except Exception as e:
                error_msg = f"Error in node '{node.name}': {str(e)}"
                metrics.errors.append(error_msg)
                raise
    
    async def _execute_hybrid(
        self, 
        dag: DAG, 
        inputs: Dict[str, Any], 
        analysis: 'DAGAnalysis'
    ) -> ExecutionMetrics:
        """Hybrid execution using both threads and processes"""
        metrics = ExecutionMetrics()
        context = dag.context
        
        # Initialize inputs
        for key, value in inputs.items():
            context.set_result(key, value)
        
        # Execute by levels with appropriate executor
        for level, node_names in sorted(analysis.levels.items()):
            tasks = []
            
            for node_name in node_names:
                node = dag.nodes[node_name]
                
                if node_name in analysis.heavy_nodes and self.process_pool:
                    # Use process pool for CPU-bound tasks
                    task = self._execute_in_process(node, context, metrics)
                else:
                    # Use async for I/O-bound tasks
                    task = self._execute_node_async(node, context, metrics)
                
                tasks.append(task)
            
            await asyncio.gather(*tasks)
        
        return metrics
    
    async def _execute_in_process(
        self, 
        node: Node, 
        context: Context, 
        metrics: ExecutionMetrics
    ) -> Any:
        """Execute CPU-bound node in separate process"""
        loop = asyncio.get_event_loop()
        
        # Prepare serializable inputs
        inputs = {}
        for input_name, source_node in node.input_connections.items():
            inputs[input_name] = context.get_result(source_node.name)
        
        # Execute in process pool
        result = await loop.run_in_executor(
            self.process_pool,
            self._execute_node_sync,
            node,
            inputs
        )
        
        # Store result
        context.set_result(node.name, result)
        return result
    
    def _execute_node_sync(self, node: Node, inputs: Dict[str, Any]) -> Any:
        """Synchronous node execution for process pool"""
        # Create a new event loop for this process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Create minimal context
            context = Context()
            
            # Execute node
            if asyncio.iscoroutinefunction(node.func):
                result = loop.run_until_complete(node.execute(context, **inputs))
            else:
                result = node.func(**inputs)
            
            return result
        finally:
            loop.close()
    
    def _find_critical_path(self, dag: DAG) -> List[str]:
        """Find the critical path through the DAG"""
        # Simplified critical path - longest path from entry to exit
        paths = []
        
        def find_paths(node_name: str, current_path: List[str]):
            current_path = current_path + [node_name]
            node = dag.nodes[node_name]
            
            if not node.output_connections:
                paths.append(current_path)
                return
            
            for output_nodes in node.output_connections.values():
                for next_node in output_nodes:
                    find_paths(next_node.name, current_path)
        
        for entry in dag.entrypoints:
            find_paths(entry, [])
        
        # Return longest path
        return max(paths, key=len) if paths else []
    
    def cleanup(self):
        """Clean up executor resources"""
        self.thread_pool.shutdown(wait=True)
        if self.process_pool:
            self.process_pool.shutdown(wait=True)

@dataclass
class DAGAnalysis:
    """Analysis results for DAG structure"""
    levels: Dict[int, List[str]] = field(default_factory=dict)
    max_parallelism: int = 1
    critical_path: List[str] = field(default_factory=list)
    has_heavy_computation: bool = False
    heavy_nodes: Set[str] = field(default_factory=set)

### Resource Management

class ResourceManager:
    """Manage resources for workflow execution"""
    
    def __init__(self, memory_limit_mb: Optional[int] = None):
        self.memory_limit_mb = memory_limit_mb
        self.node_memory_usage: Dict[str, int] = {}
        self.current_memory_usage = 0
        self.memory_lock = asyncio.Lock()
    
    async def acquire_resources(self, node: Node, estimated_memory_mb: int = 100) -> bool:
        """Acquire resources for node execution"""
        if not self.memory_limit_mb:
            return True
        
        async with self.memory_lock:
            if self.current_memory_usage + estimated_memory_mb > self.memory_limit_mb:
                # Wait for resources to be available
                await self._wait_for_resources(estimated_memory_mb)
            
            self.current_memory_usage += estimated_memory_mb
            self.node_memory_usage[node.name] = estimated_memory_mb
            return True
    
    async def release_resources(self, node: Node) -> None:
        """Release resources after node execution"""
        if not self.memory_limit_mb:
            return
        
        async with self.memory_lock:
            memory_used = self.node_memory_usage.get(node.name, 0)
            self.current_memory_usage -= memory_used
            del self.node_memory_usage[node.name]
    
    async def _wait_for_resources(self, required_memory_mb: int) -> None:
        """Wait for sufficient resources to become available"""
        while self.current_memory_usage + required_memory_mb > self.memory_limit_mb:
            await asyncio.sleep(0.1)

### Distributed Execution

class DistributedExecutor:
    """Distributed workflow execution across multiple machines"""
    
    def __init__(self, coordinator_url: str, worker_id: str):
        self.coordinator_url = coordinator_url
        self.worker_id = worker_id
        self.task_queue: asyncio.Queue = asyncio.Queue()
        self.result_queue: asyncio.Queue = asyncio.Queue()
    
    async def connect_to_coordinator(self) -> None:
        """Connect to the workflow coordinator"""
        # Implementation would use actual networking (e.g., gRPC, REST API)
        pass
    
    async def execute_as_worker(self) -> None:
        """Run as a worker node"""
        await self.connect_to_coordinator()
        
        while True:
            # Get task from coordinator
            task = await self.task_queue.get()
            
            if task is None:  # Shutdown signal
                break
            
            # Execute task
            result = await self._execute_task(task)
            
            # Send result back
            await self.result_queue.put(result)
    
    async def distribute_dag(self, dag: DAG, inputs: Dict[str, Any]) -> ExecutionMetrics:
        """Distribute DAG execution across workers"""
        # Analyze DAG and partition work
        partitions = self._partition_dag(dag)
        
        # Distribute partitions to workers
        tasks = []
        for partition in partitions:
            task = self._send_partition_to_worker(partition)
            tasks.append(task)
        
        # Collect results
        results = await asyncio.gather(*tasks)
        
        # Merge results
        return self._merge_results(results)
    
    def _partition_dag(self, dag: DAG) -> List['DAGPartition']:
        """Partition DAG for distributed execution"""
        # Implementation would use graph partitioning algorithms
        # to minimize communication between partitions
        pass

### Execution Optimization

class ExecutionOptimizer:
    """Optimize workflow execution plans"""
    
    def optimize_execution_plan(self, dag: DAG) -> 'ExecutionPlan':
        """Generate optimized execution plan"""
        plan = ExecutionPlan()
        
        # Identify fusion opportunities
        plan.fusion_groups = self._identify_fusion_opportunities(dag)
        
        # Identify caching opportunities
        plan.cacheable_nodes = self._identify_cacheable_nodes(dag)
        
        # Optimize data transfer
        plan.data_transfer_plan = self._optimize_data_transfer(dag)
        
        return plan
    
    def _identify_fusion_opportunities(self, dag: DAG) -> List[List[str]]:
        """Identify nodes that can be fused for better performance"""
        fusion_groups = []
        
        # Look for linear chains that can be fused
        for node in dag.nodes.values():
            if len(node.output_connections) == 1:
                # Check if this could be start of a fusion chain
                chain = self._find_fusion_chain(node, dag)
                if len(chain) > 1:
                    fusion_groups.append(chain)
        
        return fusion_groups
    
    def _identify_cacheable_nodes(self, dag: DAG) -> Set[str]:
        """Identify nodes whose results should be cached"""
        cacheable = set()
        
        for node in dag.nodes.values():
            # Cache nodes with multiple consumers
            total_consumers = sum(len(conns) for conns in node.output_connections.values())
            if total_consumers > 1:
                cacheable.add(node.name)
            
            # Cache expensive computations
            if node.name in self._get_expensive_nodes(dag):
                cacheable.add(node.name)
        
        return cacheable

@dataclass
class ExecutionPlan:
    """Optimized execution plan"""
    fusion_groups: List[List[str]] = field(default_factory=list)
    cacheable_nodes: Set[str] = field(default_factory=set)
    data_transfer_plan: Dict[str, str] = field(default_factory=dict)
```

## Usage Examples

### Basic DAG Workflow

```python
# Create workflow
workflow = DAG("data_processing")

@workflow.node()
def load_data(file_path: str) -> dict:
    # Load data implementation
    return {"data": "loaded"}

@workflow.node()
def process_data(data: dict, context: Context) -> dict:
    # Process data implementation
    return {"processed": True}

@workflow.node()
def save_results(processed: dict) -> bool:
    # Save implementation
    return True

# Connect nodes using operators
load_data >> process_data >> save_results

# Or using method chaining
load_data.connect_to(process_data).connect_to(save_results)

# Execute
runner = Runner(workflow)
metrics = runner.run(inputs={"file_path": "data.csv"})

# Access results
result = workflow["save_results"]  # or workflow.get_result("save_results")
```

### Conditional Workflow

```python
workflow = DAG("conditional_example")

@workflow.node()
def check_condition(value: int) -> ConditionalReturn:
    return ConditionalReturn(
        condition=value > 10,
        true_branch="handle_large",
        false_branch="handle_small",
        value=value
    )

@workflow.condition(true_branch="process_large", false_branch="process_small")
def branch_logic(value: int) -> ConditionalReturn:
    return ConditionalReturn(condition=value > 100)

@workflow.node()
def process_large(value: int) -> str:
    return f"Large: {value}"

@workflow.node()
def process_small(value: int) -> str:
    return f"Small: {value}"
```

### FSM Workflow

```python
fsm = FSM("state_machine")

@fsm.state(is_initial=True)
def initial_state(data: dict) -> FSMReturn:
    if data["ready"]:
        return FSMReturn(next_state="processing", value=data)
    return FSMReturn(next_state="waiting", value=data)

@fsm.state()
def processing(data: dict) -> FSMReturn:
    # Process data
    if data["complete"]:
        return FSMReturn(next_state="final", stop_condition=True)
    return FSMReturn(next_state="processing", value=data)

@fsm.state(is_final=True)
def final(data: dict) -> FSMReturn:
    return FSMReturn(stop_condition=True, value="complete")

# Execute FSM
runner = Runner(fsm)
metrics = runner.run(inputs={"data": {"ready": True, "complete": False}})

# Access latest results
latest_result = fsm["processing"]  # Latest cycle result
specific_result = fsm["processing.2"]  # Specific cycle result
```

## Visualization Support

```python
class Visualizer:
    """Workflow visualization generator"""
    
    def __init__(self, workflow: WorkflowBase):
        self.workflow = workflow
    
    def to_mermaid(self, include_results: bool = False) -> str:
        """Generate Mermaid diagram"""
        lines = ["graph TD"]
        
        for node in self.workflow.nodes.values():
            node_label = node.name
            if include_results and node.name in self.workflow.results:
                result = self.workflow.results[node.name]
                node_label += f"\\n[{result}]"
            
            lines.append(f"    {node.name}[\"{node_label}\"]")
            
            for output_conns in node.output_connections.values():
                for conn_node in output_conns:
                    lines.append(f"    {node.name} --> {conn_node.name}")
        
        return "\\n".join(lines)
    
    def to_graphviz(self, include_results: bool = False) -> str:
        """Generate Graphviz DOT notation"""
        lines = ["digraph workflow {"]
        
        for node in self.workflow.nodes.values():
            node_label = node.name
            if include_results and node.name in self.workflow.results:
                result = self.workflow.results[node.name]
                node_label += f"\\n[{result}]"
            
            lines.append(f'    {node.name} [label="{node_label}"];')
            
            for output_conns in node.output_connections.values():
                for conn_node in output_conns:
                    lines.append(f"    {node.name} -> {conn_node.name};")
        
        lines.append("}")
        return "\\n".join(lines)

# Usage
viz = Visualizer(workflow)
mermaid_diagram = viz.to_mermaid(include_results=True)
graphviz_diagram = viz.to_graphviz(include_results=True)
```

## Serialization Support

```python
import yaml
import json

class WorkflowSerializer:
    """Serialize/deserialize workflows to/from YAML/JSON"""
    
    @staticmethod
    def to_yaml(workflow: WorkflowBase) -> str:
        """Convert workflow to YAML"""
        data = {
            "name": workflow.name,
            "type": "DAG" if isinstance(workflow, DAG) else "FSM",
            "nodes": [],
            "connections": []
        }
        
        for node in workflow.nodes.values():
            node_data = {
                "name": node.name,
                "function": node.func.__name__,
                "inputs": node.inputs,
                "outputs": node.outputs,
                "description": node.description
            }
            data["nodes"].append(node_data)
        
        for node in workflow.nodes.values():
            for output_name, connections in node.output_connections.items():
                for conn_node in connections:
                    data["connections"].append({
                        "from": node.name,
                        "to": conn_node.name,
                        "output": output_name,
                        "input": list(conn_node.input_connections.keys())[0]
                    })
        
        return yaml.dump(data, default_flow_style=False)
    
    @staticmethod
    def from_yaml(yaml_str: str, function_registry: Dict[str, Callable]) -> WorkflowBase:
        """Create workflow from YAML"""
        data = yaml.safe_load(yaml_str)
        
        if data["type"] == "DAG":
            workflow = DAG(data["name"])
        else:
            workflow = FSM(data["name"])
        
        # Create nodes
        for node_data in data["nodes"]:
            func = function_registry[node_data["function"]]
            node = Node(
                func=func,
                name=node_data["name"],
                inputs=node_data["inputs"],
                outputs=node_data["outputs"],
                description=node_data["description"]
            )
            workflow.add_node(node)
        
        # Create connections
        for conn_data in data["connections"]:
            from_node = workflow.nodes[conn_data["from"]]
            to_node = workflow.nodes[conn_data["to"]]
            from_node.connect_to(to_node, conn_data["output"], conn_data["input"])
        
        return workflow
```

## Error Handling & Validation

```python
class WorkflowValidationError(Exception):
    """Raised when workflow validation fails"""
    pass

class WorkflowExecutionError(Exception):
    """Raised during workflow execution"""
    pass

class ValidationResult:
    """Workflow validation result"""
    
    def __init__(self):
        self.is_valid = True
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def add_error(self, message: str):
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str):
        self.warnings.append(message)

def validate_workflow(workflow: WorkflowBase) -> ValidationResult:
    """Comprehensive workflow validation"""
    result = ValidationResult()
    
    # Check for unconnected nodes
    for node in workflow.nodes.values():
        if not node.input_connections and node.name not in workflow.entrypoints:
            result.add_error(f"Node '{node.name}' has no input connections and is not an entrypoint")
        
        if not node.output_connections:
            result.add_warning(f"Node '{node.name}' has no output connections")
    
    # Check for cycles in DAG
    if isinstance(workflow, DAG) and workflow._has_cycles():
        result.add_error("DAG contains cycles")
    
    # Check function signatures
    for node in workflow.nodes.values():
        try:
            node._validate_signature()
        except Exception as e:
            result.add_error(f"Node '{node.name}' signature validation failed: {e}")
    
    return result
```

## Package Structure

```
fast-dag/
├── fast_dag/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── node.py
│   │   ├── context.py
│   │   ├── dag.py
│   │   ├── fsm.py
│   │   └── runner.py
│   ├── visualization/  # Optional [viz] extra
│   │   ├── __init__.py
│   │   ├── mermaid.py
│   │   └── graphviz.py
│   ├── serialization/
│   │   ├── __init__.py
│   │   ├── yaml_serializer.py
│   │   └── json_serializer.py
│   ├── validation/
│   │   ├── __init__.py
│   │   └── validator.py
│   └── exceptions.py
├── tests/
├── examples/
├── pyproject.toml
└── README.md
```

## Installation & Dependencies

```toml
[project]
name = "fast-dag"
version = "0.1.0"
description = "Fast DAG and FSM workflow library"
requires-python = ">=3.10"
dependencies = [
    "pyyaml>=6.0",
]

[project.optional-dependencies]
viz = [
    "graphviz>=0.20.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.ruff]
line-length = 88
target-version = "py310"

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "--cov=fast_dag --cov-report=html --cov-report=term"
```

## Testing Strategy

### Test Structure

```python
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock
from fast_dag import DAG, FSM, Node, Context, Runner
from fast_dag.exceptions import WorkflowValidationError, WorkflowExecutionError

class TestNode:
    """Test Node functionality"""
    
    def test_node_creation(self):
        """Test basic node creation"""
        def sample_func(x: int, y: int) -> int:
            return x + y
        
        node = Node(sample_func, name="adder")
        assert node.name == "adder"
        assert node.inputs == ["x", "y"]
        assert node.outputs == ["result"]
    
    def test_node_with_context(self):
        """Test node with context parameter"""
        def func_with_context(x: int, context: Context) -> int:
            context.metadata["processed"] = True
            return x * 2
        
        node = Node(func_with_context)
        assert "context" not in node.inputs
        assert node.inputs == ["x"]
    
    @pytest.mark.asyncio
    async def test_async_node_execution(self):
        """Test async node execution"""
        async def async_func(x: int) -> int:
            await asyncio.sleep(0.1)
            return x * 2
        
        node = Node(async_func)
        context = Context()
        result = await node.execute(context, x=5)
        assert result == 10
    
    def test_node_connection_operators(self):
        """Test node connection operators"""
        node1 = Node(lambda x: x * 2, name="double")
        node2 = Node(lambda x: x + 1, name="increment")
        
        # Test >> operator
        connected = node1 >> node2
        assert connected == node2
        assert node2.input_connections["x"] == node1
        
        # Test | operator
        node3 = Node(lambda x: x * x, name="square")
        connected = node1 | node3
        assert connected == node3

class TestDAGWorkflow:
    """Test DAG workflow functionality"""
    
    def test_simple_dag_creation(self):
        """Test basic DAG creation and execution"""
        dag = DAG("simple_pipeline")
        
        @dag.node()
        def start(x: int) -> int:
            return x * 2
        
        @dag.node()
        def middle(x: int) -> int:
            return x + 10
        
        @dag.node()
        def end(x: int) -> int:
            return x * 3
        
        start >> middle >> end
        
        runner = Runner(dag)
        metrics = runner.run(inputs={"x": 5})
        
        assert dag["end"] == 60  # (5 * 2 + 10) * 3
        assert metrics.nodes_executed == 3
        assert len(metrics.errors) == 0
    
    def test_parallel_branches(self):
        """Test DAG with parallel branches"""
        dag = DAG("parallel_processing")
        
        @dag.node()
        def split(data: list) -> dict:
            return {"upper": data[:len(data)//2], "lower": data[len(data)//2:]}
        
        @dag.node()
        def process_upper(upper: list) -> int:
            return sum(upper)
        
        @dag.node()
        def process_lower(lower: list) -> int:
            return sum(lower)
        
        @dag.node()
        def merge(upper_sum: int, lower_sum: int) -> int:
            return upper_sum + lower_sum
        
        split >> process_upper >> merge
        split >> process_lower >> merge
        
        runner = Runner(dag)
        metrics = runner.run(inputs={"data": [1, 2, 3, 4, 5, 6]}, mode="parallel")
        
        assert dag["merge"] == 21
        assert metrics.nodes_executed == 4
    
    def test_conditional_workflow(self):
        """Test conditional branching in DAG"""
        dag = DAG("conditional_flow")
        
        @dag.condition(name="check_value")
        def check(value: int) -> ConditionalReturn:
            return ConditionalReturn(
                condition=value > 100,
                true_branch="large_handler",
                false_branch="small_handler"
            )
        
        @dag.node(name="large_handler")
        def handle_large(value: int) -> str:
            return f"Large value: {value}"
        
        @dag.node(name="small_handler")
        def handle_small(value: int) -> str:
            return f"Small value: {value}"
        
        check >> handle_large
        check >> handle_small
        
        # Test with large value
        runner = Runner(dag)
        runner.run(inputs={"value": 150})
        assert dag["large_handler"] == "Large value: 150"
        assert dag.get_result("small_handler") is None
        
        # Test with small value
        dag.context = Context()  # Reset context
        runner.run(inputs={"value": 50})
        assert dag["small_handler"] == "Small value: 50"
    
    def test_dag_validation(self):
        """Test DAG validation"""
        dag = DAG("invalid_dag")
        
        @dag.node()
        def node1(x: int) -> int:
            return x
        
        @dag.node()
        def node2(y: int) -> int:
            return y
        
        # Create cycle
        node1 >> node2 >> node1
        
        with pytest.raises(WorkflowValidationError):
            validate_workflow(dag)
    
    @pytest.mark.asyncio
    async def test_async_dag_execution(self):
        """Test DAG with async nodes"""
        dag = DAG("async_pipeline")
        
        @dag.node()
        async def fetch_data(url: str) -> dict:
            await asyncio.sleep(0.1)
            return {"data": "fetched"}
        
        @dag.node()
        async def process_data(data: dict) -> dict:
            await asyncio.sleep(0.1)
            return {"processed": True, **data}
        
        fetch_data >> process_data
        
        runner = Runner(dag)
        metrics = await runner.run_async(inputs={"url": "http://example.com"})
        
        assert dag["process_data"] == {"processed": True, "data": "fetched"}
        assert metrics.total_time >= 0.2  # Sequential execution

class TestFSMWorkflow:
    """Test FSM workflow functionality"""
    
    def test_simple_fsm(self):
        """Test basic FSM with state transitions"""
        fsm = FSM("traffic_light")
        
        @fsm.state(is_initial=True)
        def red(context: Context) -> FSMReturn:
            context.metadata["light"] = "red"
            return FSMReturn(next_state="green", value="red")
        
        @fsm.state()
        def green(context: Context) -> FSMReturn:
            context.metadata["light"] = "green"
            return FSMReturn(next_state="yellow", value="green")
        
        @fsm.state()
        def yellow(context: Context) -> FSMReturn:
            context.metadata["light"] = "yellow"
            context.metadata["cycle_count"] = context.metadata.get("cycle_count", 0) + 1
            
            if context.metadata["cycle_count"] >= 3:
                return FSMReturn(stop_condition=True, value="yellow")
            
            return FSMReturn(next_state="red", value="yellow")
        
        runner = Runner(fsm)
        metrics = runner.run()
        
        assert fsm.state_history == ["green", "yellow", "red", "green", "yellow", "red", "green", "yellow", "END"]
        assert fsm.cycle_count == 9
        assert fsm["yellow"] == "yellow"  # Latest result
        assert fsm["yellow.8"] == "yellow"  # Specific cycle
    
    def test_fsm_with_conditions(self):
        """Test FSM with conditional state transitions"""
        fsm = FSM("vending_machine")
        
        @fsm.state(is_initial=True)
        def idle(coins: int, context: Context) -> FSMReturn:
            context.metadata["total_coins"] = context.metadata.get("total_coins", 0) + coins
            
            if context.metadata["total_coins"] >= 100:
                return FSMReturn(next_state="dispense", value="ready")
            return FSMReturn(next_state="waiting", value="need_more_coins")
        
        @fsm.state()
        def waiting(coins: int, context: Context) -> FSMReturn:
            context.metadata["total_coins"] += coins
            
            if context.metadata["total_coins"] >= 100:
                return FSMReturn(next_state="dispense", value="ready")
            return FSMReturn(next_state="waiting", value="need_more_coins")
        
        @fsm.state(is_final=True)
        def dispense(context: Context) -> FSMReturn:
            context.metadata["dispensed"] = True
            return FSMReturn(stop_condition=True, value="product_dispensed")
        
        runner = Runner(fsm)
        runner.run(inputs={"coins": 25})
        
        assert fsm["waiting.3"] == "need_more_coins"
        assert fsm["dispense"] == "product_dispensed"
        assert fsm.context.metadata["total_coins"] >= 100

class TestComplexWorkflows:
    """Test complex workflow scenarios"""
    
    def test_nested_workflows(self):
        """Test DAG containing another DAG as a node"""
        # Inner DAG
        inner_dag = DAG("data_processing")
        
        @inner_dag.node()
        def clean_data(data: list) -> list:
            return [x for x in data if x > 0]
        
        @inner_dag.node()
        def normalize_data(data: list) -> list:
            max_val = max(data) if data else 1
            return [x / max_val for x in data]
        
        clean_data >> normalize_data
        
        # Outer DAG
        outer_dag = DAG("full_pipeline")
        
        @outer_dag.node()
        def load_data(filename: str) -> list:
            return [1, -2, 3, -4, 5, 6]  # Mock data
        
        # Create a node from the inner DAG
        process_node = Node(
            func=lambda data: Runner(inner_dag).run(inputs={"data": data}).workflow["normalize_data"],
            name="process_data"
        )
        outer_dag.add_node(process_node)
        
        @outer_dag.node()
        def save_results(data: list) -> bool:
            return len(data) > 0
        
        load_data >> process_node >> save_results
        
        runner = Runner(outer_dag)
        metrics = runner.run(inputs={"filename": "data.csv"})
        
        assert outer_dag["save_results"] is True
    
    def test_error_handling_and_recovery(self):
        """Test error handling in workflows"""
        dag = DAG("error_handling")
        
        @dag.node()
        def risky_operation(x: int) -> int:
            if x < 0:
                raise ValueError("Negative input not allowed")
            return x * 2
        
        @dag.node()
        def safe_operation(x: int, context: Context) -> int:
            try:
                # Access previous result
                risky_result = context.get_result("risky_operation")
                return risky_result + 10
            except:
                return x + 10  # Fallback
        
        risky_operation >> safe_operation
        
        runner = Runner(dag)
        
        # Test with error
        with pytest.raises(WorkflowExecutionError):
            runner.run(inputs={"x": -5})
        
        assert len(runner.metrics.errors) > 0
        assert "risky_operation" in runner.metrics.errors[0]
    
    def test_long_running_workflow_with_checkpoints(self):
        """Test workflow with checkpoints for pause/resume"""
        dag = DAG("long_running")
        
        checkpoint_data = {}
        
        @dag.node()
        def step1(data: list) -> list:
            checkpoint_data["step1"] = True
            return [x * 2 for x in data]
        
        @dag.node()
        def checkpoint(data: list, context: Context) -> list:
            # Save state
            checkpoint_data["context"] = context.results.copy()
            checkpoint_data["checkpoint"] = True
            return data
        
        @dag.node()
        def step2(data: list) -> list:
            if not checkpoint_data.get("checkpoint"):
                raise RuntimeError("Must pass through checkpoint")
            return [x + 1 for x in data]
        
        step1 >> checkpoint >> step2
        
        runner = Runner(dag)
        runner.run(inputs={"data": [1, 2, 3]})
        
        assert checkpoint_data["checkpoint"] is True
        assert dag["step2"] == [3, 5, 7]

class TestVisualization:
    """Test workflow visualization"""
    
    def test_mermaid_generation(self):
        """Test Mermaid diagram generation"""
        dag = DAG("viz_test")
        
        @dag.node()
        def input_node(x: int) -> int:
            return x
        
        @dag.node()
        def process_node(x: int) -> int:
            return x * 2
        
        @dag.node()
        def output_node(x: int) -> int:
            return x
        
        input_node >> process_node >> output_node
        
        viz = Visualizer(dag)
        mermaid = viz.to_mermaid()
        
        assert "graph TD" in mermaid
        assert "input_node" in mermaid
        assert "process_node" in mermaid
        assert "output_node" in mermaid
        assert "input_node --> process_node" in mermaid
        assert "process_node --> output_node" in mermaid
    
    def test_visualization_with_results(self):
        """Test visualization including execution results"""
        dag = DAG("viz_results")
        
        @dag.node()
        def compute(x: int) -> int:
            return x * 10
        
        runner = Runner(dag)
        runner.run(inputs={"x": 5})
        
        viz = Visualizer(dag)
        mermaid = viz.to_mermaid(include_results=True)
        
        assert "[50]" in mermaid  # Result should be included

class TestSerialization:
    """Test workflow serialization"""
    
    def test_yaml_serialization(self):
        """Test YAML serialization and deserialization"""
        dag = DAG("serialization_test")
        
        @dag.node()
        def func1(x: int) -> int:
            return x * 2
        
        @dag.node()
        def func2(x: int) -> int:
            return x + 1
        
        func1 >> func2
        
        # Serialize
        serializer = WorkflowSerializer()
        yaml_str = serializer.to_yaml(dag)
        
        assert "name: serialization_test" in yaml_str
        assert "type: DAG" in yaml_str
        assert "func1" in yaml_str
        assert "func2" in yaml_str
        
        # Deserialize
        function_registry = {
            "func1": func1.func,
            "func2": func2.func
        }
        
        restored_dag = serializer.from_yaml(yaml_str, function_registry)
        assert restored_dag.name == "serialization_test"
        assert len(restored_dag.nodes) == 2
```

## Error Handling and Recovery Strategies

### Error Types and Handling

```python
from enum import Enum
from typing import Optional, Callable, Any
import traceback

class ErrorSeverity(Enum):
    """Error severity levels"""
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class WorkflowError:
    """Detailed error information"""
    node_name: str
    error_type: str
    message: str
    severity: ErrorSeverity
    timestamp: float
    traceback: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3

class ErrorHandler:
    """Centralized error handling for workflows"""
    
    def __init__(self):
        self.error_handlers: Dict[str, Callable] = {}
        self.global_handler: Optional[Callable] = None
        self.errors: List[WorkflowError] = []
    
    def register_handler(self, error_type: str, handler: Callable) -> None:
        """Register error handler for specific error type"""
        self.error_handlers[error_type] = handler
    
    def set_global_handler(self, handler: Callable) -> None:
        """Set global error handler"""
        self.global_handler = handler
    
    async def handle_error(self, error: Exception, node: Node, context: Context) -> Any:
        """Handle error with appropriate strategy"""
        import time
        
        workflow_error = WorkflowError(
            node_name=node.name,
            error_type=type(error).__name__,
            message=str(error),
            severity=self._determine_severity(error),
            timestamp=time.time(),
            traceback=traceback.format_exc()
        )
        
        self.errors.append(workflow_error)
        
        # Try specific handler first
        handler = self.error_handlers.get(workflow_error.error_type)
        if handler:
            return await handler(error, node, context)
        
        # Fall back to global handler
        if self.global_handler:
            return await self.global_handler(error, node, context)
        
        # Default behavior - re-raise
        raise error
    
    def _determine_severity(self, error: Exception) -> ErrorSeverity:
        """Determine error severity based on error type"""
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            return ErrorSeverity.CRITICAL
        elif isinstance(error, (ValueError, TypeError, KeyError)):
            return ErrorSeverity.ERROR
        else:
            return ErrorSeverity.WARNING

### Recovery Strategies

class RecoveryStrategy(ABC):
    """Base class for recovery strategies"""
    
    @abstractmethod
    async def recover(self, error: WorkflowError, node: Node, context: Context) -> Any:
        """Attempt to recover from error"""
        pass

class RetryStrategy(RecoveryStrategy):
    """Retry failed node execution"""
    
    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
    
    async def recover(self, error: WorkflowError, node: Node, context: Context) -> Any:
        """Retry with exponential backoff"""
        if error.retry_count >= self.max_retries:
            raise WorkflowExecutionError(f"Max retries ({self.max_retries}) exceeded for node '{node.name}'")
        
        wait_time = self.backoff_factor ** error.retry_count
        await asyncio.sleep(wait_time)
        
        error.retry_count += 1
        return await node.execute(context)

class FallbackStrategy(RecoveryStrategy):
    """Use fallback value or function"""
    
    def __init__(self, fallback_value: Any = None, fallback_func: Optional[Callable] = None):
        self.fallback_value = fallback_value
        self.fallback_func = fallback_func
    
    async def recover(self, error: WorkflowError, node: Node, context: Context) -> Any:
        """Return fallback value or execute fallback function"""
        if self.fallback_func:
            return await self.fallback_func(error, node, context)
        return self.fallback_value

class CircuitBreakerStrategy(RecoveryStrategy):
    """Circuit breaker pattern for failing nodes"""
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count: Dict[str, int] = {}
        self.last_failure_time: Dict[str, float] = {}
    
    async def recover(self, error: WorkflowError, node: Node, context: Context) -> Any:
        """Implement circuit breaker logic"""
        import time
        
        current_time = time.time()
        
        # Check if circuit should be open
        if node.name in self.last_failure_time:
            time_since_failure = current_time - self.last_failure_time[node.name]
            if time_since_failure < self.timeout:
                raise WorkflowExecutionError(f"Circuit breaker open for node '{node.name}'")
        
        # Increment failure count
        self.failure_count[node.name] = self.failure_count.get(node.name, 0) + 1
        
        if self.failure_count[node.name] >= self.failure_threshold:
            self.last_failure_time[node.name] = current_time
            raise WorkflowExecutionError(f"Circuit breaker triggered for node '{node.name}'")
        
        # Try to execute
        try:
            result = await node.execute(context)
            # Reset on success
            self.failure_count[node.name] = 0
            return result
        except Exception:
            raise

### Enhanced Runner with Error Handling

class EnhancedRunner(Runner):
    """Runner with advanced error handling capabilities"""
    
    def __init__(self, workflow: WorkflowBase, error_handler: Optional[ErrorHandler] = None):
        super().__init__(workflow)
        self.error_handler = error_handler or ErrorHandler()
        self.recovery_strategies: Dict[str, RecoveryStrategy] = {}
    
    def set_recovery_strategy(self, node_name: str, strategy: RecoveryStrategy) -> None:
        """Set recovery strategy for specific node"""
        self.recovery_strategies[node_name] = strategy
    
    async def _execute_node_with_recovery(self, node: Node, cycle: int = 0) -> Any:
        """Execute node with error handling and recovery"""
        try:
            return await super()._execute_node(node, cycle)
        except Exception as e:
            # Try recovery strategy
            if node.name in self.recovery_strategies:
                strategy = self.recovery_strategies[node.name]
                workflow_error = WorkflowError(
                    node_name=node.name,
                    error_type=type(e).__name__,
                    message=str(e),
                    severity=ErrorSeverity.ERROR,
                    timestamp=time.time()
                )
                return await strategy.recover(workflow_error, node, self.workflow.context)
            
            # Use error handler
            return await self.error_handler.handle_error(e, node, self.workflow.context)

# Usage example
async def main():
    dag = DAG("error_prone_workflow")
    
    @dag.node()
    def unreliable_service(data: dict) -> dict:
        import random
        if random.random() < 0.5:
            raise ConnectionError("Service unavailable")
        return {"status": "success", **data}
    
    @dag.node()
    def process_result(data: dict) -> str:
        return data.get("status", "failed")
    
    unreliable_service >> process_result
    
    # Configure error handling
    runner = EnhancedRunner(dag)
    runner.set_recovery_strategy("unreliable_service", RetryStrategy(max_retries=5))
    
    # Add fallback for process_result
    runner.set_recovery_strategy("process_result", FallbackStrategy(fallback_value="fallback_result"))
    
    metrics = await runner.run_async(inputs={"data": {"id": 123}})
```

## Performance Requirements and Benchmarking

### Performance Targets

```python
@dataclass
class PerformanceRequirements:
    """Performance requirements for the workflow engine"""
    # Node execution overhead
    node_overhead_ms: float = 1.0  # < 1ms per node
    
    # Parallel scaling efficiency
    parallel_efficiency: float = 0.85  # 85% efficiency up to 1000 nodes
    
    # Memory usage
    memory_per_node_mb: float = 10.0  # ~10MB per node
    memory_growth_factor: float = 1.2  # O(n) with 20% overhead
    
    # Visualization performance
    viz_generation_ms_per_node: float = 0.1  # < 100ms for 1000 nodes
    
    # Serialization performance
    serialization_ms_per_node: float = 0.5
    deserialization_ms_per_node: float = 0.5
    
    # Validation performance
    validation_ms_per_edge: float = 0.01

### Benchmarking Framework

```python
import time
import psutil
import statistics
from typing import List, Callable, Tuple
import matplotlib.pyplot as plt

class BenchmarkRunner:
    """Comprehensive benchmarking for workflow engine"""
    
    def __init__(self):
        self.results: List[BenchmarkResult] = []
    
    async def benchmark_node_overhead(self, sizes: List[int] = None) -> BenchmarkResult:
        """Measure node execution overhead"""
        sizes = sizes or [10, 100, 1000, 10000]
        results = []
        
        for size in sizes:
            # Create simple linear DAG
            dag = self._create_linear_dag(size)
            
            # Measure execution time
            start_time = time.perf_counter()
            runner = Runner(dag)
            await runner.run_async(inputs={"x": 1})
            end_time = time.perf_counter()
            
            total_time = end_time - start_time
            overhead_per_node = (total_time / size) * 1000  # ms
            
            results.append({
                "nodes": size,
                "total_time_ms": total_time * 1000,
                "overhead_per_node_ms": overhead_per_node
            })
        
        return BenchmarkResult(
            name="node_overhead",
            results=results,
            metric="overhead_per_node_ms"
        )
    
    async def benchmark_parallel_scaling(self, dag_shapes: List[str] = None) -> BenchmarkResult:
        """Measure parallel execution scaling"""
        dag_shapes = dag_shapes or ["linear", "fan_out", "diamond", "complex"]
        results = []
        
        for shape in dag_shapes:
            for width in [1, 2, 4, 8, 16, 32]:
                dag = self._create_shaped_dag(shape, width)
                
                # Sequential execution
                seq_runner = Runner(dag)
                seq_start = time.perf_counter()
                await seq_runner.run_async(inputs={}, mode="sequential")
                seq_time = time.perf_counter() - seq_start
                
                # Parallel execution
                par_runner = Runner(dag)
                par_start = time.perf_counter()
                await par_runner.run_async(inputs={}, mode="parallel")
                par_time = time.perf_counter() - par_start
                
                speedup = seq_time / par_time if par_time > 0 else 1
                efficiency = speedup / width if width > 0 else 1
                
                results.append({
                    "shape": shape,
                    "width": width,
                    "sequential_time_ms": seq_time * 1000,
                    "parallel_time_ms": par_time * 1000,
                    "speedup": speedup,
                    "efficiency": efficiency
                })
        
        return BenchmarkResult(
            name="parallel_scaling",
            results=results,
            metric="efficiency"
        )
    
    def benchmark_memory_usage(self, sizes: List[int] = None) -> BenchmarkResult:
        """Measure memory usage scaling"""
        sizes = sizes or [100, 1000, 10000, 100000]
        results = []
        process = psutil.Process()
        
        for size in sizes:
            # Get baseline memory
            gc.collect()
            baseline_memory = process.memory_info().rss / 1024 / 1024  # MB
            
            # Create and execute DAG
            dag = self._create_complex_dag(size)
            runner = Runner(dag)
            runner.run(inputs={"data": list(range(100))})
            
            # Measure memory after execution
            peak_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_used = peak_memory - baseline_memory
            memory_per_node = memory_used / size
            
            results.append({
                "nodes": size,
                "memory_used_mb": memory_used,
                "memory_per_node_mb": memory_per_node
            })
            
            # Clean up
            del dag
            del runner
            gc.collect()
        
        return BenchmarkResult(
            name="memory_usage",
            results=results,
            metric="memory_per_node_mb"
        )
    
    def benchmark_visualization_performance(self, sizes: List[int] = None) -> BenchmarkResult:
        """Measure visualization generation performance"""
        sizes = sizes or [10, 100, 1000, 5000]
        results = []
        
        for size in sizes:
            dag = self._create_complex_dag(size)
            viz = Visualizer(dag)
            
            # Benchmark Mermaid generation
            mermaid_times = []
            for _ in range(10):
                start = time.perf_counter()
                mermaid_diagram = viz.to_mermaid()
                end = time.perf_counter()
                mermaid_times.append((end - start) * 1000)
            
            # Benchmark Graphviz generation
            graphviz_times = []
            for _ in range(10):
                start = time.perf_counter()
                graphviz_diagram = viz.to_graphviz()
                end = time.perf_counter()
                graphviz_times.append((end - start) * 1000)
            
            results.append({
                "nodes": size,
                "mermaid_avg_ms": statistics.mean(mermaid_times),
                "mermaid_std_ms": statistics.stdev(mermaid_times),
                "graphviz_avg_ms": statistics.mean(graphviz_times),
                "graphviz_std_ms": statistics.stdev(graphviz_times)
            })
        
        return BenchmarkResult(
            name="visualization_performance",
            results=results,
            metric="mermaid_avg_ms"
        )
    
    async def benchmark_error_handling_overhead(self) -> BenchmarkResult:
        """Measure error handling overhead"""
        results = []
        
        # Create DAG with potential errors
        dag = DAG("error_benchmark")
        
        @dag.node()
        def may_fail(x: int, fail_rate: float) -> int:
            import random
            if random.random() < fail_rate:
                raise ValueError("Simulated failure")
            return x * 2
        
        @dag.node()
        def safe_node(x: int) -> int:
            return x + 1
        
        may_fail >> safe_node
        
        for fail_rate in [0.0, 0.1, 0.3, 0.5, 0.7, 0.9]:
            # Without error handling
            basic_runner = Runner(dag)
            basic_times = []
            
            for _ in range(100):
                try:
                    start = time.perf_counter()
                    await basic_runner.run_async(inputs={"x": 1, "fail_rate": fail_rate})
                    end = time.perf_counter()
                    basic_times.append((end - start) * 1000)
                except:
                    pass
            
            # With error handling
            enhanced_runner = EnhancedRunner(dag)
            enhanced_runner.set_recovery_strategy("may_fail", RetryStrategy(max_retries=3))
            enhanced_times = []
            
            for _ in range(100):
                start = time.perf_counter()
                await enhanced_runner.run_async(inputs={"x": 1, "fail_rate": fail_rate})
                end = time.perf_counter()
                enhanced_times.append((end - start) * 1000)
            
            if basic_times:
                overhead = (statistics.mean(enhanced_times) - statistics.mean(basic_times)) / statistics.mean(basic_times) * 100
            else:
                overhead = 100.0
            
            results.append({
                "fail_rate": fail_rate,
                "basic_avg_ms": statistics.mean(basic_times) if basic_times else None,
                "enhanced_avg_ms": statistics.mean(enhanced_times),
                "overhead_percent": overhead
            })
        
        return BenchmarkResult(
            name="error_handling_overhead",
            results=results,
            metric="overhead_percent"
        )
    
    def _create_linear_dag(self, size: int) -> DAG:
        """Create a linear DAG of given size"""
        dag = DAG(f"linear_{size}")
        
        prev_node = None
        for i in range(size):
            @dag.node(name=f"node_{i}")
            def process(x: int = 0) -> int:
                return x + 1
            
            if prev_node:
                prev_node >> dag.nodes[f"node_{i}"]
            prev_node = dag.nodes[f"node_{i}"]
        
        return dag
    
    def _create_shaped_dag(self, shape: str, width: int) -> DAG:
        """Create DAG with specific shape"""
        dag = DAG(f"{shape}_{width}")
        
        if shape == "fan_out":
            # One node fans out to many
            @dag.node(name="source")
            def source() -> int:
                return 1
            
            for i in range(width):
                @dag.node(name=f"worker_{i}")
                def worker(x: int) -> int:
                    import time
                    time.sleep(0.01)  # Simulate work
                    return x * 2
                
                dag.nodes["source"] >> dag.nodes[f"worker_{i}"]
        
        elif shape == "diamond":
            # Diamond pattern
            @dag.node(name="start")
            def start() -> int:
                return 1
            
            # Middle layer
            for i in range(width):
                @dag.node(name=f"middle_{i}")
                def middle(x: int) -> int:
                    import time
                    time.sleep(0.01)
                    return x * 2
                
                dag.nodes["start"] >> dag.nodes[f"middle_{i}"]
            
            @dag.node(name="end")
            def end(**kwargs) -> int:
                return sum(kwargs.values())
            
            for i in range(width):
                dag.nodes[f"middle_{i}"] >> dag.nodes["end"]
        
        return dag
    
    def generate_report(self, output_file: str = "benchmark_report.md") -> None:
        """Generate comprehensive benchmark report"""
        with open(output_file, "w") as f:
            f.write("# Fast-DAG Performance Benchmark Report\n\n")
            f.write(f"Generated at: {datetime.now().isoformat()}\n\n")
            
            for result in self.results:
                f.write(f"## {result.name.replace('_', ' ').title()}\n\n")
                
                # Create table
                if result.results:
                    headers = list(result.results[0].keys())
                    f.write("| " + " | ".join(headers) + " |\n")
                    f.write("| " + " | ".join(["---"] * len(headers)) + " |\n")
                    
                    for row in result.results:
                        values = [f"{v:.2f}" if isinstance(v, float) else str(v) for v in row.values()]
                        f.write("| " + " | ".join(values) + " |\n")
                
                f.write("\n")
                
                # Add analysis
                if result.metric in row:
                    metric_values = [r[result.metric] for r in result.results if r[result.metric] is not None]
                    if metric_values:
                        f.write(f"**{result.metric}**: ")
                        f.write(f"avg={statistics.mean(metric_values):.2f}, ")
                        f.write(f"min={min(metric_values):.2f}, ")
                        f.write(f"max={max(metric_values):.2f}\n\n")

@dataclass
class BenchmarkResult:
    """Results from a benchmark run"""
    name: str
    results: List[Dict[str, Any]]
    metric: str  # Primary metric to track

### Performance Optimization Strategies

class PerformanceOptimizer:
    """Optimize workflow performance"""
    
    @staticmethod
    def optimize_node_execution(node: Node) -> Node:
        """Optimize individual node execution"""
        # Add caching decorator
        if node.func.__name__ not in ["__lambda__", "<lambda>"]:
            node.func = lru_cache(maxsize=128)(node.func)
        
        # Add execution time tracking
        original_func = node.func
        
        async def timed_func(*args, **kwargs):
            start = time.perf_counter()
            result = await original_func(*args, **kwargs) if asyncio.iscoroutinefunction(original_func) else original_func(*args, **kwargs)
            execution_time = time.perf_counter() - start
            
            # Store timing information
            if 'context' in kwargs:
                kwargs['context'].metadata[f"{node.name}_execution_time"] = execution_time
            
            return result
        
        node.func = timed_func
        return node
    
    @staticmethod
    def optimize_dag_structure(dag: DAG) -> DAG:
        """Optimize DAG structure for better performance"""
        # Remove redundant edges
        dag = PerformanceOptimizer._remove_redundant_edges(dag)
        
        # Identify and mark parallelizable regions
        dag = PerformanceOptimizer._mark_parallel_regions(dag)
        
        return dag
    
    @staticmethod
    def _remove_redundant_edges(dag: DAG) -> DAG:
        """Remove redundant edges (transitive reduction)"""
        # Implementation of transitive reduction algorithm
        for node in dag.nodes.values():
            # Find all nodes reachable from this node
            reachable = dag.get_downstream(node.name)
            
            # Remove direct connections to nodes that are reachable indirectly
            for output_name, connections in list(node.output_connections.items()):
                for conn_node in list(connections):
                    # Check if conn_node is reachable through other paths
                    other_paths_exist = False
                    for intermediate in reachable:
                        if intermediate != conn_node.name:
                            intermediate_downstream = dag.get_downstream(intermediate)
                            if conn_node.name in intermediate_downstream:
                                other_paths_exist = True
                                break
                    
                    if other_paths_exist:
                        connections.remove(conn_node)
        
        return dag

### Usage Example

```python
async def run_performance_benchmarks():
    """Run comprehensive performance benchmarks"""
    benchmarker = BenchmarkRunner()
    
    print("Running node overhead benchmark...")
    node_overhead = await benchmarker.benchmark_node_overhead()
    
    print("Running parallel scaling benchmark...")
    parallel_scaling = await benchmarker.benchmark_parallel_scaling()
    
    print("Running memory usage benchmark...")
    memory_usage = benchmarker.benchmark_memory_usage()
    
    print("Running visualization performance benchmark...")
    viz_perf = benchmarker.benchmark_visualization_performance()
    
    print("Running error handling overhead benchmark...")
    error_overhead = await benchmarker.benchmark_error_handling_overhead()
    
    # Generate report
    benchmarker.generate_report("performance_report.md")
    
    # Check against requirements
    requirements = PerformanceRequirements()
    
    # Validate performance meets requirements
    node_overhead_results = [r["overhead_per_node_ms"] for r in node_overhead.results]
    if any(overhead > requirements.node_overhead_ms for overhead in node_overhead_results):
        print("WARNING: Node overhead exceeds requirements!")
    
    efficiency_results = [r["efficiency"] for r in parallel_scaling.results if r["width"] <= 32]
    if any(eff < requirements.parallel_efficiency for eff in efficiency_results):
        print("WARNING: Parallel efficiency below target!")
    
    print("Benchmark complete!")

# Run benchmarks
if __name__ == "__main__":
    asyncio.run(run_performance_benchmarks())
```

## Debugging and Observability

### Debugging Support

```python
import logging
from enum import Enum
from typing import Optional, Any, Dict, List
import json
from datetime import datetime

class DebugLevel(Enum):
    """Debug verbosity levels"""
    OFF = 0
    ERROR = 1
    WARNING = 2
    INFO = 3
    DEBUG = 4
    TRACE = 5

class WorkflowDebugger:
    """Comprehensive debugging support for workflows"""
    
    def __init__(self, level: DebugLevel = DebugLevel.INFO):
        self.level = level
        self.breakpoints: Set[str] = set()
        self.watch_expressions: Dict[str, str] = {}
        self.execution_trace: List[TraceEvent] = []
        self.step_mode: bool = False
        self.paused: bool = False
        
        # Setup logging
        self.logger = logging.getLogger("fast_dag.debugger")
        self._setup_logging()
    
    def set_breakpoint(self, node_name: str, condition: Optional[str] = None) -> None:
        """Set breakpoint at node"""
        self.breakpoints.add(node_name)
        if condition:
            self.watch_expressions[node_name] = condition
    
    def remove_breakpoint(self, node_name: str) -> None:
        """Remove breakpoint"""
        self.breakpoints.discard(node_name)
        self.watch_expressions.pop(node_name, None)
    
    async def on_node_start(self, node: Node, context: Context, inputs: Dict[str, Any]) -> None:
        """Called when node execution starts"""
        event = TraceEvent(
            timestamp=datetime.now(),
            event_type="node_start",
            node_name=node.name,
            data={"inputs": self._serialize_data(inputs)}
        )
        self.execution_trace.append(event)
        
        if self.level >= DebugLevel.DEBUG:
            self.logger.debug(f"Starting node '{node.name}' with inputs: {inputs}")
        
        # Check breakpoint
        if node.name in self.breakpoints:
            should_break = True
            
            # Evaluate condition if present
            if node.name in self.watch_expressions:
                condition = self.watch_expressions[node.name]
                should_break = self._evaluate_condition(condition, context, inputs)
            
            if should_break:
                await self._handle_breakpoint(node, context, inputs)
    
    async def on_node_complete(self, node: Node, context: Context, result: Any, duration: float) -> None:
        """Called when node execution completes"""
        event = TraceEvent(
            timestamp=datetime.now(),
            event_type="node_complete",
            node_name=node.name,
            data={
                "result": self._serialize_data(result),
                "duration_ms": duration * 1000
            }
        )
        self.execution_trace.append(event)
        
        if self.level >= DebugLevel.INFO:
            self.logger.info(f"Node '{node.name}' completed in {duration*1000:.2f}ms")
        
        if self.step_mode:
            await self._handle_step()
    
    async def on_node_error(self, node: Node, context: Context, error: Exception) -> None:
        """Called when node execution fails"""
        event = TraceEvent(
            timestamp=datetime.now(),
            event_type="node_error",
            node_name=node.name,
            data={
                "error_type": type(error).__name__,
                "error_message": str(error),
                "traceback": traceback.format_exc()
            }
        )
        self.execution_trace.append(event)
        
        if self.level >= DebugLevel.ERROR:
            self.logger.error(f"Node '{node.name}' failed: {error}")
        
        # Always break on error in debug mode
        if self.level >= DebugLevel.DEBUG:
            await self._handle_breakpoint(node, context, {"error": error})
    
    async def _handle_breakpoint(self, node: Node, context: Context, data: Dict[str, Any]) -> None:
        """Handle breakpoint hit"""
        self.paused = True
        print(f"\n🔴 Breakpoint hit at node '{node.name}'")
        print(f"Context: {context.results}")
        print(f"Data: {data}")
        
        # Interactive debugging session
        while self.paused:
            command = input("\n(debug) ").strip().lower()
            
            if command == "continue" or command == "c":
                self.paused = False
            elif command == "step" or command == "s":
                self.step_mode = True
                self.paused = False
            elif command == "inspect" or command == "i":
                self._inspect_state(context)
            elif command == "trace" or command == "t":
                self._show_trace()
            elif command == "help" or command == "h":
                self._show_help()
            else:
                print(f"Unknown command: {command}")
    
    def _serialize_data(self, data: Any) -> Any:
        """Serialize data for tracing"""
        try:
            # Attempt to JSON serialize
            json.dumps(data)
            return data
        except:
            # Fall back to string representation
            return str(data)
    
    def _evaluate_condition(self, condition: str, context: Context, inputs: Dict[str, Any]) -> bool:
        """Evaluate watch expression"""
        try:
            # Create evaluation context
            eval_context = {
                "context": context,
                "inputs": inputs,
                "results": context.results
            }
            return eval(condition, {}, eval_context)
        except Exception as e:
            self.logger.warning(f"Failed to evaluate condition '{condition}': {e}")
            return False
    
    def export_trace(self, filename: str) -> None:
        """Export execution trace to file"""
        with open(filename, "w") as f:
            trace_data = [event.to_dict() for event in self.execution_trace]
            json.dump(trace_data, f, indent=2, default=str)
    
    def generate_flamegraph(self) -> str:
        """Generate flamegraph data from execution trace"""
        # Implementation would generate flamegraph-compatible format
        pass

@dataclass
class TraceEvent:
    """Single trace event"""
    timestamp: datetime
    event_type: str
    node_name: str
    data: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type,
            "node_name": self.node_name,
            "data": self.data
        }

### Observability Integration

```python
from opentelemetry import trace, metrics
from opentelemetry.trace import Status, StatusCode
from prometheus_client import Counter, Histogram, Gauge, Summary
import structlog

class ObservabilityManager:
    """Manage observability for workflows"""
    
    def __init__(self, service_name: str = "fast_dag"):
        self.service_name = service_name
        
        # OpenTelemetry setup
        self.tracer = trace.get_tracer(__name__)
        self.meter = metrics.get_meter(__name__)
        
        # Prometheus metrics
        self.node_execution_counter = Counter(
            'workflow_node_executions_total',
            'Total number of node executions',
            ['workflow_name', 'node_name', 'status']
        )
        
        self.node_execution_duration = Histogram(
            'workflow_node_duration_seconds',
            'Node execution duration in seconds',
            ['workflow_name', 'node_name']
        )
        
        self.workflow_execution_duration = Summary(
            'workflow_execution_duration_seconds',
            'Workflow execution duration in seconds',
            ['workflow_name', 'status']
        )
        
        self.active_workflows = Gauge(
            'workflow_active_count',
            'Number of currently active workflows',
            ['workflow_name']
        )
        
        # Structured logging
        self.logger = structlog.get_logger()
    
    def create_workflow_span(self, workflow_name: str) -> Any:
        """Create span for workflow execution"""
        return self.tracer.start_as_current_span(
            f"workflow.{workflow_name}",
            attributes={
                "workflow.name": workflow_name,
                "service.name": self.service_name
            }
        )
    
    def create_node_span(self, node_name: str, workflow_name: str) -> Any:
        """Create span for node execution"""
        return self.tracer.start_as_current_span(
            f"node.{node_name}",
            attributes={
                "node.name": node_name,
                "workflow.name": workflow_name
            }
        )
    
    def record_node_execution(
        self, 
        workflow_name: str, 
        node_name: str, 
        duration: float, 
        status: str
    ) -> None:
        """Record node execution metrics"""
        # Prometheus metrics
        self.node_execution_counter.labels(
            workflow_name=workflow_name,
            node_name=node_name,
            status=status
        ).inc()
        
        if status == "success":
            self.node_execution_duration.labels(
                workflow_name=workflow_name,
                node_name=node_name
            ).observe(duration)
        
        # Structured logging
        self.logger.info(
            "node_executed",
            workflow_name=workflow_name,
            node_name=node_name,
            duration_ms=duration * 1000,
            status=status
        )
    
    def record_workflow_execution(
        self, 
        workflow_name: str, 
        duration: float, 
        status: str, 
        nodes_executed: int
    ) -> None:
        """Record workflow execution metrics"""
        self.workflow_execution_duration.labels(
            workflow_name=workflow_name,
            status=status
        ).observe(duration)
        
        self.logger.info(
            "workflow_executed",
            workflow_name=workflow_name,
            duration_ms=duration * 1000,
            status=status,
            nodes_executed=nodes_executed
        )

### Instrumented Runner

class InstrumentedRunner(Runner):
    """Runner with full observability instrumentation"""
    
    def __init__(
        self, 
        workflow: WorkflowBase,
        debugger: Optional[WorkflowDebugger] = None,
        observability: Optional[ObservabilityManager] = None
    ):
        super().__init__(workflow)
        self.debugger = debugger
        self.observability = observability or ObservabilityManager()
    
    async def run_async(
        self,
        inputs: Optional[Dict[str, Any]] = None,
        mode: str = "sequential"
    ) -> ExecutionMetrics:
        """Run workflow with full instrumentation"""
        workflow_name = self.workflow.name
        
        # Start workflow span
        with self.observability.create_workflow_span(workflow_name) as span:
            # Track active workflows
            self.observability.active_workflows.labels(workflow_name=workflow_name).inc()
            
            try:
                # Run with debugging if enabled
                if self.debugger:
                    self.debugger.execution_trace.clear()
                
                # Execute workflow
                metrics = await super().run_async(inputs, mode)
                
                # Record success
                span.set_status(Status(StatusCode.OK))
                self.observability.record_workflow_execution(
                    workflow_name=workflow_name,
                    duration=metrics.total_time,
                    status="success",
                    nodes_executed=metrics.nodes_executed
                )
                
                return metrics
                
            except Exception as e:
                # Record failure
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                
                self.observability.record_workflow_execution(
                    workflow_name=workflow_name,
                    duration=0,
                    status="error",
                    nodes_executed=0
                )
                
                raise
            finally:
                # Update active workflows
                self.observability.active_workflows.labels(workflow_name=workflow_name).dec()
    
    async def _execute_node(self, node: Node, cycle: int = 0) -> Any:
        """Execute node with instrumentation"""
        workflow_name = self.workflow.name
        
        # Create node span
        with self.observability.create_node_span(node.name, workflow_name) as span:
            try:
                # Prepare inputs
                inputs = {}
                for input_name, source_node in node.input_connections.items():
                    inputs[input_name] = self.workflow.context.get_result(source_node.name)
                
                # Debug hook - before execution
                if self.debugger:
                    await self.debugger.on_node_start(node, self.workflow.context, inputs)
                
                # Execute node
                start_time = time.time()
                result = await node.execute(self.workflow.context, **inputs)
                duration = time.time() - start_time
                
                # Store result
                result_key = f"{node.name}.{cycle}" if cycle > 0 else node.name
                self.workflow.context.set_result(result_key, result)
                
                # Debug hook - after execution
                if self.debugger:
                    await self.debugger.on_node_complete(node, self.workflow.context, result, duration)
                
                # Record metrics
                span.set_status(Status(StatusCode.OK))
                self.observability.record_node_execution(
                    workflow_name=workflow_name,
                    node_name=node.name,
                    duration=duration,
                    status="success"
                )
                
                # Update execution metrics
                self.metrics.node_times[node.name] = duration
                self.metrics.nodes_executed += 1
                self.metrics.execution_order.append(node.name)
                
                return result
                
            except Exception as e:
                # Debug hook - on error
                if self.debugger:
                    await self.debugger.on_node_error(node, self.workflow.context, e)
                
                # Record error
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                
                self.observability.record_node_execution(
                    workflow_name=workflow_name,
                    node_name=node.name,
                    duration=0,
                    status="error"
                )
                
                error_msg = f"Error in node '{node.name}': {str(e)}"
                self.metrics.errors.append(error_msg)
                raise

### Visual Debugging

class VisualDebugger:
    """Visual debugging interface for workflows"""
    
    def __init__(self, workflow: WorkflowBase):
        self.workflow = workflow
        self.execution_state: Dict[str, NodeExecutionState] = {}
    
    def generate_debug_visualization(self, format: str = "html") -> str:
        """Generate interactive debug visualization"""
        if format == "html":
            return self._generate_html_debugger()
        elif format == "mermaid":
            return self._generate_mermaid_debug()
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _generate_html_debugger(self) -> str:
        """Generate HTML/JavaScript debugging interface"""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Fast-DAG Debugger</title>
            <script src="https://d3js.org/d3.v7.min.js"></script>
            <style>
                .node {
                    fill: #f0f0f0;
                    stroke: #333;
                    stroke-width: 2px;
                }
                .node.running { fill: #ffeb3b; }
                .node.completed { fill: #4caf50; }
                .node.error { fill: #f44336; }
                .node.breakpoint { stroke: #ff0000; stroke-width: 4px; }
                .link { stroke: #999; stroke-width: 2px; }
            </style>
        </head>
        <body>
            <div id="controls">
                <button onclick="step()">Step</button>
                <button onclick="continue()">Continue</button>
                <button onclick="reset()">Reset</button>
            </div>
            <div id="graph"></div>
            <div id="inspector">
                <h3>Node Inspector</h3>
                <div id="node-details"></div>
            </div>
            <script>
                // D3.js visualization code
                const nodes = %s;
                const links = %s;
                const debugState = %s;
                
                // Interactive debugging logic
            </script>
        </body>
        </html>
        """ % (
            json.dumps(self._get_nodes_data()),
            json.dumps(self._get_links_data()),
            json.dumps(self._get_debug_state())
        )
        return html
    
    def _generate_mermaid_debug(self) -> str:
        """Generate Mermaid diagram with debug state"""
        lines = ["graph TD"]
        
        for node_name, node in self.workflow.nodes.items():
            state = self.execution_state.get(node_name, NodeExecutionState())
            
            # Style based on state
            style = "fill:#f0f0f0"
            if state.status == "running":
                style = "fill:#ffeb3b"
            elif state.status == "completed":
                style = "fill:#4caf50"
            elif state.status == "error":
                style = "fill:#f44336"
            
            # Add execution time if available
            label = node_name
            if state.execution_time:
                label += f"\\n{state.execution_time:.2f}ms"
            
            lines.append(f'    {node_name}["{label}"]')
            lines.append(f'    style {node_name} {style}')
            
            # Add connections
            for output_conns in node.output_connections.values():
                for conn_node in output_conns:
                    lines.append(f"    {node_name} --> {conn_node.name}")
        
        return "\\n".join(lines)

@dataclass
class NodeExecutionState:
    """State of node during execution"""
    status: str = "pending"  # pending, running, completed, error
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    execution_time: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[Exception] = None

### Usage Examples

```python
# Debug a workflow
async def debug_workflow():
    # Create workflow
    dag = DAG("complex_pipeline")
    
    @dag.node()
    def process_data(data: list) -> dict:
        return {"count": len(data), "sum": sum(data)}
    
    @dag.node()
    def validate_results(results: dict) -> bool:
        return results["count"] > 0
    
    process_data >> validate_results
    
    # Setup debugging
    debugger = WorkflowDebugger(level=DebugLevel.DEBUG)
    debugger.set_breakpoint("validate_results", "results['count'] < 10")
    
    # Setup observability
    observability = ObservabilityManager("my_service")
    
    # Create instrumented runner
    runner = InstrumentedRunner(dag, debugger=debugger, observability=observability)
    
    # Run with debugging
    metrics = await runner.run_async(inputs={"data": [1, 2, 3, 4, 5]})
    
    # Export trace
    debugger.export_trace("execution_trace.json")
    
    # Generate visualization
    visual_debugger = VisualDebugger(dag)
    html_debug = visual_debugger.generate_debug_visualization("html")
    with open("debug.html", "w") as f:
        f.write(html_debug)

# Monitor workflow in production
def setup_monitoring():
    # Configure Prometheus endpoint
    from prometheus_client import start_http_server
    start_http_server(8000)
    
    # Configure OpenTelemetry
    from opentelemetry.exporter.otlp.proto.grpc import trace_exporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    
    otlp_exporter = trace_exporter.OTLPSpanExporter(
        endpoint="http://localhost:4317",
        insecure=True
    )
    
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(otlp_exporter)
    )
```

## Security Considerations

### Input Validation and Sanitization

```python
from typing import Any, Type, Union, get_type_hints
import re
import ast
from dataclasses import dataclass

@dataclass
class SecurityPolicy:
    """Security policy configuration"""
    allow_eval: bool = False
    allow_exec: bool = False
    allow_file_access: bool = True
    allowed_file_paths: List[str] = field(default_factory=list)
    max_input_size_bytes: int = 10 * 1024 * 1024  # 10MB
    max_string_length: int = 1_000_000
    allowed_modules: Set[str] = field(default_factory=lambda: {
        "math", "datetime", "json", "collections", "itertools", "functools"
    })
    forbidden_attributes: Set[str] = field(default_factory=lambda: {
        "__import__", "__builtins__", "__loader__", "__spec__", 
        "eval", "exec", "compile", "open", "input"
    })

class InputValidator:
    """Validate and sanitize inputs for security"""
    
    def __init__(self, policy: SecurityPolicy = None):
        self.policy = policy or SecurityPolicy()
    
    def validate_node_input(self, node: Node, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Validate inputs against node type hints"""
        validated = {}
        
        # Get type hints from function
        try:
            hints = get_type_hints(node.func)
        except:
            hints = {}
        
        for key, value in inputs.items():
            # Check input size
            if self._get_size(value) > self.policy.max_input_size_bytes:
                raise SecurityError(f"Input '{key}' exceeds maximum size limit")
            
            # Validate against type hint if available
            if key in hints:
                expected_type = hints[key]
                if not self._validate_type(value, expected_type):
                    raise TypeError(f"Input '{key}' has incorrect type. Expected {expected_type}, got {type(value)}")
            
            # Sanitize strings
            if isinstance(value, str):
                value = self._sanitize_string(value)
            
            validated[key] = value
        
        return validated
    
    def _validate_type(self, value: Any, expected_type: Type) -> bool:
        """Check if value matches expected type"""
        # Handle Union types
        if hasattr(expected_type, '__origin__'):
            if expected_type.__origin__ is Union:
                return any(self._validate_type(value, t) for t in expected_type.__args__)
        
        # Direct type check
        return isinstance(value, expected_type)
    
    def _sanitize_string(self, value: str) -> str:
        """Sanitize string inputs"""
        # Limit length
        if len(value) > self.policy.max_string_length:
            value = value[:self.policy.max_string_length]
        
        # Remove null bytes
        value = value.replace('\x00', '')
        
        # Validate no code injection attempts
        if self._contains_code_injection(value):
            raise SecurityError("Potential code injection detected")
        
        return value
    
    def _contains_code_injection(self, value: str) -> bool:
        """Check for potential code injection patterns"""
        dangerous_patterns = [
            r'__[a-zA-Z]+__',  # Dunder methods
            r'eval\s*\(',       # eval calls
            r'exec\s*\(',       # exec calls
            r'import\s+',       # import statements
            r'globals\s*\(',    # globals access
            r'locals\s*\(',     # locals access
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, value):
                return True
        
        return False
    
    def _get_size(self, obj: Any) -> int:
        """Get approximate size of object in bytes"""
        import sys
        return sys.getsizeof(obj)

### Sandboxed Execution

class SandboxedExecutor:
    """Execute nodes in a sandboxed environment"""
    
    def __init__(self, policy: SecurityPolicy = None):
        self.policy = policy or SecurityPolicy()
    
    async def execute_sandboxed(self, node: Node, context: Context, **kwargs) -> Any:
        """Execute node with security restrictions"""
        # Create restricted globals
        restricted_globals = self._create_restricted_globals()
        
        # Wrap the function to run in restricted environment
        if asyncio.iscoroutinefunction(node.func):
            result = await self._execute_async_restricted(
                node.func, restricted_globals, context, **kwargs
            )
        else:
            result = self._execute_sync_restricted(
                node.func, restricted_globals, context, **kwargs
            )
        
        return result
    
    def _create_restricted_globals(self) -> Dict[str, Any]:
        """Create restricted global namespace"""
        import builtins
        
        # Start with safe builtins only
        safe_builtins = {
            'None': None,
            'True': True,
            'False': False,
            'abs': abs,
            'all': all,
            'any': any,
            'bool': bool,
            'dict': dict,
            'enumerate': enumerate,
            'filter': filter,
            'float': float,
            'int': int,
            'len': len,
            'list': list,
            'map': map,
            'max': max,
            'min': min,
            'range': range,
            'round': round,
            'set': set,
            'sorted': sorted,
            'str': str,
            'sum': sum,
            'tuple': tuple,
            'zip': zip,
        }
        
        # Add allowed modules
        restricted_globals = {'__builtins__': safe_builtins}
        
        for module_name in self.policy.allowed_modules:
            try:
                module = __import__(module_name)
                restricted_globals[module_name] = module
            except ImportError:
                pass
        
        return restricted_globals
    
    def _execute_sync_restricted(self, func, restricted_globals, context, **kwargs):
        """Execute synchronous function with restrictions"""
        # Create a new function with restricted globals
        import types
        
        restricted_func = types.FunctionType(
            func.__code__,
            restricted_globals,
            func.__name__,
            func.__defaults__,
            func.__closure__
        )
        
        return restricted_func(context=context, **kwargs)
    
    async def _execute_async_restricted(self, func, restricted_globals, context, **kwargs):
        """Execute async function with restrictions"""
        import types
        
        restricted_func = types.FunctionType(
            func.__code__,
            restricted_globals,
            func.__name__,
            func.__defaults__,
            func.__closure__
        )
        
        return await restricted_func(context=context, **kwargs)

### Resource Limits

class ResourceLimiter:
    """Enforce resource limits on node execution"""
    
    def __init__(
        self,
        max_memory_mb: int = 1024,
        max_cpu_time_seconds: float = 60.0,
        max_real_time_seconds: float = 300.0,
        max_file_handles: int = 100
    ):
        self.max_memory_mb = max_memory_mb
        self.max_cpu_time_seconds = max_cpu_time_seconds
        self.max_real_time_seconds = max_real_time_seconds
        self.max_file_handles = max_file_handles
    
    @contextmanager
    def limit_resources(self):
        """Context manager to enforce resource limits"""
        if sys.platform != "win32":
            import resource
            
            # Set memory limit
            soft, hard = resource.getrlimit(resource.RLIMIT_AS)
            resource.setrlimit(
                resource.RLIMIT_AS,
                (self.max_memory_mb * 1024 * 1024, hard)
            )
            
            # Set CPU time limit
            resource.setrlimit(
                resource.RLIMIT_CPU,
                (int(self.max_cpu_time_seconds), int(self.max_cpu_time_seconds) + 10)
            )
            
            # Set file handle limit
            resource.setrlimit(
                resource.RLIMIT_NOFILE,
                (self.max_file_handles, self.max_file_handles)
            )
            
            try:
                yield
            finally:
                # Restore original limits
                resource.setrlimit(resource.RLIMIT_AS, (soft, hard))
        else:
            # Windows doesn't support resource limits the same way
            # Use alternative methods or skip
            yield
    
    async def execute_with_timeout(self, coro, timeout: float = None):
        """Execute coroutine with timeout"""
        timeout = timeout or self.max_real_time_seconds
        
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            raise SecurityError(f"Execution exceeded time limit of {timeout}s")

### Audit Logging

class AuditLogger:
    """Security audit logging"""
    
    def __init__(self, log_file: str = "workflow_audit.log"):
        self.log_file = log_file
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Setup audit logger with secure configuration"""
        import logging.handlers
        
        logger = logging.getLogger("workflow_audit")
        logger.setLevel(logging.INFO)
        
        # Rotating file handler with size limit
        handler = logging.handlers.RotatingFileHandler(
            self.log_file,
            maxBytes=100 * 1024 * 1024,  # 100MB
            backupCount=10,
            encoding='utf-8'
        )
        
        # Secure format with all relevant info
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def log_workflow_execution(
        self,
        workflow_name: str,
        user_id: Optional[str],
        inputs: Dict[str, Any],
        status: str,
        duration: float
    ):
        """Log workflow execution for audit"""
        self.logger.info(
            f"WORKFLOW_EXECUTION: "
            f"workflow={workflow_name}, "
            f"user={user_id or 'anonymous'}, "
            f"input_keys={list(inputs.keys())}, "
            f"status={status}, "
            f"duration_ms={duration * 1000:.2f}"
        )
    
    def log_security_event(
        self,
        event_type: str,
        workflow_name: str,
        node_name: str,
        details: str
    ):
        """Log security-related events"""
        self.logger.warning(
            f"SECURITY_EVENT: "
            f"type={event_type}, "
            f"workflow={workflow_name}, "
            f"node={node_name}, "
            f"details={details}"
        )
    
    def log_access_denied(
        self,
        resource: str,
        user_id: Optional[str],
        reason: str
    ):
        """Log access denied events"""
        self.logger.error(
            f"ACCESS_DENIED: "
            f"resource={resource}, "
            f"user={user_id or 'anonymous'}, "
            f"reason={reason}"
        )

### Secure Runner

class SecureRunner(InstrumentedRunner):
    """Runner with full security enforcement"""
    
    def __init__(
        self,
        workflow: WorkflowBase,
        security_policy: SecurityPolicy = None,
        audit_logger: Optional[AuditLogger] = None,
        **kwargs
    ):
        super().__init__(workflow, **kwargs)
        self.security_policy = security_policy or SecurityPolicy()
        self.input_validator = InputValidator(self.security_policy)
        self.sandboxed_executor = SandboxedExecutor(self.security_policy)
        self.resource_limiter = ResourceLimiter()
        self.audit_logger = audit_logger or AuditLogger()
    
    async def run_async(
        self,
        inputs: Optional[Dict[str, Any]] = None,
        mode: str = "sequential",
        user_id: Optional[str] = None
    ) -> ExecutionMetrics:
        """Run workflow with security checks"""
        workflow_name = self.workflow.name
        start_time = time.time()
        
        try:
            # Validate inputs
            if inputs:
                validated_inputs = {}
                for key, value in inputs.items():
                    try:
                        validated = self.input_validator.validate_node_input(
                            self.workflow.nodes.get(key, Node(lambda x: x)),
                            {key: value}
                        )
                        validated_inputs.update(validated)
                    except Exception as e:
                        self.audit_logger.log_security_event(
                            "INPUT_VALIDATION_FAILED",
                            workflow_name,
                            key,
                            str(e)
                        )
                        raise
            else:
                validated_inputs = {}
            
            # Execute with resource limits
            with self.resource_limiter.limit_resources():
                metrics = await super().run_async(validated_inputs, mode)
            
            # Log successful execution
            self.audit_logger.log_workflow_execution(
                workflow_name=workflow_name,
                user_id=user_id,
                inputs=validated_inputs,
                status="success",
                duration=time.time() - start_time
            )
            
            return metrics
            
        except Exception as e:
            # Log failed execution
            self.audit_logger.log_workflow_execution(
                workflow_name=workflow_name,
                user_id=user_id,
                inputs=inputs or {},
                status="error",
                duration=time.time() - start_time
            )
            raise
    
    async def _execute_node(self, node: Node, cycle: int = 0) -> Any:
        """Execute node with security sandboxing"""
        # Use sandboxed execution if policy requires
        if not self.security_policy.allow_exec:
            # Prepare inputs
            inputs = {}
            for input_name, source_node in node.input_connections.items():
                inputs[input_name] = self.workflow.context.get_result(source_node.name)
            
            # Validate inputs
            validated_inputs = self.input_validator.validate_node_input(node, inputs)
            
            # Execute in sandbox with timeout
            coro = self.sandboxed_executor.execute_sandboxed(
                node, self.workflow.context, **validated_inputs
            )
            result = await self.resource_limiter.execute_with_timeout(coro)
            
            # Store result
            result_key = f"{node.name}.{cycle}" if cycle > 0 else node.name
            self.workflow.context.set_result(result_key, result)
            
            return result
        else:
            # Use normal execution
            return await super()._execute_node(node, cycle)

class SecurityError(Exception):
    """Security-related errors"""
    pass

### Usage Example

```python
# Secure workflow execution
async def run_secure_workflow():
    # Create workflow
    dag = DAG("user_data_processing")
    
    @dag.node()
    def process_user_input(user_data: dict) -> dict:
        # This function runs in a sandbox
        return {
            "name": user_data.get("name", "").upper(),
            "age": int(user_data.get("age", 0))
        }
    
    @dag.node()
    def validate_data(data: dict) -> bool:
        return data["age"] >= 0 and len(data["name"]) > 0
    
    process_user_input >> validate_data
    
    # Configure security policy
    policy = SecurityPolicy(
        allow_eval=False,
        allow_exec=False,
        max_input_size_bytes=1024 * 1024,  # 1MB
        allowed_modules={"json", "math"}
    )
    
    # Create secure runner
    runner = SecureRunner(
        workflow=dag,
        security_policy=policy,
        audit_logger=AuditLogger("secure_workflow.log")
    )
    
    # Run with user input
    try:
        metrics = await runner.run_async(
            inputs={
                "user_data": {
                    "name": "Alice",
                    "age": "25"
                }
            },
            user_id="user123"
        )
        print(f"Workflow completed successfully: {dag.results}")
    except SecurityError as e:
        print(f"Security violation: {e}")
```

## Enhanced Node System with Lifecycle Hooks

### Node Lifecycle and Hooks

```python
from enum import Enum
from typing import Optional, Callable, Any, Dict
from abc import ABC, abstractmethod

class NodeLifecycle(Enum):
    """Node lifecycle stages"""
    CREATED = "created"
    INITIALIZING = "initializing"
    READY = "ready"
    EXECUTING = "executing"
    COMPLETED = "completed"
    ERROR = "error"
    DISPOSED = "disposed"

class LifecycleHook(ABC):
    """Base class for lifecycle hooks"""
    
    @abstractmethod
    async def on_before_init(self, node: 'EnhancedNode', context: Context) -> None:
        """Called before node initialization"""
        pass
    
    @abstractmethod
    async def on_after_init(self, node: 'EnhancedNode', context: Context) -> None:
        """Called after node initialization"""
        pass
    
    @abstractmethod
    async def on_before_execute(self, node: 'EnhancedNode', context: Context, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Called before node execution, can modify inputs"""
        return inputs
    
    @abstractmethod
    async def on_after_execute(self, node: 'EnhancedNode', context: Context, result: Any) -> Any:
        """Called after node execution, can modify result"""
        return result
    
    @abstractmethod
    async def on_error(self, node: 'EnhancedNode', context: Context, error: Exception) -> Optional[Any]:
        """Called on execution error, can provide fallback result"""
        raise error
    
    @abstractmethod
    async def on_dispose(self, node: 'EnhancedNode', context: Context) -> None:
        """Called when node is being disposed"""
        pass

class LoggingHook(LifecycleHook):
    """Example logging hook"""
    
    def __init__(self, logger):
        self.logger = logger
    
    async def on_before_init(self, node, context):
        self.logger.debug(f"Initializing node: {node.name}")
    
    async def on_after_init(self, node, context):
        self.logger.debug(f"Node initialized: {node.name}")
    
    async def on_before_execute(self, node, context, inputs):
        self.logger.info(f"Executing node: {node.name} with inputs: {list(inputs.keys())}")
        return inputs
    
    async def on_after_execute(self, node, context, result):
        self.logger.info(f"Node completed: {node.name}")
        return result
    
    async def on_error(self, node, context, error):
        self.logger.error(f"Node error: {node.name} - {error}")
        raise error
    
    async def on_dispose(self, node, context):
        self.logger.debug(f"Disposing node: {node.name}")

class ValidationHook(LifecycleHook):
    """Input/output validation hook"""
    
    def __init__(self, input_schema: Dict = None, output_schema: Dict = None):
        self.input_schema = input_schema
        self.output_schema = output_schema
    
    async def on_before_execute(self, node, context, inputs):
        if self.input_schema:
            # Validate inputs against schema
            for key, schema in self.input_schema.items():
                if key in inputs:
                    value = inputs[key]
                    if not isinstance(value, schema.get("type", object)):
                        raise ValueError(f"Invalid type for {key}: expected {schema['type']}, got {type(value)}")
        return inputs
    
    async def on_after_execute(self, node, context, result):
        if self.output_schema:
            # Validate output against schema
            expected_type = self.output_schema.get("type", object)
            if not isinstance(result, expected_type):
                raise ValueError(f"Invalid output type: expected {expected_type}, got {type(result)}")
        return result

class EnhancedNode(Node):
    """Node with lifecycle hooks and versioning"""
    
    def __init__(
        self,
        func: Callable,
        name: Optional[str] = None,
        version: str = "1.0.0",
        hooks: List[LifecycleHook] = None,
        metadata: Dict[str, Any] = None,
        **kwargs
    ):
        super().__init__(func, name, **kwargs)
        self.version = version
        self.hooks = hooks or []
        self.metadata = metadata or {}
        self.lifecycle_state = NodeLifecycle.CREATED
        self._initialized = False
    
    async def initialize(self, context: Context) -> None:
        """Initialize node with hooks"""
        if self._initialized:
            return
        
        self.lifecycle_state = NodeLifecycle.INITIALIZING
        
        # Run before init hooks
        for hook in self.hooks:
            await hook.on_before_init(self, context)
        
        # Perform initialization
        # ... custom initialization logic ...
        
        # Run after init hooks
        for hook in self.hooks:
            await hook.on_after_init(self, context)
        
        self.lifecycle_state = NodeLifecycle.READY
        self._initialized = True
    
    async def execute(self, context: Context, **kwargs) -> Any:
        """Execute with lifecycle hooks"""
        if not self._initialized:
            await self.initialize(context)
        
        self.lifecycle_state = NodeLifecycle.EXECUTING
        
        try:
            # Run before execute hooks
            inputs = kwargs
            for hook in self.hooks:
                inputs = await hook.on_before_execute(self, context, inputs)
            
            # Execute function
            if asyncio.iscoroutinefunction(self.func):
                result = await self.func(**inputs)
            else:
                result = self.func(**inputs)
            
            # Run after execute hooks
            for hook in self.hooks:
                result = await hook.on_after_execute(self, context, result)
            
            self.lifecycle_state = NodeLifecycle.COMPLETED
            return result
            
        except Exception as e:
            self.lifecycle_state = NodeLifecycle.ERROR
            
            # Run error hooks
            for hook in self.hooks:
                try:
                    result = await hook.on_error(self, context, e)
                    if result is not None:
                        return result
                except Exception:
                    continue
            
            raise
    
    async def dispose(self, context: Context) -> None:
        """Dispose node resources"""
        for hook in self.hooks:
            await hook.on_dispose(self, context)
        
        self.lifecycle_state = NodeLifecycle.DISPOSED
    
    def add_hook(self, hook: LifecycleHook) -> None:
        """Add lifecycle hook"""
        self.hooks.append(hook)
    
    def remove_hook(self, hook: LifecycleHook) -> None:
        """Remove lifecycle hook"""
        self.hooks.remove(hook)

### Node Versioning

class NodeVersion:
    """Semantic versioning for nodes"""
    
    def __init__(self, version_string: str):
        parts = version_string.split('.')
        self.major = int(parts[0])
        self.minor = int(parts[1]) if len(parts) > 1 else 0
        self.patch = int(parts[2]) if len(parts) > 2 else 0
    
    def __str__(self):
        return f"{self.major}.{self.minor}.{self.patch}"
    
    def __eq__(self, other):
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)
    
    def __lt__(self, other):
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    
    def is_compatible(self, other: 'NodeVersion') -> bool:
        """Check if versions are compatible (same major version)"""
        return self.major == other.major

class VersionedNodeRegistry:
    """Registry for versioned nodes"""
    
    def __init__(self):
        self.nodes: Dict[str, Dict[str, EnhancedNode]] = {}  # name -> version -> node
    
    def register(self, node: EnhancedNode) -> None:
        """Register a versioned node"""
        if node.name not in self.nodes:
            self.nodes[node.name] = {}
        
        self.nodes[node.name][node.version] = node
    
    def get(self, name: str, version: str = None) -> EnhancedNode:
        """Get node by name and version"""
        if name not in self.nodes:
            raise KeyError(f"Node '{name}' not found")
        
        if version:
            if version not in self.nodes[name]:
                raise KeyError(f"Node '{name}' version '{version}' not found")
            return self.nodes[name][version]
        else:
            # Return latest version
            versions = sorted(self.nodes[name].keys(), key=NodeVersion, reverse=True)
            return self.nodes[name][versions[0]]
    
    def get_compatible(self, name: str, version: str) -> EnhancedNode:
        """Get compatible node version"""
        if name not in self.nodes:
            raise KeyError(f"Node '{name}' not found")
        
        target_version = NodeVersion(version)
        compatible_versions = []
        
        for ver_str, node in self.nodes[name].items():
            ver = NodeVersion(ver_str)
            if ver.is_compatible(target_version) and ver >= target_version:
                compatible_versions.append((ver, node))
        
        if not compatible_versions:
            raise KeyError(f"No compatible version found for '{name}' version '{version}'")
        
        # Return the minimum compatible version
        compatible_versions.sort(key=lambda x: x[0])
        return compatible_versions[0][1]

### Advanced Node Features

class CachedNode(EnhancedNode):
    """Node with result caching"""
    
    def __init__(self, *args, cache_ttl: float = 3600, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache_ttl = cache_ttl
        self.cache: Dict[str, Tuple[Any, float]] = {}
    
    async def execute(self, context: Context, **kwargs) -> Any:
        # Create cache key
        cache_key = self._create_cache_key(kwargs)
        
        # Check cache
        if cache_key in self.cache:
            result, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return result
        
        # Execute and cache
        result = await super().execute(context, **kwargs)
        self.cache[cache_key] = (result, time.time())
        
        return result
    
    def _create_cache_key(self, inputs: Dict[str, Any]) -> str:
        """Create cache key from inputs"""
        import hashlib
        import json
        
        # Sort keys for consistent hashing
        sorted_inputs = dict(sorted(inputs.items()))
        input_str = json.dumps(sorted_inputs, sort_keys=True, default=str)
        
        return hashlib.sha256(input_str.encode()).hexdigest()
    
    def clear_cache(self) -> None:
        """Clear the cache"""
        self.cache.clear()

class RetryableNode(EnhancedNode):
    """Node with built-in retry logic"""
    
    def __init__(
        self,
        *args,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
        retry_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff
        self.retry_exceptions = retry_exceptions
    
    async def execute(self, context: Context, **kwargs) -> Any:
        """Execute with retry logic"""
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return await super().execute(context, **kwargs)
            except self.retry_exceptions as e:
                last_error = e
                
                if attempt < self.max_retries:
                    delay = self.retry_delay * (self.retry_backoff ** attempt)
                    await asyncio.sleep(delay)
                    continue
                else:
                    raise
        
        raise last_error

### Usage Example

```python
# Using enhanced nodes with lifecycle hooks
async def test_enhanced_nodes():
    # Create workflow with enhanced nodes
    dag = DAG("enhanced_workflow")
    
    # Create hooks
    logger = logging.getLogger(__name__)
    logging_hook = LoggingHook(logger)
    validation_hook = ValidationHook(
        input_schema={"data": {"type": list}},
        output_schema={"type": dict}
    )
    
    # Create versioned node with hooks
    @dag.node()
    def process_v1(data: list) -> dict:
        return {"count": len(data), "version": "1.0.0"}
    
    enhanced_node = EnhancedNode(
        func=process_v1,
        name="processor",
        version="1.0.0",
        hooks=[logging_hook, validation_hook],
        metadata={"author": "system", "description": "Data processor v1"}
    )
    
    # Register in version registry
    registry = VersionedNodeRegistry()
    registry.register(enhanced_node)
    
    # Create v2 with caching
    cached_node = CachedNode(
        func=lambda data: {"count": len(data), "sum": sum(data), "version": "2.0.0"},
        name="processor",
        version="2.0.0",
        cache_ttl=300,
        hooks=[logging_hook]
    )
    registry.register(cached_node)
    
    # Use specific version
    dag.add_node(registry.get("processor", "1.0.0"))
    
    # Run workflow
    runner = Runner(dag)
    await runner.run_async(inputs={"data": [1, 2, 3, 4, 5]})
```

## Context Isolation and Serialization

### Thread-Safe Context Management

```python
import threading
import copy
from typing import Any, Dict, Optional
import pickle
import json

class IsolatedContext(Context):
    """Thread-safe isolated context"""
    
    def __init__(self):
        super().__init__()
        self._lock = threading.RLock()
        self._local = threading.local()
    
    def get_result(self, node_name: str) -> Any:
        """Thread-safe result retrieval"""
        with self._lock:
            return super().get_result(node_name)
    
    def set_result(self, node_name: str, value: Any) -> None:
        """Thread-safe result storage"""
        with self._lock:
            # Create deep copy to prevent mutations
            super().set_result(node_name, copy.deepcopy(value))
    
    def create_child_context(self) -> 'IsolatedContext':
        """Create isolated child context"""
        with self._lock:
            child = IsolatedContext()
            child.results = copy.deepcopy(self.results)
            child.metadata = copy.deepcopy(self.metadata)
            return child
    
    def merge_child_context(self, child: 'IsolatedContext', prefix: str = None) -> None:
        """Merge child context results back"""
        with self._lock:
            for key, value in child.results.items():
                if prefix:
                    key = f"{prefix}.{key}"
                self.set_result(key, value)

class SerializableContext(IsolatedContext):
    """Context with serialization support"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary"""
        with self._lock:
            return {
                "results": self._serialize_results(self.results),
                "metadata": self._serialize_results(self.metadata),
                "execution_info": self._serialize_results(self.execution_info)
            }
    
    def from_dict(self, data: Dict[str, Any]) -> None:
        """Load context from dictionary"""
        with self._lock:
            self.results = self._deserialize_results(data.get("results", {}))
            self.metadata = self._deserialize_results(data.get("metadata", {}))
            self.execution_info = self._deserialize_results(data.get("execution_info", {}))
    
    def to_json(self) -> str:
        """Convert context to JSON"""
        return json.dumps(self.to_dict(), default=str)
    
    def from_json(self, json_str: str) -> None:
        """Load context from JSON"""
        data = json.loads(json_str)
        self.from_dict(data)
    
    def to_pickle(self) -> bytes:
        """Pickle context"""
        with self._lock:
            return pickle.dumps(self.to_dict())
    
    def from_pickle(self, data: bytes) -> None:
        """Load context from pickle"""
        self.from_dict(pickle.loads(data))
    
    def _serialize_results(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize results for storage"""
        serialized = {}
        for key, value in data.items():
            try:
                # Try JSON serialization first
                json.dumps(value)
                serialized[key] = value
            except:
                # Fall back to string representation
                serialized[key] = {"__type__": "repr", "__value__": repr(value)}
        return serialized
    
    def _deserialize_results(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize results from storage"""
        deserialized = {}
        for key, value in data.items():
            if isinstance(value, dict) and "__type__" in value:
                if value["__type__"] == "repr":
                    # Keep as string representation
                    deserialized[key] = value["__value__"]
                else:
                    deserialized[key] = value
            else:
                deserialized[key] = value
        return deserialized

### Context Checkpointing

class CheckpointableContext(SerializableContext):
    """Context with checkpointing support"""
    
    def __init__(self, checkpoint_dir: str = None):
        super().__init__()
        self.checkpoint_dir = checkpoint_dir or tempfile.mkdtemp()
        self.checkpoint_history: List[str] = []
    
    def save_checkpoint(self, name: str = None) -> str:
        """Save context checkpoint"""
        import uuid
        
        if not name:
            name = f"checkpoint_{uuid.uuid4().hex[:8]}_{int(time.time())}"
        
        checkpoint_path = os.path.join(self.checkpoint_dir, f"{name}.pkl")
        
        with open(checkpoint_path, "wb") as f:
            f.write(self.to_pickle())
        
        self.checkpoint_history.append(checkpoint_path)
        return checkpoint_path
    
    def load_checkpoint(self, checkpoint_path: str) -> None:
        """Load context from checkpoint"""
        with open(checkpoint_path, "rb") as f:
            self.from_pickle(f.read())
    
    def list_checkpoints(self) -> List[str]:
        """List available checkpoints"""
        return sorted(glob.glob(os.path.join(self.checkpoint_dir, "checkpoint_*.pkl")))
    
    def cleanup_checkpoints(self, keep_last: int = 5) -> None:
        """Clean up old checkpoints"""
        checkpoints = self.list_checkpoints()
        if len(checkpoints) > keep_last:
            for checkpoint in checkpoints[:-keep_last]:
                os.remove(checkpoint)

### Distributed Context

class DistributedContext(SerializableContext):
    """Context for distributed execution"""
    
    def __init__(self, backend: str = "redis", **backend_config):
        super().__init__()
        self.backend = self._setup_backend(backend, backend_config)
        self.node_id = socket.gethostname()
    
    def _setup_backend(self, backend: str, config: Dict[str, Any]):
        """Setup distributed backend"""
        if backend == "redis":
            import redis
            return redis.Redis(**config)
        elif backend == "etcd":
            import etcd3
            return etcd3.client(**config)
        else:
            raise ValueError(f"Unsupported backend: {backend}")
    
    def set_result(self, node_name: str, value: Any) -> None:
        """Set result in distributed storage"""
        super().set_result(node_name, value)
        
        # Also store in distributed backend
        key = f"workflow:result:{node_name}"
        serialized = json.dumps(value, default=str)
        
        if hasattr(self.backend, "set"):  # Redis
            self.backend.set(key, serialized)
        elif hasattr(self.backend, "put"):  # etcd
            self.backend.put(key, serialized)
    
    def get_result(self, node_name: str) -> Any:
        """Get result from distributed storage"""
        # Try local first
        try:
            return super().get_result(node_name)
        except KeyError:
            pass
        
        # Try distributed backend
        key = f"workflow:result:{node_name}"
        
        if hasattr(self.backend, "get"):  # Redis
            value = self.backend.get(key)
        elif hasattr(self.backend, "get"):  # etcd
            value, _ = self.backend.get(key)
        
        if value:
            deserialized = json.loads(value)
            self.results[node_name] = deserialized
            return deserialized
        
        raise KeyError(f"Result for node '{node_name}' not found")
    
    def sync_with_backend(self) -> None:
        """Sync local context with distributed backend"""
        # Pull all results from backend
        if hasattr(self.backend, "scan_iter"):  # Redis
            for key in self.backend.scan_iter("workflow:result:*"):
                node_name = key.decode().split(":")[-1]
                if node_name not in self.results:
                    self.get_result(node_name)

### Usage Example

```python
# Using isolated contexts
async def test_context_isolation():
    # Create parent workflow
    parent_dag = DAG("parent_workflow")
    
    @parent_dag.node()
    async def parent_task(data: list) -> dict:
        # Create isolated context for sub-workflow
        context = IsolatedContext()
        
        # Create sub-workflow
        sub_dag = DAG("sub_workflow")
        
        @sub_dag.node()
        def sub_task(item: int) -> int:
            return item * 2
        
        # Run sub-workflows in parallel with isolated contexts
        tasks = []
        for item in data:
            child_context = context.create_child_context()
            runner = Runner(sub_dag)
            runner.workflow.context = child_context
            tasks.append(runner.run_async(inputs={"item": item}))
        
        await asyncio.gather(*tasks)
        
        # Merge results
        results = {}
        for i, task in enumerate(tasks):
            child_context = task.workflow.context
            context.merge_child_context(child_context, prefix=f"item_{i}")
        
        return {"processed": len(data)}
    
    # Run with checkpointing
    checkpoint_context = CheckpointableContext("/tmp/workflow_checkpoints")
    parent_dag.context = checkpoint_context
    
    runner = Runner(parent_dag)
    await runner.run_async(inputs={"data": [1, 2, 3, 4, 5]})
    
    # Save checkpoint
    checkpoint_path = checkpoint_context.save_checkpoint("after_completion")
    print(f"Checkpoint saved: {checkpoint_path}")
    
    # Test distributed context
    distributed_context = DistributedContext(
        backend="redis",
        host="localhost",
        port=6379
    )
    
    # Share results across workers
    distributed_context.set_result("shared_data", {"status": "ready"})
```

## API Reference

### Core Classes

#### Node
```python
class Node:
    """Base node class representing a unit of work
    
    Args:
        func: Callable to execute
        name: Optional node name (defaults to function name)
        inputs: Optional list of input parameter names
        outputs: Optional list of output names
        description: Optional node description
        node_type: Type of node ("standard", "condition", "fsm_state")
    
    Methods:
        execute(context, **kwargs): Execute the node function
        connect_to(other, output_name, input_name): Connect to another node
        
    Operators:
        >> : Connect nodes (node1 >> node2)
        | : Alternative connection syntax (node1 | node2)
    """
```

#### Context
```python
class Context:
    """Execution context passed between nodes
    
    Attributes:
        results: Dict[str, Any] - Node execution results
        metadata: Dict[str, Any] - Workflow metadata
        execution_info: Dict[str, Any] - Execution information
    
    Methods:
        get_result(node_name): Get result by node name
        set_result(node_name, value): Set result for node
    """
```

#### DAG
```python
class DAG(WorkflowBase):
    """Directed Acyclic Graph workflow
    
    Args:
        name: Workflow name
    
    Methods:
        node(name, inputs, outputs, description): Decorator to create nodes
        condition(name, true_branch, false_branch): Decorator for conditional nodes
        add_node(node): Add existing node to workflow
        set_entrypoint(*node_names): Set workflow entry points
        validate(): Validate workflow structure
        get_dependencies(node_name): Get upstream dependencies
        get_downstream(node_name): Get downstream nodes
        get_result(node_name): Get node result
        
    Properties:
        nodes: Dict of all nodes
        results: All execution results
        context: Workflow context
    """
```

#### FSM
```python
class FSM(WorkflowBase):
    """Finite State Machine workflow
    
    Args:
        name: Workflow name
    
    Additional Attributes:
        current_state: Current state name
        state_history: List of visited states
        cycle_count: Number of cycles executed
        max_cycles: Maximum allowed cycles
    
    Methods:
        state(name, is_initial, is_final): Decorator for state nodes
        get_result(node_name): Get result (supports .X cycle notation)
    """
```

#### Runner
```python
class Runner:
    """Workflow execution engine
    
    Args:
        workflow: Workflow to execute
    
    Methods:
        run(inputs, mode): Run workflow synchronously
        run_async(inputs, mode): Run workflow asynchronously
        
    Args for run methods:
        inputs: Dict[str, Any] - Input values
        mode: str - "sequential" or "parallel"
        
    Returns:
        ExecutionMetrics with timing and error information
    """
```

### Return Types

#### ConditionalReturn
```python
@dataclass
class ConditionalReturn:
    """Return type for conditional nodes
    
    Attributes:
        condition: bool - Condition result
        true_branch: Optional[str] - Node to execute if true
        false_branch: Optional[str] - Node to execute if false
        value: Any - Additional return value
    """
```

#### FSMReturn
```python
@dataclass
class FSMReturn:
    """Return type for FSM state transitions
    
    Attributes:
        next_state: Optional[str] - Next state to transition to
        continue_execution: bool - Whether to continue execution
        value: Any - State execution result
        stop_condition: bool - Whether to stop FSM execution
    """
```

#### ExecutionMetrics
```python
@dataclass
class ExecutionMetrics:
    """Workflow execution metrics
    
    Attributes:
        total_time: float - Total execution time in seconds
        node_times: Dict[str, float] - Per-node execution times
        nodes_executed: int - Number of nodes executed
        errors: List[str] - List of errors encountered
        execution_order: List[str] - Order of node execution
    """
```

### Decorators

#### @workflow.node()
```python
@workflow.node(name=None, inputs=None, outputs=None, description=None)
def my_function(...) -> Any:
    """Create a node from a function"""
```

#### @workflow.condition()
```python
@workflow.condition(name=None, true_branch=None, false_branch=None)
def my_condition(...) -> ConditionalReturn:
    """Create a conditional node"""
```

#### @fsm.state()
```python
@fsm.state(name=None, is_initial=False, is_final=False)
def my_state(...) -> FSMReturn:
    """Create an FSM state"""
```

This specification provides a complete blueprint for implementing the workflow library with all the features requested. The design is modular, extensible, and follows Python best practices with strong typing and comprehensive error handling.