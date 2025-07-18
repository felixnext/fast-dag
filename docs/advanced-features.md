# Advanced Features

This guide covers the advanced features of fast-dag that enable powerful workflow patterns, optimization, and integration capabilities.

## Table of Contents
- [Caching](#caching)
- [Serialization](#serialization)
- [Visualization](#visualization)
- [Custom Node Types](#custom-node-types)
- [Dynamic Workflow Generation](#dynamic-workflow-generation)
- [Advanced Error Handling](#advanced-error-handling)
- [Hooks and Middleware](#hooks-and-middleware)
- [Performance Profiling](#performance-profiling)
- [Integration Patterns](#integration-patterns)

## Caching

fast-dag provides multiple caching strategies to optimize performance.

### Built-in Caching

```python
# Simple TTL cache
@dag.cached_node(ttl=3600)  # Cache for 1 hour
def expensive_api_call(endpoint: str) -> dict:
    return requests.get(endpoint).json()

# Cache with custom key
@dag.cached_node(
    ttl=1800,
    cache_key=["user_id", "date"]  # Cache varies by these parameters
)
def user_report(user_id: str, date: str, filters: dict = None) -> dict:
    # Filters not in cache key - same report for different filters
    return generate_report(user_id, date, filters)
```

### Custom Cache Backends

```python
from fast_dag.caching import CacheBackend
import redis

class RedisCache(CacheBackend):
    def __init__(self, redis_url: str):
        self.client = redis.from_url(redis_url)
        
    def get(self, key: str) -> Any:
        value = self.client.get(key)
        return json.loads(value) if value else None
    
    def set(self, key: str, value: Any, ttl: int = None):
        self.client.set(
            key, 
            json.dumps(value), 
            ex=ttl
        )
    
    def invalidate(self, key: str):
        self.client.delete(key)
    
    def clear(self):
        self.client.flushdb()

# Use custom cache
redis_cache = RedisCache("redis://localhost:6379")

@dag.cached_node(backend=redis_cache, ttl=7200)
def cached_computation(data: dict) -> dict:
    return expensive_computation(data)
```

### Cache Invalidation Patterns

```python
# Manual invalidation
dag.invalidate_cache("expensive_api_call", endpoint="/api/users")

# Pattern-based invalidation
dag.invalidate_cache_pattern("user_report", user_id="123")

# Time-based invalidation with versioning
@dag.cached_node(
    ttl=3600,
    version=lambda: datetime.now().strftime("%Y-%m-%d")  # New cache daily
)
def daily_analytics() -> dict:
    return compute_analytics()

# Conditional caching
@dag.node
def smart_cache(data: dict, use_cache: bool = True) -> dict:
    if use_cache:
        cache_key = hash(json.dumps(data, sort_keys=True))
        cached = cache.get(cache_key)
        if cached:
            return cached
    
    result = process_data(data)
    
    if use_cache:
        cache.set(cache_key, result, ttl=1800)
    
    return result
```

### Distributed Caching

```python
# Distributed cache with consistency
class DistributedCache:
    def __init__(self, nodes: list[str]):
        self.nodes = [Redis(node) for node in nodes]
        self.hash_ring = ConsistentHash(nodes)
    
    def get_node(self, key: str) -> Redis:
        return self.hash_ring.get_node(key)
    
    def get(self, key: str) -> Any:
        node = self.get_node(key)
        return node.get(key)
    
    def set(self, key: str, value: Any, ttl: int = None):
        node = self.get_node(key)
        node.set(key, value, ex=ttl)
        
        # Replicate to backup nodes
        backup_nodes = self.hash_ring.get_replicas(key, n=2)
        for backup in backup_nodes:
            backup.set(f"backup:{key}", value, ex=ttl)
```

## Serialization

Save and load workflows for persistence and distribution.

### Basic Serialization

```python
# Save DAG to file
dag.to_json("workflow.json")
dag.to_yaml("workflow.yaml")
dag.to_msgspec("workflow.msgspec")  # Binary format

# Load DAG from file
loaded_dag = DAG.from_json("workflow.json")
loaded_dag = DAG.from_yaml("workflow.yaml")
loaded_dag = DAG.from_msgspec("workflow.msgspec")

# Serialize to string
json_str = dag.to_json()
yaml_str = dag.to_yaml()
```

### Custom Serialization

```python
# Register custom serializers for your types
from fast_dag.serialization import register_serializer

@dataclass
class CustomModel:
    id: str
    data: dict
    
def serialize_custom_model(obj: CustomModel) -> dict:
    return {"id": obj.id, "data": obj.data, "_type": "CustomModel"}

def deserialize_custom_model(data: dict) -> CustomModel:
    return CustomModel(id=data["id"], data=data["data"])

register_serializer(
    CustomModel,
    serialize_custom_model,
    deserialize_custom_model
)

# Now CustomModel can be used in serialized workflows
@dag.node
def process_model(model: CustomModel) -> dict:
    return model.data
```

### Workflow Templates

```python
# Create a workflow template
template = {
    "name": "data_pipeline_template",
    "nodes": [
        {
            "name": "source",
            "type": "parameter",  # Placeholder node
            "outputs": ["data"]
        },
        {
            "name": "transform",
            "type": "function",
            "function": "transforms.clean_data",
            "inputs": ["data"],
            "outputs": ["clean_data"]
        },
        {
            "name": "sink",
            "type": "parameter",
            "inputs": ["clean_data"]
        }
    ],
    "connections": [
        {"from": "source", "to": "transform"},
        {"from": "transform", "to": "sink"}
    ]
}

# Instantiate template with specific implementations
def create_pipeline(source_func, sink_func):
    dag = DAG.from_dict(template)
    dag.replace_node("source", source_func)
    dag.replace_node("sink", sink_func)
    return dag

# Create specific pipelines
csv_pipeline = create_pipeline(
    source_func=read_csv,
    sink_func=write_to_database
)

api_pipeline = create_pipeline(
    source_func=fetch_from_api,
    sink_func=write_to_s3
)
```

## Visualization

Visualize workflows for debugging and documentation.

### Mermaid Diagrams

```python
# Generate Mermaid diagram
mermaid_code = dag.visualize(backend="mermaid")
print(mermaid_code)

# Output:
# graph TD
#     load_data["load_data"]
#     process["process"]
#     save["save"]
#     load_data --> process
#     process --> save

# With execution results
result = dag.run()
mermaid_code = dag.visualize(
    backend="mermaid",
    show_results=True,
    show_execution_time=True
)

# Custom styling
mermaid_code = dag.visualize(
    backend="mermaid",
    node_style={
        "load_data": "fill:#f9f,stroke:#333,stroke-width:4px",
        "process": "fill:#bbf,stroke:#333,stroke-width:2px"
    },
    edge_style={
        ("load_data", "process"): "stroke:#ff3,stroke-width:4px"
    }
)
```

### Graphviz Visualization

```python
# Generate Graphviz
dag.visualize(
    backend="graphviz",
    filename="workflow",
    format="png",  # or pdf, svg, dot
    engine="dot",  # or neato, fdp, sfdp, twopi, circo
    graph_attr={"rankdir": "LR"},  # Left to right
    node_attr={"shape": "box", "style": "rounded,filled", "fillcolor": "lightblue"},
    edge_attr={"color": "gray"}
)

# With execution metrics
dag.run()
dag.visualize(
    backend="graphviz",
    filename="workflow_with_metrics",
    show_metrics=True,
    metric_format="{name}\n{execution_time:.2f}s\n{memory_mb:.1f}MB"
)
```

### Interactive Visualization

```python
# Web-based visualization server
from fast_dag.visualization import WorkflowVisualizer

visualizer = WorkflowVisualizer(dag)
visualizer.serve(port=8080)  # Opens browser with interactive view

# Features:
# - Zoom and pan
# - Click nodes for details
# - Show/hide execution results
# - Export as image
# - Real-time updates during execution
```

### Custom Visualization

```python
# Create custom visualizer
class CustomVisualizer:
    def __init__(self, dag):
        self.dag = dag
    
    def to_d3_json(self):
        """Convert to D3.js format"""
        nodes = []
        links = []
        
        for name, node in self.dag.nodes.items():
            nodes.append({
                "id": name,
                "group": node.node_type.value,
                "description": node.description
            })
        
        for connection in self.dag.get_all_connections():
            links.append({
                "source": connection["from"],
                "target": connection["to"],
                "value": 1
            })
        
        return {"nodes": nodes, "links": links}
    
    def to_cytoscape(self):
        """Convert to Cytoscape.js format"""
        elements = []
        
        # Add nodes
        for name, node in self.dag.nodes.items():
            elements.append({
                "data": {"id": name, "label": name},
                "classes": node.node_type.value
            })
        
        # Add edges
        for i, connection in enumerate(self.dag.get_all_connections()):
            elements.append({
                "data": {
                    "id": f"edge_{i}",
                    "source": connection["from"],
                    "target": connection["to"]
                }
            })
        
        return elements
```

## Custom Node Types

Create specialized node types for your domain.

### Creating Custom Node Types

```python
from fast_dag.core.node import Node
from fast_dag.core.types import NodeType, NodeReturn

# Define custom node type
class MLNode(Node):
    """Node specialized for ML operations"""
    
    def __init__(self, *args, model_type: str = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.model_type = model_type
        self.model = None
    
    def train(self, X, y):
        """Train the model"""
        if self.model_type == "sklearn":
            from sklearn.ensemble import RandomForestClassifier
            self.model = RandomForestClassifier()
            self.model.fit(X, y)
    
    def predict(self, X):
        """Make predictions"""
        if self.model is None:
            raise ValueError("Model not trained")
        return self.model.predict(X)
    
    def execute(self, inputs: dict, context: Context) -> Any:
        """Custom execution logic"""
        if "train_data" in inputs:
            X, y = inputs["train_data"]
            self.train(X, y)
            return {"model": self.model, "trained": True}
        elif "predict_data" in inputs:
            X = inputs["predict_data"]
            predictions = self.predict(X)
            return {"predictions": predictions}
        else:
            return super().execute(inputs, context)

# Use custom node
ml_node = MLNode(
    func=None,  # Custom execution
    name="ml_classifier",
    model_type="sklearn"
)
dag.add_node(ml_node)
```

### Node Factories

```python
# Factory for creating specialized nodes
class NodeFactory:
    @staticmethod
    def create_validator(validation_type: str, **config):
        """Create validation nodes"""
        validators = {
            "schema": SchemaValidator,
            "range": RangeValidator,
            "regex": RegexValidator,
            "custom": CustomValidator
        }
        
        validator_class = validators.get(validation_type)
        if not validator_class:
            raise ValueError(f"Unknown validator: {validation_type}")
        
        return validator_class(**config)
    
    @staticmethod
    def create_transformer(transform_type: str, **config):
        """Create transformation nodes"""
        transformers = {
            "normalize": NormalizeTransformer,
            "aggregate": AggregateTransformer,
            "pivot": PivotTransformer,
            "join": JoinTransformer
        }
        
        transformer_class = transformers.get(transform_type)
        return transformer_class(**config)

# Use factory
dag.add_node(NodeFactory.create_validator(
    "schema",
    schema={"type": "object", "properties": {"id": {"type": "string"}}}
))

dag.add_node(NodeFactory.create_transformer(
    "normalize",
    columns=["value"],
    method="min-max"
))
```

### Plugin System

```python
# Plugin base class
class WorkflowPlugin:
    """Base class for workflow plugins"""
    
    def register_nodes(self, dag: DAG):
        """Register plugin nodes with DAG"""
        raise NotImplementedError
    
    def register_hooks(self, dag: DAG):
        """Register plugin hooks"""
        pass
    
    def register_serializers(self):
        """Register custom serializers"""
        pass

# Database plugin
class DatabasePlugin(WorkflowPlugin):
    def __init__(self, connection_string: str):
        self.db = Database(connection_string)
    
    def register_nodes(self, dag: DAG):
        @dag.node
        def db_query(query: str) -> list:
            return self.db.execute(query)
        
        @dag.node
        def db_insert(table: str, records: list) -> int:
            return self.db.insert_many(table, records)
        
        @dag.node
        def db_upsert(table: str, records: list, keys: list) -> int:
            return self.db.upsert(table, records, keys)

# Use plugins
db_plugin = DatabasePlugin("postgresql://localhost/mydb")
db_plugin.register_nodes(dag)

# Plugin manager
class PluginManager:
    def __init__(self):
        self.plugins = {}
    
    def register(self, name: str, plugin: WorkflowPlugin):
        self.plugins[name] = plugin
    
    def apply_to_dag(self, dag: DAG):
        for plugin in self.plugins.values():
            plugin.register_nodes(dag)
            plugin.register_hooks(dag)
            plugin.register_serializers()
```

## Dynamic Workflow Generation

Build workflows programmatically based on configuration or runtime conditions.

### Configuration-Driven Workflows

```python
# Workflow configuration
config = {
    "name": "dynamic_etl",
    "sources": [
        {"type": "csv", "path": "data/sales.csv"},
        {"type": "api", "endpoint": "https://api.example.com/orders"},
        {"type": "database", "query": "SELECT * FROM customers"}
    ],
    "transformations": [
        {"type": "filter", "condition": "amount > 100"},
        {"type": "aggregate", "group_by": ["category"], "agg": "sum"},
        {"type": "join", "on": "customer_id"}
    ],
    "destination": {
        "type": "s3",
        "bucket": "processed-data",
        "format": "parquet"
    }
}

# Dynamic DAG builder
class DynamicDAGBuilder:
    def __init__(self, config: dict):
        self.config = config
        self.dag = DAG(config["name"])
        self.source_nodes = []
        
    def build(self) -> DAG:
        # Create source nodes
        for i, source in enumerate(self.config["sources"]):
            node_name = f"source_{source['type']}_{i}"
            self._create_source_node(node_name, source)
            self.source_nodes.append(node_name)
        
        # Create transformation pipeline
        prev_node = self._create_merger_node()
        
        for i, transform in enumerate(self.config["transformations"]):
            node_name = f"transform_{transform['type']}_{i}"
            self._create_transform_node(node_name, transform)
            self.dag.connect(prev_node, node_name)
            prev_node = node_name
        
        # Create destination node
        self._create_destination_node("destination", self.config["destination"])
        self.dag.connect(prev_node, "destination")
        
        return self.dag
    
    def _create_source_node(self, name: str, config: dict):
        source_funcs = {
            "csv": lambda: pd.read_csv(config["path"]),
            "api": lambda: requests.get(config["endpoint"]).json(),
            "database": lambda: db.execute(config["query"])
        }
        
        func = source_funcs.get(config["type"])
        self.dag.add_node(name, func)
    
    def _create_merger_node(self):
        @self.dag.all()
        def merge_sources(**sources) -> pd.DataFrame:
            # Combine all sources into single DataFrame
            dfs = []
            for source_data in sources.values():
                if isinstance(source_data, pd.DataFrame):
                    dfs.append(source_data)
                else:
                    dfs.append(pd.DataFrame(source_data))
            return pd.concat(dfs, ignore_index=True)
        
        # Connect all sources to merger
        for source in self.source_nodes:
            self.dag.connect(source, "merge_sources", input=source)
        
        return "merge_sources"

# Build and run dynamic DAG
builder = DynamicDAGBuilder(config)
etl_dag = builder.build()
result = etl_dag.run()
```

### Runtime Workflow Modification

```python
# Workflow that adapts based on data
@dag.node
def analyze_data(data: pd.DataFrame) -> dict:
    """Analyze data and determine required processing"""
    analysis = {
        "has_nulls": data.isnull().any().any(),
        "has_duplicates": data.duplicated().any(),
        "needs_scaling": (data.select_dtypes(include=[np.number]).std() > 10).any(),
        "has_outliers": detect_outliers(data),
        "categorical_columns": list(data.select_dtypes(include=['object']).columns)
    }
    
    # Dynamically add processing nodes based on analysis
    if analysis["has_nulls"]:
        @dag.node
        def handle_nulls(data: pd.DataFrame) -> pd.DataFrame:
            return data.fillna(data.mean())
        dag.connect("analyze_data", "handle_nulls")
    
    if analysis["has_duplicates"]:
        @dag.node
        def remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
            return data.drop_duplicates()
        if "handle_nulls" in dag.nodes:
            dag.connect("handle_nulls", "remove_duplicates")
        else:
            dag.connect("analyze_data", "remove_duplicates")
    
    if analysis["categorical_columns"]:
        @dag.node
        def encode_categoricals(data: pd.DataFrame) -> pd.DataFrame:
            return pd.get_dummies(data, columns=analysis["categorical_columns"])
        # Connect to last node in chain
        last_node = dag.get_leaf_nodes()[0]
        dag.connect(last_node, "encode_categoricals")
    
    return analysis
```

### Workflow Composition

```python
# Compose workflows from reusable components
class WorkflowLibrary:
    """Library of reusable workflow components"""
    
    @staticmethod
    def create_data_quality_dag() -> DAG:
        dag = DAG("data_quality")
        
        @dag.node
        def check_completeness(data: pd.DataFrame) -> dict:
            return {
                "missing_values": data.isnull().sum().to_dict(),
                "complete_rows": len(data.dropna())
            }
        
        @dag.node
        def check_validity(data: pd.DataFrame) -> dict:
            return validate_data_types(data)
        
        @dag.node
        def check_consistency(data: pd.DataFrame) -> dict:
            return check_referential_integrity(data)
        
        return dag
    
    @staticmethod
    def create_ml_pipeline_dag(model_type: str) -> DAG:
        dag = DAG(f"ml_pipeline_{model_type}")
        
        @dag.node
        def split_data(data: pd.DataFrame) -> dict:
            X = data.drop('target', axis=1)
            y = data['target']
            return train_test_split(X, y, test_size=0.2)
        
        @dag.node
        def train_model(split_data: dict) -> Any:
            model = create_model(model_type)
            model.fit(split_data['X_train'], split_data['y_train'])
            return model
        
        @dag.node
        def evaluate_model(model: Any, split_data: dict) -> dict:
            predictions = model.predict(split_data['X_test'])
            return {
                "accuracy": accuracy_score(split_data['y_test'], predictions),
                "report": classification_report(split_data['y_test'], predictions)
            }
        
        return dag

# Compose complex workflow
def create_full_pipeline(include_quality_check: bool = True) -> DAG:
    main_dag = DAG("full_pipeline")
    
    # Add data loading
    @main_dag.node
    def load_data() -> pd.DataFrame:
        return pd.read_csv("data.csv")
    
    # Optionally add quality checks
    if include_quality_check:
        quality_dag = WorkflowLibrary.create_data_quality_dag()
        main_dag.add_dag("quality_check", quality_dag)
        main_dag.connect("load_data", "quality_check")
        prev_node = "quality_check"
    else:
        prev_node = "load_data"
    
    # Add ML pipeline
    ml_dag = WorkflowLibrary.create_ml_pipeline_dag("random_forest")
    main_dag.add_dag("ml_pipeline", ml_dag)
    main_dag.connect(prev_node, "ml_pipeline")
    
    return main_dag
```

## Advanced Error Handling

Sophisticated error handling patterns for production workflows.

### Error Recovery Strategies

```python
# Retry with exponential backoff and jitter
class RetryWithBackoff:
    def __init__(self, max_retries=3, base_delay=1.0, max_delay=60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(self.max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt < self.max_retries - 1:
                        # Calculate delay with exponential backoff and jitter
                        delay = min(
                            self.base_delay * (2 ** attempt) + random.uniform(0, 1),
                            self.max_delay
                        )
                        time.sleep(delay)
            
            raise last_exception
        
        return wrapper

@dag.node
@RetryWithBackoff(max_retries=5, base_delay=2.0)
def unreliable_api_call(endpoint: str) -> dict:
    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()
    return response.json()
```

### Circuit Breaker Pattern

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "closed"  # closed, open, half-open
    
    def call(self, func, *args, **kwargs):
        if self.state == "open":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "half-open"
            else:
                raise CircuitBreakerOpenError("Circuit breaker is open")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
            return result
        
        except self.expected_exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "open"
                raise CircuitBreakerOpenError(f"Circuit opened after {self.failure_count} failures")
            
            raise e

# Use circuit breaker
breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

@dag.node
def protected_service_call(data: dict) -> dict:
    return breaker.call(external_service.process, data)
```

### Fallback Chains

```python
# Fallback chain pattern
class FallbackChain:
    def __init__(self, *fallbacks):
        self.fallbacks = fallbacks
    
    def execute(self, *args, **kwargs):
        exceptions = []
        
        for i, fallback in enumerate(self.fallbacks):
            try:
                return fallback(*args, **kwargs)
            except Exception as e:
                exceptions.append((fallback.__name__, str(e)))
                if i < len(self.fallbacks) - 1:
                    print(f"Fallback {i+1} failed, trying next...")
        
        raise Exception(f"All fallbacks failed: {exceptions}")

# Define fallback functions
def primary_data_source(query: str) -> list:
    return database.execute(query)

def cache_data_source(query: str) -> list:
    return cache.get(f"query:{query}")

def backup_data_source(query: str) -> list:
    return backup_db.execute(query)

def static_data_source(query: str) -> list:
    return load_static_data(query)

# Create fallback chain
data_source = FallbackChain(
    primary_data_source,
    cache_data_source,
    backup_data_source,
    static_data_source
)

@dag.node
def load_data(query: str) -> list:
    return data_source.execute(query)
```

## Hooks and Middleware

Extend workflow behavior with hooks and middleware.

### Global Hooks

```python
# Define global hooks
class WorkflowHooks:
    @staticmethod
    def pre_node_execute(node: Node, inputs: dict, context: Context):
        # Log node execution
        logger.info(f"Executing {node.name} with inputs: {list(inputs.keys())}")
        
        # Add timing
        context.metadata[f"{node.name}_start_time"] = time.time()
        
        # Validate inputs
        if hasattr(node, 'input_schema'):
            validate_schema(inputs, node.input_schema)
    
    @staticmethod
    def post_node_execute(node: Node, inputs: dict, result: Any, context: Context):
        # Calculate execution time
        start_time = context.metadata.get(f"{node.name}_start_time")
        if start_time:
            execution_time = time.time() - start_time
            context.metrics.setdefault('node_times', {})[node.name] = execution_time
        
        # Validate output
        if hasattr(node, 'output_schema'):
            validate_schema(result, node.output_schema)
        
        # Send metrics
        send_metrics({
            "node": node.name,
            "execution_time": execution_time,
            "success": True
        })
    
    @staticmethod
    def on_node_error(node: Node, inputs: dict, error: Exception, context: Context):
        # Log error
        logger.error(f"Error in {node.name}: {error}", exc_info=True)
        
        # Send alert
        if isinstance(error, CriticalError):
            send_alert(f"Critical error in {node.name}: {error}")
        
        # Store error context
        context.metadata.setdefault('errors', []).append({
            "node": node.name,
            "error": str(error),
            "timestamp": time.time(),
            "inputs": list(inputs.keys())
        })

# Apply hooks to DAG
dag.set_global_hooks(
    pre_execute=WorkflowHooks.pre_node_execute,
    post_execute=WorkflowHooks.post_node_execute,
    on_error=WorkflowHooks.on_node_error
)
```

### Middleware System

```python
# Middleware base class
class Middleware:
    def process_input(self, node: Node, inputs: dict) -> dict:
        return inputs
    
    def process_output(self, node: Node, output: Any) -> Any:
        return output
    
    def handle_error(self, node: Node, error: Exception) -> Any:
        raise error

# Authentication middleware
class AuthMiddleware(Middleware):
    def process_input(self, node: Node, inputs: dict) -> dict:
        # Check if node requires auth
        if hasattr(node, 'requires_auth') and node.requires_auth:
            if 'auth_token' not in inputs:
                raise AuthenticationError("Auth token required")
            
            # Validate token
            if not validate_token(inputs['auth_token']):
                raise AuthenticationError("Invalid auth token")
        
        return inputs

# Rate limiting middleware
class RateLimitMiddleware(Middleware):
    def __init__(self, max_calls_per_minute=60):
        self.max_calls = max_calls_per_minute
        self.calls = defaultdict(list)
    
    def process_input(self, node: Node, inputs: dict) -> dict:
        now = time.time()
        minute_ago = now - 60
        
        # Clean old calls
        self.calls[node.name] = [t for t in self.calls[node.name] if t > minute_ago]
        
        # Check rate limit
        if len(self.calls[node.name]) >= self.max_calls:
            raise RateLimitExceededError(f"Rate limit exceeded for {node.name}")
        
        self.calls[node.name].append(now)
        return inputs

# Apply middleware
dag.add_middleware(AuthMiddleware())
dag.add_middleware(RateLimitMiddleware(max_calls_per_minute=100))
```

## Performance Profiling

Profile and optimize workflow performance.

### Built-in Profiling

```python
# Enable profiling
profiler = dag.enable_profiling()

# Run with profiling
result = dag.run()

# Get profiling results
profile_data = profiler.get_results()

# Analyze results
print(f"Total execution time: {profile_data['total_time']:.2f}s")
print("\nNode execution times:")
for node, time in profile_data['node_times'].items():
    print(f"  {node}: {time:.3f}s")

print("\nMemory usage:")
for node, memory in profile_data['memory_usage'].items():
    print(f"  {node}: {memory:.1f} MB")

# Find bottlenecks
bottlenecks = profiler.find_bottlenecks(threshold=0.5)  # Nodes taking >50% time
print(f"\nBottlenecks: {bottlenecks}")
```

### Custom Profiling

```python
# Advanced profiler
class AdvancedProfiler:
    def __init__(self):
        self.stats = defaultdict(dict)
    
    def profile_node(self, node: Node):
        def wrapper(func):
            def profiled_func(*args, **kwargs):
                # CPU profiling
                process = psutil.Process()
                cpu_before = process.cpu_percent()
                memory_before = process.memory_info().rss / 1024 / 1024  # MB
                
                # I/O profiling
                io_before = process.io_counters()
                
                # Time profiling
                start_time = time.perf_counter()
                
                try:
                    # Run function with cProfile
                    profiler = cProfile.Profile()
                    profiler.enable()
                    result = func(*args, **kwargs)
                    profiler.disable()
                    
                    # Store detailed stats
                    s = io.StringIO()
                    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
                    ps.print_stats(10)
                    
                    self.stats[node.name]['profile'] = s.getvalue()
                    
                    return result
                
                finally:
                    # Collect metrics
                    end_time = time.perf_counter()
                    cpu_after = process.cpu_percent()
                    memory_after = process.memory_info().rss / 1024 / 1024
                    io_after = process.io_counters()
                    
                    self.stats[node.name].update({
                        'execution_time': end_time - start_time,
                        'cpu_usage': cpu_after - cpu_before,
                        'memory_delta': memory_after - memory_before,
                        'memory_peak': memory_after,
                        'io_reads': io_after.read_count - io_before.read_count,
                        'io_writes': io_after.write_count - io_before.write_count,
                        'io_read_bytes': io_after.read_bytes - io_before.read_bytes,
                        'io_write_bytes': io_after.write_bytes - io_before.write_bytes
                    })
            
            return profiled_func
        
        return wrapper
    
    def get_report(self) -> str:
        report = []
        report.append("=== Performance Report ===\n")
        
        for node, stats in self.stats.items():
            report.append(f"\nNode: {node}")
            report.append(f"  Execution Time: {stats['execution_time']:.3f}s")
            report.append(f"  CPU Usage: {stats['cpu_usage']:.1f}%")
            report.append(f"  Memory Delta: {stats['memory_delta']:.1f} MB")
            report.append(f"  Memory Peak: {stats['memory_peak']:.1f} MB")
            report.append(f"  I/O Reads: {stats['io_reads']} ({stats['io_read_bytes'] / 1024 / 1024:.1f} MB)")
            report.append(f"  I/O Writes: {stats['io_writes']} ({stats['io_write_bytes'] / 1024 / 1024:.1f} MB)")
        
        return "\n".join(report)

# Use advanced profiler
profiler = AdvancedProfiler()

@dag.node
@profiler.profile_node(dag.nodes["process_data"])
def process_data(data: list) -> dict:
    return expensive_computation(data)
```

## Integration Patterns

Integrate fast-dag with external systems and frameworks.

### REST API Integration

```python
# Expose DAG as REST API
from fastapi import FastAPI, BackgroundTasks
from fast_dag.integrations import DAGRouter

app = FastAPI()

# Create DAG router
dag_router = DAGRouter(dag)

# Add endpoints
app.include_router(dag_router, prefix="/workflow")

# Custom endpoints
@app.post("/workflow/run")
async def run_workflow(inputs: dict, background_tasks: BackgroundTasks):
    # Run DAG in background
    task_id = str(uuid.uuid4())
    background_tasks.add_task(run_dag_async, task_id, inputs)
    return {"task_id": task_id, "status": "started"}

@app.get("/workflow/status/{task_id}")
async def get_status(task_id: str):
    return get_task_status(task_id)

async def run_dag_async(task_id: str, inputs: dict):
    try:
        result = await dag.run_async(inputs=inputs)
        store_result(task_id, result, status="completed")
    except Exception as e:
        store_result(task_id, None, status="failed", error=str(e))
```

### Message Queue Integration

```python
# Celery integration
from celery import Celery
from fast_dag.integrations import CeleryExecutor

celery_app = Celery('workflows', broker='redis://localhost:6379')

# Create Celery executor
executor = CeleryExecutor(celery_app)

# Convert DAG to Celery tasks
@dag.node
@celery_app.task
def process_async(data: dict) -> dict:
    return process_data(data)

# Run DAG with Celery
result = dag.run(executor=executor)

# Kafka integration
from kafka import KafkaProducer, KafkaConsumer

class KafkaWorkflowTrigger:
    def __init__(self, dag: DAG, topic: str):
        self.dag = dag
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
    
    def start(self):
        for message in self.consumer:
            # Run DAG for each message
            inputs = message.value
            result = self.dag.run(inputs=inputs)
            
            # Publish result
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.send(f"{self.topic}_results", result)

# Start Kafka trigger
trigger = KafkaWorkflowTrigger(dag, "workflow_inputs")
trigger.start()
```

### Database Integration

```python
# SQLAlchemy integration
from sqlalchemy import create_engine
from fast_dag.integrations import SQLAlchemyIntegration

# Setup database
engine = create_engine("postgresql://user:pass@localhost/db")
db_integration = SQLAlchemyIntegration(engine)

# Add database nodes
db_integration.add_nodes_to_dag(dag)

# Now you can use database operations
@dag.node
def load_from_db(table_name: str) -> pd.DataFrame:
    return pd.read_sql_table(table_name, engine)

@dag.node
def save_to_db(df: pd.DataFrame, table_name: str) -> None:
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Transaction support
@dag.node
@db_integration.transactional
def update_records(updates: list) -> int:
    # All operations in transaction
    count = 0
    for update in updates:
        count += engine.execute(update).rowcount
    return count
```

## Summary

fast-dag's advanced features enable:

- **Performance optimization** through caching and profiling
- **Flexibility** with custom node types and dynamic generation
- **Reliability** through advanced error handling
- **Extensibility** via hooks and middleware
- **Integration** with external systems and frameworks

These features make fast-dag suitable for production workloads requiring robustness, performance, and flexibility.

Next, check out the [API Reference](api-reference.md) for detailed documentation of all classes and methods.