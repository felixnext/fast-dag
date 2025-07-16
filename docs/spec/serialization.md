# Serialization

This document specifies the serialization strategy for fast-dag, with msgspec as the primary implementation and a design that allows future migration to pydantic or other libraries.

## Design Principles

1. **Performance First**: msgspec for minimal serialization overhead
2. **Pluggable Architecture**: Easy to swap serialization backends
3. **Type Safety**: Full type preservation during round-trips
4. **Human Readable**: Support both binary and text formats
5. **Backward Compatible**: Versioning for schema evolution

## Architecture

### Serialization Protocol

```python
from typing import Protocol, TypeVar, Any

T = TypeVar('T')

class Serializer(Protocol):
    """Protocol for serialization implementations"""
    
    def encode(self, obj: Any) -> bytes:
        """Encode object to bytes"""
        ...
    
    def decode(self, data: bytes, type_: type[T]) -> T:
        """Decode bytes to typed object"""
        ...
    
    def encode_json(self, obj: Any) -> str:
        """Encode object to JSON string"""
        ...
    
    def decode_json(self, data: str, type_: type[T]) -> T:
        """Decode JSON string to typed object"""
        ...
```

### Serializable Types

All core types implement serialization through dataclass transforms:

```python
from dataclasses import dataclass
from typing import Any
import msgspec

# Make dataclasses msgspec-compatible
@dataclass
class SerializableNode(msgspec.Struct):
    """Node representation for serialization"""
    name: str
    func_name: str
    inputs: list[str]
    outputs: list[str]
    description: str | None = None
    node_type: str = "standard"
    metadata: dict[str, Any] = msgspec.field(default_factory=dict)

@dataclass
class SerializableDAG(msgspec.Struct):
    """DAG representation for serialization"""
    name: str
    nodes: dict[str, SerializableNode]
    connections: list[tuple[str, str, str, str]]  # from, to, output, input
    metadata: dict[str, Any] = msgspec.field(default_factory=dict)
```

## msgspec Implementation

### Core Serializer

```python
import msgspec
from typing import Any, TypeVar

T = TypeVar('T')

class MsgspecSerializer:
    """High-performance msgspec-based serialization"""
    
    def __init__(self):
        # Binary format (MessagePack)
        self.msgpack_encoder = msgspec.msgpack.Encoder()
        self.msgpack_decoder = msgspec.msgpack.Decoder()
        
        # JSON format
        self.json_encoder = msgspec.json.Encoder()
        self.json_decoder = msgspec.json.Decoder()
    
    def encode(self, obj: Any) -> bytes:
        """Encode to MessagePack bytes"""
        return self.msgpack_encoder.encode(obj)
    
    def decode(self, data: bytes, type_: type[T]) -> T:
        """Decode from MessagePack bytes"""
        decoder = msgspec.msgpack.Decoder(type_)
        return decoder.decode(data)
    
    def encode_json(self, obj: Any) -> str:
        """Encode to JSON string"""
        return self.json_encoder.encode(obj).decode('utf-8')
    
    def decode_json(self, data: str, type_: type[T]) -> T:
        """Decode from JSON string"""
        decoder = msgspec.json.Decoder(type_)
        return decoder.decode(data)
```

### Type Conversion

Convert between runtime objects and serializable representations:

```python
class TypeConverter:
    """Convert between runtime and serializable types"""
    
    def node_to_serializable(self, node: Node) -> SerializableNode:
        """Convert Node to serializable format"""
        return SerializableNode(
            name=node.name,
            func_name=node.func.__name__,
            inputs=node.inputs,
            outputs=node.outputs,
            description=node.description,
            node_type=node.node_type.value,
            metadata=node.metadata
        )
    
    def serializable_to_node(
        self,
        data: SerializableNode,
        func_registry: dict[str, Callable]
    ) -> Node:
        """Convert serializable format to Node"""
        func = func_registry.get(data.func_name)
        if not func:
            raise ValueError(f"Function '{data.func_name}' not in registry")
        
        return Node(
            func=func,
            name=data.name,
            inputs=data.inputs,
            outputs=data.outputs,
            description=data.description,
            node_type=NodeType(data.node_type)
        )
```

## Workflow Serialization

### YAML Format

Human-readable workflow definitions:

```yaml
name: data_pipeline
type: DAG
metadata:
  version: "1.0"
  author: "user@example.com"

nodes:
  - name: load_data
    func: load_data_from_file
    inputs: [file_path]
    outputs: [data]
    description: "Load data from CSV file"
    
  - name: validate
    func: validate_data
    inputs: [data]
    outputs: [validated_data]
    node_type: conditional
    
  - name: process
    func: process_data
    inputs: [validated_data]
    outputs: [result]

connections:
  - from: load_data
    to: validate
    output: data
    input: data
    
  - from: validate
    to: process
    output: validated_data
    input: validated_data
    condition: true  # For conditional nodes
```

### JSON Format

Programmatic workflow definitions:

```json
{
  "name": "data_pipeline",
  "type": "DAG",
  "nodes": {
    "load_data": {
      "func_name": "load_data_from_file",
      "inputs": ["file_path"],
      "outputs": ["data"],
      "node_type": "standard"
    }
  },
  "connections": [
    {
      "from": "load_data",
      "to": "validate",
      "output": "data",
      "input": "data"
    }
  ]
}
```

### Binary Format

For performance-critical applications:

```python
# Serialize to MessagePack
serializer = MsgspecSerializer()
workflow_data = serializer.encode(serializable_dag)

# Save to file
with open("workflow.msgpack", "wb") as f:
    f.write(workflow_data)

# Load and deserialize
with open("workflow.msgpack", "rb") as f:
    data = f.read()
    
dag = serializer.decode(data, SerializableDAG)
```

## Function Registry

Since functions cannot be directly serialized, we use a registry pattern:

```python
class FunctionRegistry:
    """Registry for workflow functions"""
    
    def __init__(self):
        self._functions: dict[str, Callable] = {}
        self._modules: dict[str, Any] = {}
    
    def register(
        self,
        func: Callable,
        name: str | None = None
    ) -> None:
        """Register a function"""
        func_name = name or func.__name__
        self._functions[func_name] = func
    
    def register_module(
        self,
        module: Any,
        prefix: str = ""
    ) -> None:
        """Register all functions from a module"""
        import inspect
        
        for name, obj in inspect.getmembers(module):
            if inspect.isfunction(obj):
                full_name = f"{prefix}{name}" if prefix else name
                self.register(obj, full_name)
    
    def get(self, name: str) -> Callable | None:
        """Get function by name"""
        return self._functions.get(name)
    
    def auto_discover(self, package: str) -> None:
        """Auto-discover functions in a package"""
        import importlib
        import pkgutil
        
        module = importlib.import_module(package)
        for _, name, _ in pkgutil.iter_modules(module.__path__):
            submodule = importlib.import_module(f"{package}.{name}")
            self.register_module(submodule, prefix=f"{name}.")
```

### Usage Example

```python
# Define functions
def load_data(file_path: str) -> dict:
    return {"data": "..."}

def process_data(data: dict) -> dict:
    return {"processed": data}

# Create registry
registry = FunctionRegistry()
registry.register(load_data)
registry.register(process_data)

# Or auto-discover
registry.auto_discover("my_workflow_functions")

# Use with deserialization
converter = TypeConverter()
dag = converter.deserialize_dag(data, registry)
```

## Context Serialization

Contexts require special handling for results:

```python
@dataclass
class SerializableContext(msgspec.Struct):
    """Context representation for serialization"""
    results: dict[str, Any]
    metadata: dict[str, Any]
    metrics: dict[str, float]
    
    # For large results, store references
    result_refs: dict[str, str] = msgspec.field(default_factory=dict)

class ContextSerializer:
    """Handle context serialization with large data"""
    
    def __init__(self, storage_backend: StorageBackend):
        self.storage = storage_backend
        self.size_threshold = 10_000_000  # 10MB
    
    def serialize(self, context: Context) -> SerializableContext:
        """Serialize context, offloading large results"""
        results = {}
        refs = {}
        
        for key, value in context.results.items():
            size = sys.getsizeof(value)
            if size > self.size_threshold:
                # Store large result externally
                ref = self.storage.store(key, value)
                refs[key] = ref
            else:
                results[key] = value
        
        return SerializableContext(
            results=results,
            metadata=context.metadata,
            metrics=context.metrics,
            result_refs=refs
        )
```

## Schema Evolution

Handle version changes gracefully:

```python
class SchemaVersion:
    """Track and migrate schema versions"""
    CURRENT = "1.0"
    
    @staticmethod
    def migrate(data: dict, from_version: str) -> dict:
        """Migrate data from old schema version"""
        if from_version == "0.9":
            # Example migration
            data = SchemaVersion._migrate_0_9_to_1_0(data)
        
        return data
    
    @staticmethod
    def _migrate_0_9_to_1_0(data: dict) -> dict:
        """Specific migration logic"""
        # Rename fields, add defaults, etc.
        if "node_list" in data:
            data["nodes"] = {n["name"]: n for n in data.pop("node_list")}
        return data
```

## Future Compatibility

### Pydantic Adapter

Design allows easy addition of pydantic support:

```python
class PydanticSerializer:
    """Future pydantic-based serialization"""
    
    def encode(self, obj: Any) -> bytes:
        # Convert to pydantic model
        model = self._to_pydantic_model(obj)
        return model.model_dump_json().encode()
    
    def decode(self, data: bytes, type_: type[T]) -> T:
        # Parse with pydantic
        model_class = self._get_pydantic_class(type_)
        model = model_class.model_validate_json(data)
        return self._from_pydantic_model(model)
```

### Serializer Factory

```python
def get_serializer(backend: str = "msgspec") -> Serializer:
    """Get serializer by backend name"""
    if backend == "msgspec":
        return MsgspecSerializer()
    elif backend == "pydantic":
        return PydanticSerializer()
    else:
        raise ValueError(f"Unknown backend: {backend}")
```

## Performance Considerations

1. **msgspec Performance**: 10-50x faster than json/pickle
2. **Lazy Loading**: Don't deserialize until needed
3. **Streaming**: Support for large workflows
4. **Compression**: Optional zstd compression
5. **Caching**: Cache deserialized objects

## Security

1. **No Code Execution**: Only serialize data, not code
2. **Function Whitelist**: Registry controls allowed functions
3. **Input Validation**: Validate all deserialized data
4. **Sandboxing**: Optional sandbox for untrusted workflows

## Usage Examples

### Save Workflow

```python
# Create and save workflow
dag = DAG("pipeline")
# ... build workflow

# Serialize
serializer = get_serializer("msgspec")
converter = TypeConverter()
serializable = converter.dag_to_serializable(dag)

# Save as JSON
with open("workflow.json", "w") as f:
    f.write(serializer.encode_json(serializable))

# Save as MessagePack
with open("workflow.msgpack", "wb") as f:
    f.write(serializer.encode(serializable))
```

### Load Workflow

```python
# Load workflow
registry = FunctionRegistry()
registry.auto_discover("my_functions")

# From JSON
with open("workflow.json", "r") as f:
    data = serializer.decode_json(f.read(), SerializableDAG)
    dag = converter.serializable_to_dag(data, registry)

# From MessagePack
with open("workflow.msgpack", "rb") as f:
    data = serializer.decode(f.read(), SerializableDAG)
    dag = converter.serializable_to_dag(data, registry)
```