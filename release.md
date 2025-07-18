# Release v0.2.0 - Major Architecture Overhaul & Feature Expansion

We're excited to announce fast-dag v0.2.0, a major release that significantly enhances the library's capabilities while improving performance and maintainability.

## 🎯 Highlights

### 🚀 New Features

- **Caching System**: Boost performance with built-in LRU and TTL-based caching for node execution results
- **Nested Workflows**: Compose complex workflows by using DAGs/FSMs as nodes within other workflows
- **High-Performance Serialization**: msgspec-based serialization for 10x faster workflow persistence
- **Enhanced Visualization**: Improved Graphviz and Mermaid diagram generation with execution state visualization

### 🏗️ Architecture Improvements

- **Modular Design**: Cleaner, more maintainable codebase with focused modules
- **Better Error Handling**: More informative error messages and validation
- **Type Safety**: Comprehensive type hints throughout the codebase
- **Performance**: Optimized execution paths and reduced memory footprint

### 📚 Documentation

- Comprehensive user guide with 10+ chapters
- API reference with practical examples
- Migration guide from v0.1.x
- Advanced features guide and troubleshooting

## 💻 Installation

```bash
# Core installation
pip install fast-dag==0.2.0

# With visualization support
pip install fast-dag[viz]==0.2.0

# With serialization support
pip install fast-dag[serialize]==0.2.0

# All features
pip install fast-dag[all]==0.2.0
```

## 🔄 Migration

For users upgrading from v0.1.x, please refer to our [migration guide](docs/migration-guide.md). The main changes involve updated import paths due to modularization.

## 📊 What's Next

- Distributed execution support
- Web UI for workflow monitoring
- Additional node types and conditions
- Performance benchmarking suite
