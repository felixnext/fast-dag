"""Unit tests for caching functionality."""

import time
from unittest.mock import Mock

from fast_dag import DAG
from fast_dag.core.cache import (
    DiskCache,
    MemoryCache,
    generate_cache_key,
    get_cache,
    register_cache,
)


class TestCacheKey:
    """Test cache key generation."""

    def test_generate_cache_key_simple(self):
        """Test generating cache key with simple inputs."""
        key1 = generate_cache_key("my_func", {"a": 1, "b": 2})
        key2 = generate_cache_key("my_func", {"b": 2, "a": 1})

        # Should be the same regardless of order
        assert key1 == key2

        # Different inputs should give different keys
        key3 = generate_cache_key("my_func", {"a": 1, "b": 3})
        assert key1 != key3

    def test_generate_cache_key_complex(self):
        """Test cache key with complex inputs."""
        key1 = generate_cache_key(
            "func",
            {
                "list": [1, 2, 3],
                "dict": {"x": 1, "y": 2},
                "string": "hello",
            },
        )

        # Same values should give same key
        key2 = generate_cache_key(
            "func",
            {
                "string": "hello",
                "list": [1, 2, 3],
                "dict": {"y": 2, "x": 1},
            },
        )
        assert key1 == key2

    def test_generate_cache_key_ignores_context(self):
        """Test that context is ignored in cache key."""
        key1 = generate_cache_key("func", {"a": 1, "context": "ignored"})
        key2 = generate_cache_key("func", {"a": 1})
        assert key1 == key2


class TestMemoryCache:
    """Test in-memory cache backend."""

    def test_basic_operations(self):
        """Test basic cache operations."""
        cache = MemoryCache(max_size=10)

        # Set and get
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

        # Non-existent key
        assert cache.get("key2") is None

        # Delete
        assert cache.delete("key1") is True
        assert cache.get("key1") is None
        assert cache.delete("key1") is False

    def test_lru_eviction(self):
        """Test LRU eviction policy."""
        cache = MemoryCache(max_size=3)

        # Fill cache
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        # Access 'a' to make it recently used
        cache.get("a")

        # Add new item, should evict 'b' (least recently used)
        cache.set("d", 4)

        assert cache.get("a") == 1  # Still there
        assert cache.get("b") is None  # Evicted
        assert cache.get("c") == 3  # Still there
        assert cache.get("d") == 4  # New item

    def test_ttl_expiration(self):
        """Test TTL expiration."""
        cache = MemoryCache()

        # Set with short TTL
        cache.set("key", "value", ttl=0.1)
        assert cache.get("key") == "value"

        # Wait for expiration
        time.sleep(0.15)
        assert cache.get("key") is None

    def test_memory_limit(self):
        """Test memory limit eviction."""
        cache = MemoryCache(max_size=100, max_memory_mb=0.001)  # 1KB limit

        # Add large values
        large_value = "x" * 500  # ~500 bytes
        cache.set("a", large_value)
        cache.set("b", large_value)
        cache.set("c", large_value)

        # Should have evicted some entries
        assert len([k for k in ["a", "b", "c"] if cache.get(k) is not None]) < 3

    def test_cache_stats(self):
        """Test cache statistics."""
        cache = MemoryCache()

        # Generate some hits and misses
        cache.set("key", "value")
        cache.get("key")  # Hit
        cache.get("key")  # Hit
        cache.get("missing")  # Miss

        stats = cache.stats()
        assert stats["hits"] == 2
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 2 / 3


class TestDiskCache:
    """Test disk-based cache backend."""

    def test_basic_operations(self, tmp_path):
        """Test basic disk cache operations."""
        cache = DiskCache(cache_dir=tmp_path / "cache")

        # Set and get
        cache.set("key1", {"data": [1, 2, 3]})
        result = cache.get("key1")
        assert result == {"data": [1, 2, 3]}

        # Delete
        assert cache.delete("key1") is True
        assert cache.get("key1") is None

    def test_persistence(self, tmp_path):
        """Test that disk cache persists across instances."""
        cache_dir = tmp_path / "cache"

        # First instance
        cache1 = DiskCache(cache_dir=cache_dir)
        cache1.set("key", "value")

        # Second instance should see the data
        cache2 = DiskCache(cache_dir=cache_dir)
        assert cache2.get("key") == "value"

    def test_clear(self, tmp_path):
        """Test clearing disk cache."""
        cache = DiskCache(cache_dir=tmp_path / "cache")

        # Add multiple items
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        # Clear all
        cache.clear()

        assert cache.get("a") is None
        assert cache.get("b") is None
        assert cache.get("c") is None


class TestCacheRegistry:
    """Test cache registry functionality."""

    def test_get_default_cache(self):
        """Test getting default memory cache."""
        cache = get_cache("memory")
        assert isinstance(cache, MemoryCache)

        # Should return same instance
        cache2 = get_cache("memory")
        assert cache is cache2

    def test_get_disk_cache(self):
        """Test getting disk cache."""
        cache = get_cache("disk")
        assert isinstance(cache, DiskCache)

    def test_register_custom_cache(self):
        """Test registering custom cache backend."""
        mock_cache = Mock()
        register_cache("custom", mock_cache)

        assert get_cache("custom") is mock_cache


class TestNodeCaching:
    """Test caching integration with nodes."""

    def test_cached_node_decorator(self):
        """Test the cached_node decorator."""
        dag = DAG("test")

        call_count = 0

        @dag.cached_node
        def expensive_computation(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        # First call should execute
        result1 = dag.run(x=5)
        assert result1 == 10
        assert call_count == 1

        # Second call with same input should use cache
        result2 = dag.run(x=5)
        assert result2 == 10
        assert call_count == 1  # Not incremented

        # Different input should execute again
        result3 = dag.run(x=6)
        assert result3 == 12
        assert call_count == 2

    def test_cached_node_with_ttl(self):
        """Test cached node with TTL."""
        dag = DAG("test")

        call_count = 0

        @dag.cached_node(cache_ttl=0.1)
        def time_sensitive(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x + call_count

        # First call
        result1 = dag.run(x=1)
        assert call_count == 1

        # Immediate second call uses cache
        result2 = dag.run(x=1)
        assert result2 == result1
        assert call_count == 1

        # After TTL expires
        time.sleep(0.15)
        dag.run(x=1)
        assert call_count == 2

    def test_cache_metrics_in_context(self):
        """Test that cache metrics are recorded in context."""
        dag = DAG("test")

        @dag.cached_node
        def cached_func(x: int) -> int:
            return x * 2

        # First run - cache miss
        dag.run(x=5)
        assert "cache_misses" in dag.context.metrics
        assert dag.context.metrics["cache_misses"]["cached_func"] == 1

        # Second run - cache hit
        dag.run(x=5)
        assert "cache_hits" in dag.context.metrics
        assert dag.context.metrics["cache_hits"]["cached_func"] == 1

    def test_cached_node_with_multiple_inputs(self):
        """Test caching with multiple inputs."""
        dag = DAG("test")

        @dag.cached_node
        def add(a: int, b: int) -> int:
            return a + b

        @dag.cached_node
        def multiply(x: int, y: int) -> int:
            return x * y

        dag.connect("add", "multiply", output="result", input="x")

        # Run pipeline
        result1 = dag.run(a=2, b=3, y=4)
        assert result1 == 20  # (2 + 3) * 4

        # Check cache usage on second run
        result2 = dag.run(a=2, b=3, y=4)
        assert result2 == 20

        # Both nodes should have cache hits
        assert dag.context.metrics["cache_hits"]["add"] == 1
        assert dag.context.metrics["cache_hits"]["multiply"] == 1
