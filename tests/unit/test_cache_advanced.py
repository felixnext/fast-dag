"""Advanced unit tests for caching functionality."""

import time
from unittest.mock import patch

import pytest

from fast_dag.core.cache import (
    CacheEntry,
    DiskCache,
    MemoryCache,
    generate_cache_key,
    get_cache,
    register_cache,
)


class TestCacheEntry:
    """Test CacheEntry functionality."""

    def test_cache_entry_creation(self):
        """Test creating cache entries."""
        entry = CacheEntry(key="test", value={"data": [1, 2, 3]})

        assert entry.key == "test"
        assert entry.value == {"data": [1, 2, 3]}
        assert entry.hits == 0
        assert entry.size > 0  # Should calculate size
        assert entry.timestamp > 0

    def test_cache_entry_with_ttl(self):
        """Test cache entry with TTL."""
        entry = CacheEntry(key="ttl_test", value="data", ttl=60.0)

        assert entry.ttl == 60.0

    def test_cache_entry_size_calculation(self):
        """Test automatic size calculation."""
        small_entry = CacheEntry(key="small", value="x")
        large_entry = CacheEntry(key="large", value="x" * 1000)

        assert large_entry.size > small_entry.size

    def test_cache_entry_unpickleable_value(self):
        """Test cache entry with unpickleable value."""
        # Lambda functions can't be pickled
        entry = CacheEntry(key="lambda", value=lambda x: x)

        # Should handle gracefully
        assert entry.size == 0


class TestMemoryCache:
    """Test MemoryCache advanced scenarios."""

    def test_memory_cache_eviction_by_count(self):
        """Test LRU eviction when max size reached."""
        cache = MemoryCache(max_size=3)

        # Add 4 items
        cache.set("a", "value_a")
        cache.set("b", "value_b")
        cache.set("c", "value_c")
        cache.set("d", "value_d")  # Should evict 'a'

        # 'a' should be evicted
        assert cache.get("a") is None
        assert cache.get("b") == "value_b"
        assert cache.get("c") == "value_c"
        assert cache.get("d") == "value_d"

    def test_memory_cache_eviction_by_memory(self):
        """Test eviction when memory limit reached."""
        # Set very small memory limit
        cache = MemoryCache(max_size=100, max_memory_mb=0.0001)

        # Add large values
        cache.set("large1", "x" * 1000)
        cache.set("large2", "y" * 1000)

        # Should have evicted first item
        stats = cache.stats()
        assert stats["size"] <= 1  # Should keep only one

    def test_memory_cache_lru_ordering(self):
        """Test LRU ordering is maintained."""
        cache = MemoryCache(max_size=3)

        # Add items
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)

        # Access 'a' to make it recently used
        assert cache.get("a") == 1

        # Add new item - should evict 'b' not 'a'
        cache.set("d", 4)

        assert cache.get("a") == 1  # Still there
        assert cache.get("b") is None  # Evicted
        assert cache.get("c") == 3
        assert cache.get("d") == 4

    def test_memory_cache_ttl_expiration(self):
        """Test TTL expiration."""
        cache = MemoryCache()

        # Set with very short TTL
        cache.set("expire", "value", ttl=0.1)

        # Should exist immediately
        assert cache.get("expire") == "value"
        assert cache.exists("expire")

        # Wait for expiration
        time.sleep(0.2)

        # Should be expired
        assert cache.get("expire") is None
        assert not cache.exists("expire")

    def test_memory_cache_stats(self):
        """Test cache statistics."""
        cache = MemoryCache()

        # Initial stats
        stats = cache.stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["hit_rate"] == 0

        # Add and access items
        cache.set("key1", "value1")
        cache.get("key1")  # Hit
        cache.get("key2")  # Miss

        stats = cache.stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 0.5

    def test_memory_cache_clear(self):
        """Test clearing cache."""
        cache = MemoryCache()

        # Add items
        cache.set("a", 1)
        cache.set("b", 2)

        # Clear
        cache.clear()

        # Should be empty
        assert cache.get("a") is None
        assert cache.get("b") is None
        stats = cache.stats()
        assert stats["size"] == 0
        assert stats["hits"] == 0
        assert stats["misses"] == 2  # Two misses from the gets above

    def test_memory_cache_update_existing(self):
        """Test updating existing entries."""
        cache = MemoryCache()

        # Set initial value
        cache.set("key", "value1")
        entry1_size = cache._total_size

        # Update with larger value
        cache.set("key", "value2_much_longer")

        # Size should be updated correctly
        assert cache._total_size != entry1_size
        assert cache.get("key") == "value2_much_longer"


class TestDiskCache:
    """Test DiskCache advanced scenarios."""

    def test_disk_cache_directory_creation(self, tmp_path):
        """Test cache directory creation."""
        cache_dir = tmp_path / "test_cache"
        DiskCache(cache_dir)

        assert cache_dir.exists()
        assert cache_dir.is_dir()

    def test_disk_cache_subdirectory_structure(self, tmp_path):
        """Test subdirectory structure for cache files."""
        cache = DiskCache(tmp_path)

        cache.set("test_key", "test_value")

        # Should create subdirectory based on hash
        subdirs = list(tmp_path.iterdir())
        assert len(subdirs) == 1
        assert len(subdirs[0].name) == 2  # Two-char subdir

    def test_disk_cache_ttl_expiration(self, tmp_path):
        """Test TTL expiration for disk cache."""
        cache = DiskCache(tmp_path)

        # Set with short TTL
        cache.set("expire", "value", ttl=0.1)

        # Should exist
        assert cache.get("expire") == "value"

        # Wait for expiration
        time.sleep(0.2)

        # Should be expired and file deleted
        assert cache.get("expire") is None

        # Verify file was deleted
        path = cache._get_path("expire")
        assert not path.exists()

    def test_disk_cache_corrupted_file(self, tmp_path):
        """Test handling corrupted cache files."""
        cache = DiskCache(tmp_path)

        # Set valid entry
        cache.set("key", "value")

        # Corrupt the file
        path = cache._get_path("key")
        path.write_text("corrupted data")

        # Should handle gracefully
        assert cache.get("key") is None

        # File should be deleted
        assert not path.exists()

    def test_disk_cache_write_error(self, tmp_path):
        """Test handling write errors."""
        cache = DiskCache(tmp_path)

        # Mock file write to fail
        with patch("builtins.open", side_effect=OSError("Write failed")):
            cache.set("key", "value")

        # Should handle gracefully
        assert cache.get("key") is None

    def test_disk_cache_stats(self, tmp_path):
        """Test disk cache statistics."""
        cache = DiskCache(tmp_path)

        # Add items
        cache.set("key1", "value1")
        cache.set("key2", "value2" * 100)

        # Get stats
        stats = cache.stats()
        assert stats["type"] == "disk"
        assert stats["file_count"] == 2
        assert stats["total_size_bytes"] > 0
        assert str(tmp_path) in stats["cache_dir"]

    def test_disk_cache_clear(self, tmp_path):
        """Test clearing disk cache."""
        cache = DiskCache(tmp_path)

        # Add items
        cache.set("key1", "value1")
        cache.set("key2", "value2")

        # Clear
        cache.clear()

        # Should be empty
        assert cache.get("key1") is None
        assert cache.get("key2") is None

        # No cache files should exist
        pkl_files = list(tmp_path.rglob("*.pkl"))
        assert len(pkl_files) == 0

    def test_disk_cache_delete(self, tmp_path):
        """Test deleting specific entries."""
        cache = DiskCache(tmp_path)

        cache.set("keep", "value1")
        cache.set("delete", "value2")

        # Delete one
        assert cache.delete("delete") is True
        assert cache.delete("nonexistent") is False

        # Verify
        assert cache.get("keep") == "value1"
        assert cache.get("delete") is None

    def test_disk_cache_exists_with_expiry(self, tmp_path):
        """Test exists method with TTL."""
        cache = DiskCache(tmp_path)

        cache.set("expire", "value", ttl=0.1)

        # Should exist
        assert cache.exists("expire")

        # Wait for expiration
        time.sleep(0.2)

        # Should not exist
        assert not cache.exists("expire")


class TestCacheKeyGeneration:
    """Test cache key generation."""

    def test_generate_cache_key_simple(self):
        """Test key generation with simple types."""
        key1 = generate_cache_key("func", {"a": 1, "b": "test"})
        key2 = generate_cache_key("func", {"b": "test", "a": 1})

        # Should be same regardless of order
        assert key1 == key2

        # Different inputs should give different keys
        key3 = generate_cache_key("func", {"a": 2, "b": "test"})
        assert key1 != key3

    def test_generate_cache_key_complex_types(self):
        """Test key generation with complex types."""
        key1 = generate_cache_key(
            "func", {"list": [1, 2, 3], "dict": {"nested": "value"}, "tuple": (4, 5, 6)}
        )

        # Same data should give same key
        key2 = generate_cache_key(
            "func", {"list": [1, 2, 3], "dict": {"nested": "value"}, "tuple": (4, 5, 6)}
        )
        assert key1 == key2

    def test_generate_cache_key_special_inputs(self):
        """Test key generation skips special inputs."""
        key1 = generate_cache_key(
            "func",
            {"data": "value", "context": "should_ignore", "_cache": "should_ignore"},
        )

        key2 = generate_cache_key("func", {"data": "value"})

        # Should be same (special inputs ignored)
        assert key1 == key2

    def test_generate_cache_key_unpickleable(self):
        """Test key generation with unpickleable objects."""
        # Lambda can't be pickled, should use string repr
        key = generate_cache_key("func", {"fn": lambda x: x})

        # Should not raise error
        assert isinstance(key, str)
        assert len(key) == 64  # SHA256 hex length

    def test_generate_cache_key_none_values(self):
        """Test key generation with None values."""
        key1 = generate_cache_key("func", {"a": None, "b": 1})
        key2 = generate_cache_key("func", {"a": None, "b": 2})

        assert key1 != key2


class TestCacheRegistry:
    """Test cache registry functionality."""

    def test_get_default_cache(self):
        """Test getting default memory cache."""
        cache = get_cache()
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

        class CustomCache(MemoryCache):
            pass

        custom = CustomCache()
        register_cache("custom", custom)

        retrieved = get_cache("custom")
        assert retrieved is custom

    def test_get_unknown_cache(self):
        """Test getting unknown cache type."""
        with pytest.raises(ValueError, match="Unknown cache backend"):
            get_cache("nonexistent")


class TestCacheIntegration:
    """Test cache integration scenarios."""

    def test_concurrent_cache_access(self):
        """Test concurrent access to cache."""
        import threading

        cache = MemoryCache()
        results = []

        def access_cache(key, value):
            cache.set(key, value)
            time.sleep(0.01)  # Small delay
            result = cache.get(key)
            results.append(result == value)

        # Create threads
        threads = []
        for i in range(10):
            t = threading.Thread(target=access_cache, args=(f"key{i}", f"value{i}"))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        # All accesses should succeed
        assert all(results)

    def test_cache_memory_pressure(self):
        """Test cache under memory pressure."""
        # Very small cache
        cache = MemoryCache(max_size=10, max_memory_mb=0.001)

        # Add many items
        for i in range(100):
            cache.set(f"key{i}", f"value{i}" * 100)

        # Should not exceed limits
        stats = cache.stats()
        assert stats["size"] <= 10
        assert stats["memory_bytes"] <= 1024  # 0.001 MB
