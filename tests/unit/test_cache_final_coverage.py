"""
Tests for cache module to achieve 100% coverage.
"""

import tempfile
import time
from unittest.mock import patch

from fast_dag.core.cache import CacheEntry, DiskCache, MemoryCache


class TestCacheFinalCoverage:
    """Test cache classes to achieve 100% coverage"""

    def test_memory_cache_has_with_ttl_expiration(self):
        """Test MemoryCache.has with TTL expiration (lines 157-158)"""
        cache = MemoryCache()

        # Create an entry with TTL
        entry = CacheEntry(key="test_key", value="test_value", ttl=0.1)
        cache._cache["test_key"] = entry

        # Initially should exist
        assert cache.exists("test_key") is True

        # Wait for TTL to expire
        time.sleep(0.2)

        # Now should be expired and deleted
        assert cache.exists("test_key") is False

        # Verify entry was actually deleted
        assert "test_key" not in cache._cache

    def test_memory_cache_has_without_ttl(self):
        """Test MemoryCache.has without TTL (should not expire)"""
        cache = MemoryCache()

        # Create an entry without TTL
        entry = CacheEntry(key="test_key", value="test_value")
        cache._cache["test_key"] = entry

        # Should exist and not expire
        assert cache.exists("test_key") is True

        # Wait a bit and check again
        time.sleep(0.1)
        assert cache.exists("test_key") is True

    def test_memory_cache_has_with_ttl_not_expired(self):
        """Test MemoryCache.has with TTL that hasn't expired"""
        cache = MemoryCache()

        # Create an entry with long TTL
        entry = CacheEntry(key="test_key", value="test_value", ttl=10.0)
        cache._cache["test_key"] = entry

        # Should exist and not be expired
        assert cache.exists("test_key") is True

        # Verify entry still exists in cache
        assert "test_key" in cache._cache

    def test_disk_cache_set_with_write_failure(self):
        """Test DiskCache.set with write failure that requires cleanup (line 257)"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = DiskCache(cache_dir=tmpdir)

            # Mock pickle.dump to raise an exception
            with (
                patch("pickle.dump", side_effect=Exception("Write failed")),
                patch("pathlib.Path.exists", return_value=True),
                patch("pathlib.Path.unlink") as mock_unlink,
            ):
                # This should trigger the exception handling and cleanup
                cache.set("test_key", "test_value")

                # Verify cleanup was attempted
                mock_unlink.assert_called_once()

    def test_disk_cache_set_with_write_failure_no_cleanup(self):
        """Test DiskCache.set with write failure where file doesn't exist"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = DiskCache(cache_dir=tmpdir)

            # Mock pickle.dump to raise an exception
            with (
                patch("pickle.dump", side_effect=Exception("Write failed")),
                patch("pathlib.Path.exists", return_value=False),
                patch("pathlib.Path.unlink") as mock_unlink,
            ):
                # This should trigger the exception handling without cleanup
                cache.set("test_key", "test_value")

                # Verify cleanup was not attempted
                mock_unlink.assert_not_called()

    def test_disk_cache_set_successful(self):
        """Test successful DiskCache.set operation"""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache = DiskCache(cache_dir=tmpdir)

            # This should work normally
            cache.set("test_key", "test_value")

            # Verify the value was actually stored
            assert cache.exists("test_key") is True
            assert cache.get("test_key") == "test_value"

    def test_memory_cache_ttl_edge_case(self):
        """Test TTL edge case where entry has ttl attribute but it's None"""
        cache = MemoryCache()

        # Create an entry with ttl attribute set to None
        entry = CacheEntry(key="test_key", value="test_value", ttl=None)
        cache._cache["test_key"] = entry

        # Should exist and not be considered expired
        assert cache.exists("test_key") is True

    def test_memory_cache_entry_without_ttl_attribute(self):
        """Test entry without ttl attribute (older format)"""
        cache = MemoryCache()

        # Create an entry and remove the ttl attribute
        entry = CacheEntry(key="test_key", value="test_value")
        delattr(entry, "ttl")
        cache._cache["test_key"] = entry

        # Should exist and not be considered expired
        assert cache.exists("test_key") is True

    def test_cache_entry_post_init_size_calculation(self):
        """Test CacheEntry size calculation in __post_init__"""
        # Test with size 0 (should calculate)
        entry = CacheEntry(key="test", value="test_value", size=0)
        assert entry.size > 0

        # Test with size already set (should not recalculate)
        entry = CacheEntry(key="test", value="test_value", size=100)
        assert entry.size == 100

    def test_cache_entry_post_init_size_calculation_exception(self):
        """Test CacheEntry size calculation with exception"""

        # Create a value that can't be pickled
        class UnpicklableValue:
            def __reduce__(self):
                raise Exception("Cannot pickle")

        entry = CacheEntry(key="test", value=UnpicklableValue(), size=0)
        # Should default to 0 when pickle fails
        assert entry.size == 0
