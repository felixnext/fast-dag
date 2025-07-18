"""Caching support for fast-dag nodes."""

import hashlib
import json
import pickle
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CacheEntry:
    """A single cache entry."""

    key: str
    value: Any
    timestamp: float = field(default_factory=time.time)
    hits: int = 0
    size: int = 0
    ttl: float | None = None

    def __post_init__(self):
        """Calculate size if not provided."""
        if self.size == 0:
            try:
                self.size = len(pickle.dumps(self.value))
            except Exception:
                self.size = 0


class CacheBackend(ABC):
    """Abstract base class for cache backends."""

    @abstractmethod
    def get(self, key: str) -> Any | None:
        """Get a value from the cache."""
        ...

    @abstractmethod
    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Set a value in the cache."""
        ...

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a value from the cache."""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Clear all values from the cache."""
        ...

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        ...

    @abstractmethod
    def stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        ...


class MemoryCache(CacheBackend):
    """In-memory cache backend with LRU eviction."""

    def __init__(self, max_size: int = 1000, max_memory_mb: float = 100):
        """Initialize memory cache.

        Args:
            max_size: Maximum number of entries
            max_memory_mb: Maximum memory usage in MB
        """
        self.max_size = max_size
        self.max_memory_bytes = int(max_memory_mb * 1024 * 1024)
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._total_size = 0
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Any | None:
        """Get a value from the cache."""
        if key in self._cache:
            # Move to end (most recently used)
            entry = self._cache.pop(key)
            self._cache[key] = entry

            # Check TTL
            if (
                hasattr(entry, "ttl")
                and entry.ttl is not None
                and time.time() > entry.timestamp + entry.ttl
            ):
                # Expired
                self.delete(key)
                self._misses += 1
                return None

            entry.hits += 1
            self._hits += 1
            return entry.value

        self._misses += 1
        return None

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Set a value in the cache."""
        # Create entry
        entry = CacheEntry(key=key, value=value)
        if ttl is not None:
            entry.ttl = ttl

        # Remove old entry if exists
        if key in self._cache:
            old_entry = self._cache[key]
            self._total_size -= old_entry.size

        # Add new entry
        self._cache[key] = entry
        self._total_size += entry.size

        # Evict if necessary
        self._evict_if_needed()

    def delete(self, key: str) -> bool:
        """Delete a value from the cache."""
        if key in self._cache:
            entry = self._cache.pop(key)
            self._total_size -= entry.size
            return True
        return False

    def clear(self) -> None:
        """Clear all values from the cache."""
        self._cache.clear()
        self._total_size = 0
        self._hits = 0
        self._misses = 0

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        if key not in self._cache:
            return False

        # Check TTL
        entry = self._cache[key]
        if (
            hasattr(entry, "ttl")
            and entry.ttl is not None
            and time.time() > entry.timestamp + entry.ttl
        ):
            # Expired
            self.delete(key)
            return False

        return True

    def stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._hits + self._misses
        hit_rate = self._hits / total_requests if total_requests > 0 else 0

        return {
            "type": "memory",
            "size": len(self._cache),
            "max_size": self.max_size,
            "memory_bytes": self._total_size,
            "max_memory_bytes": self.max_memory_bytes,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
        }

    def _evict_if_needed(self) -> None:
        """Evict entries if cache is too large."""
        # Evict by count
        while len(self._cache) > self.max_size:
            # Remove least recently used (first item)
            key, entry = self._cache.popitem(last=False)
            self._total_size -= entry.size

        # Evict by memory
        while self._total_size > self.max_memory_bytes and self._cache:
            # Remove least recently used
            key, entry = self._cache.popitem(last=False)
            self._total_size -= entry.size


class DiskCache(CacheBackend):
    """Disk-based cache backend."""

    def __init__(self, cache_dir: str | Path = ".cache/fast_dag"):
        """Initialize disk cache.

        Args:
            cache_dir: Directory to store cache files
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._hits = 0
        self._misses = 0

    def _get_path(self, key: str) -> Path:
        """Get the file path for a cache key."""
        # Use first 2 chars of hash for subdirectory
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        subdir = self.cache_dir / key_hash[:2]
        subdir.mkdir(exist_ok=True)
        return subdir / f"{key_hash}.pkl"

    def get(self, key: str) -> Any | None:
        """Get a value from the cache."""
        path = self._get_path(key)

        if path.exists():
            try:
                with open(path, "rb") as f:
                    entry = pickle.load(f)

                # Check TTL
                if (
                    hasattr(entry, "ttl")
                    and entry.ttl is not None
                    and time.time() > entry.timestamp + entry.ttl
                ):
                    # Expired
                    path.unlink()
                    self._misses += 1
                    return None

                self._hits += 1
                return entry.value
            except Exception:
                # Corrupted cache file
                path.unlink()

        self._misses += 1
        return None

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """Set a value in the cache."""
        path = self._get_path(key)
        entry = CacheEntry(key=key, value=value)
        if ttl is not None:
            entry.ttl = ttl

        try:
            with open(path, "wb") as f:
                pickle.dump(entry, f)
        except Exception:
            # Failed to write cache
            if path.exists():
                path.unlink()

    def delete(self, key: str) -> bool:
        """Delete a value from the cache."""
        path = self._get_path(key)
        if path.exists():
            path.unlink()
            return True
        return False

    def clear(self) -> None:
        """Clear all values from the cache."""
        for path in self.cache_dir.rglob("*.pkl"):
            path.unlink()
        self._hits = 0
        self._misses = 0

    def exists(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        return self.get(key) is not None

    def stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        # Count cache files
        file_count = sum(1 for _ in self.cache_dir.rglob("*.pkl"))
        total_size = sum(p.stat().st_size for p in self.cache_dir.rglob("*.pkl"))

        total_requests = self._hits + self._misses
        hit_rate = self._hits / total_requests if total_requests > 0 else 0

        return {
            "type": "disk",
            "cache_dir": str(self.cache_dir),
            "file_count": file_count,
            "total_size_bytes": total_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
        }


def generate_cache_key(func_name: str, inputs: dict[str, Any]) -> str:
    """Generate a cache key from function name and inputs.

    Args:
        func_name: Name of the function
        inputs: Input values to the function

    Returns:
        A unique cache key
    """
    # Create a deterministic string representation
    key_parts = [func_name]

    for k, v in sorted(inputs.items()):
        # Skip special inputs
        if k in ("context", "_cache"):
            continue

        # Convert value to string representation
        try:
            if isinstance(v, str | int | float | bool | type(None)):
                v_str = str(v)
            elif isinstance(v, list | tuple | dict):
                v_str = json.dumps(v, sort_keys=True)
            else:
                # Use pickle for complex objects
                v_str = hashlib.sha256(pickle.dumps(v)).hexdigest()
        except Exception:
            # Fallback to string representation
            v_str = str(v)

        key_parts.append(f"{k}={v_str}")

    # Generate hash of the key
    key_str = "|".join(key_parts)
    return hashlib.sha256(key_str.encode()).hexdigest()


# Global cache registry
_cache_backends: dict[str, CacheBackend] = {
    "memory": MemoryCache(),
}


def get_cache(name: str = "memory") -> CacheBackend:
    """Get a cache backend by name."""
    if name not in _cache_backends:
        if name == "disk":
            _cache_backends[name] = DiskCache()
        else:
            raise ValueError(f"Unknown cache backend: {name}")
    return _cache_backends[name]


def register_cache(name: str, backend: CacheBackend) -> None:
    """Register a custom cache backend."""
    _cache_backends[name] = backend
