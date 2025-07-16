"""
Shared pytest fixtures and configuration for fast-dag tests.
"""

import pytest
from typing import Any


# Shared test functions for use in tests
def simple_function(x: int) -> int:
    """A simple function that doubles input"""
    return x * 2


def add_numbers(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b


def multiply_numbers(x: int, y: int) -> int:
    """Multiply two numbers"""
    return x * y


def string_function(text: str) -> str:
    """Convert string to uppercase"""
    return text.upper()


def dict_function(data: dict) -> dict:
    """Process dictionary data"""
    return {"processed": True, "count": len(data)}


async def async_function(value: int) -> int:
    """An async function for testing"""
    import asyncio

    await asyncio.sleep(0.01)  # Simulate async work
    return value + 1


def conditional_function(value: int) -> Any:
    """Function that returns ConditionalReturn"""
    from fast_dag import ConditionalReturn

    return ConditionalReturn(condition=value > 0, value=value)


def fsm_state_function(context: Any) -> Any:
    """Function that returns FSMReturn"""
    from fast_dag import FSMReturn

    count = context.metadata.get("count", 0)
    if count >= 3:
        return FSMReturn(stop=True, value=count)

    context.metadata["count"] = count + 1
    return FSMReturn(next_state="state_a", value=count)


# Pytest fixtures
@pytest.fixture
def simple_functions():
    """Provide simple test functions"""
    return {
        "simple": simple_function,
        "add": add_numbers,
        "multiply": multiply_numbers,
        "string": string_function,
        "dict": dict_function,
        "async": async_function,
        "conditional": conditional_function,
        "fsm_state": fsm_state_function,
    }


@pytest.fixture
def sample_data():
    """Provide sample test data"""
    return {
        "integers": [1, 2, 3, 4, 5],
        "strings": ["hello", "world", "test"],
        "dict": {"key1": "value1", "key2": "value2"},
        "nested": {"level1": {"level2": {"value": 42}}},
    }
