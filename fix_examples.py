#!/usr/bin/env python3
"""Fix issues in example files to make them mypy-compliant."""

import re
from pathlib import Path


def fix_example_05():
    """Fix issues in example 05."""
    file_path = Path("examples/05_manual_node_creation.py")
    content = file_path.read_text()

    # Replace get_node with nodes[]
    content = re.sub(r'dag\.get_node\("([^"]+)"\)', r'dag.nodes["\1"]', content)

    # Fix the builder pattern conditional node creation
    old_builder = (
        """dag.add_node("check", quality_check, node_type=NodeType.CONDITIONAL)"""
    )
    new_builder = """dag.add_node(Node(func=quality_check, name="check", node_type=NodeType.CONDITIONAL))"""
    content = content.replace(old_builder, new_builder)

    # Fix the registry.get() type issue
    old_get = """func = registry.get(node_def["func"])"""
    new_get = """func = registry.get(node_def["func"])
        if func is None:
            raise ValueError(f"Function {node_def['func']} not found in registry")"""
    content = content.replace(old_get, new_get)

    file_path.write_text(content)
    print("Fixed examples/05_manual_node_creation.py")


if __name__ == "__main__":
    fix_example_05()
    print("All example fixes applied!")
