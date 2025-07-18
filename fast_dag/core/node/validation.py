"""Node validation functionality."""

from ..types import NodeType
from .core import CoreNode


class NodeValidator(CoreNode):
    """Node validation functionality."""

    def validate(self) -> list[str]:
        """Validate the node configuration.

        Returns a list of validation errors, empty if valid.
        """
        errors = []

        # Check that function is callable
        if not callable(self.func):
            errors.append(f"Node '{self.name}': func must be callable")

        # Check that we have at least one output
        if not self.outputs:
            errors.append(f"Node '{self.name}': must have at least one output")

        # Check for return annotation - but only warn, don't fail
        # Many Python functions don't have return annotations
        # sig = inspect.signature(self.func)
        # if sig.return_annotation is inspect.Parameter.empty:
        #     errors.append(
        #         f"Node '{self.name}': function must have a return type annotation"
        #     )

        # For conditional nodes, check specific outputs
        if (
            self.node_type == NodeType.CONDITIONAL
            and self.outputs
            and (
                ("true" not in self.outputs or "false" not in self.outputs)
                and ("on_true" not in self.outputs or "on_false" not in self.outputs)
            )
        ):
            errors.append(
                f"Conditional node '{self.name}': must have 'true' and 'false' or 'on_true' and 'on_false' outputs"
            )

        return errors
