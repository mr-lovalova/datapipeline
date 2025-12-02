from typing import Optional, Any


class ParsingError(Exception):
    """Raised when a single row fails to parse."""

    def __init__(
        self,
        row: Any,
        index: Optional[int] = None,
        original_exc: Optional[BaseException] = None,
    ):
        self.row = row
        self.index = index
        self.original_exc = original_exc

        prefix = f"Failed to parse row at index {index}: " if index is not None else "Failed to parse row: "
        message = prefix + repr(row)

        # If there’s an underlying exception, append its type/message
        if original_exc is not None:
            message += f" (caused by {type(original_exc).__name__}: {original_exc})"

        super().__init__(message)
