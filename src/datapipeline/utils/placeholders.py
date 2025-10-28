from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MissingInterpolation:
    """Sentinel representing a ${var} that resolved to an explicit null."""

    key: str

    def __bool__(self) -> bool:
        # Treat missing values as falsy in conditionals.
        return False

    def __repr__(self) -> str:
        return f"<MissingInterpolation key={self.key!r}>"


def is_missing(value: Any) -> bool:
    return isinstance(value, MissingInterpolation)


def coalesce_missing(value: Any, default: Any = None) -> Any:
    return default if is_missing(value) else value


def normalize_args(args: dict[str, Any] | None, *, default: Any = None) -> dict[str, Any]:
    if not args:
        return {}
    return {
        key: coalesce_missing(val, default=default)
        for key, val in args.items()
    }
