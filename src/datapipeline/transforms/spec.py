from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TransformSpec:
    name: str
    params: Mapping[str, Any] = field(default_factory=dict)


def parse_transform_spec(value: object) -> TransformSpec:
    """Parse one transform clause from project configuration."""
    if isinstance(value, TransformSpec):
        return value
    if not isinstance(value, Mapping) or len(value) != 1:
        raise ValueError(f"Transform must be a one-key mapping, got: {value!r}")

    name, params = next(iter(value.items()))
    if not isinstance(name, str):
        raise ValueError(f"Transform name must be a string, got: {name!r}")
    name = name.strip()
    if not name:
        raise ValueError("Transform name must not be empty")
    if params is None:
        return TransformSpec(name=name)
    if not isinstance(params, Mapping):
        raise ValueError(f"Parameters for transform '{name}' must be a mapping or null")

    non_string_keys = [key for key in params if not isinstance(key, str)]
    if non_string_keys:
        raise ValueError(
            f"Parameter keys for transform '{name}' must be strings, "
            f"got: {non_string_keys!r}"
        )
    return TransformSpec(name=name, params=dict(params))


def serialize_transform_spec(spec: TransformSpec) -> dict[str, dict[str, Any]]:
    """Return the one-key mapping used by project configuration files."""
    return {spec.name: dict(spec.params)}
