from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Optional, Tuple
from inspect import isclass

from datapipeline.utils.load import load_ep


def _extract_single_pair(clause: Mapping[str, Any], kind: str) -> Tuple[str, Any]:
    """Validate that *clause* is a one-key mapping and return that pair."""

    if not isinstance(clause, Mapping) or len(clause) != 1:
        raise TypeError(f"{kind} must be one-key mapping, got: {clause!r}")
    return next(iter(clause.items()))


def _call_with_params(fn: Callable, stream: Iterator[Any], params: Any) -> Iterator[Any]:
    """Invoke an entry-point callable with optional params semantics."""

    if params is None:
        return fn(stream)
    if isinstance(params, (list, tuple)):
        return fn(stream, *params)
    if isinstance(params, Mapping):
        return fn(stream, **params)
    return fn(stream, params)


def _instantiate_entry_point(cls: Callable[..., Any], params: Any) -> Any:
    """Instantiate a transform class with parameters from the config."""

    if params is None:
        return cls()
    if isinstance(params, Mapping):
        return cls(**params)
    if isinstance(params, (list, tuple)):
        return cls(*params)
    return cls(params)


def apply_transforms(
    stream: Iterator[Any],
    group: str,
    transforms: Optional[Sequence[Mapping[str, Any]]],
) -> Iterator[Any]:
    """Instantiate and apply configured transforms in order."""

    for transform in transforms or ():
        name, params = _extract_single_pair(transform, "Transform")
        ep = load_ep(group=group, name=name)
        if isclass(ep):
            inst = _instantiate_entry_point(ep, params)
            stream = inst(stream)
        else:
            stream = _call_with_params(ep, stream, params)
    return stream
