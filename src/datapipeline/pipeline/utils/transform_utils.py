import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Optional, Tuple
from inspect import isclass, signature, Parameter
from contextlib import nullcontext

from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.observability import ObserverRegistry, SupportsObserver, TransformEvent

from datapipeline.utils.load import load_ep


def _extract_single_pair(clause: Mapping[str, Any], kind: str) -> Tuple[str, Any]:
    """Validate that *clause* is a one-key mapping and return that pair."""

    if not isinstance(clause, Mapping) or len(clause) != 1:
        raise TypeError(f"{kind} must be one-key mapping, got: {clause!r}")
    return next(iter(clause.items()))


def _supports_parameter(callable_obj: Callable[..., Any], name: str) -> bool:
    try:
        sig = signature(callable_obj)
    except (ValueError, TypeError):
        return False
    for param in sig.parameters.values():
        if param.kind == Parameter.VAR_KEYWORD:
            return True
        if param.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY) and param.name == name:
            return True
    return False


def _split_params(params: Any) -> Tuple[Tuple[Any, ...], dict[str, Any]]:
    if params is None:
        return (), {}
    if isinstance(params, (list, tuple)):
        return tuple(params), {}
    if isinstance(params, Mapping):
        return (), dict(params)
    return (params,), {}


def _call_with_params(
    fn: Callable,
    stream: Iterator[Any],
    params: Any,
    context: Optional[PipelineContext],
) -> Iterator[Any]:
    """Invoke an entry-point callable with optional params semantics."""

    args, kwargs = _split_params(params)
    if context and _supports_parameter(fn, "context") and "context" not in kwargs:
        kwargs["context"] = context
    return fn(stream, *args, **kwargs)


def _instantiate_entry_point(
    cls: Callable[..., Any],
    params: Any,
    context: Optional[PipelineContext],
) -> Any:
    """Instantiate a transform class with parameters from the config."""

    args, kwargs = _split_params(params)
    if context and _supports_parameter(cls.__init__, "context") and "context" not in kwargs:
        kwargs["context"] = context
    return cls(*args, **kwargs)


def _bind_context(transform: Any, context: Optional[PipelineContext]) -> None:
    if not context:
        return
    binder = getattr(transform, "bind_context", None)
    if callable(binder):
        binder(context)


def apply_transforms(
    stream: Iterator[Any],
    group: str,
    transforms: Optional[Sequence[Mapping[str, Any]]],
    context: Optional[PipelineContext] = None,
    observer: Callable[[TransformEvent], None] | None = None,
    observer_registry: ObserverRegistry | None = None,
) -> Iterator[Any]:
    """Instantiate and apply configured transforms in order."""

    observer = observer or (getattr(context, "transform_observer", None)
                            if context is not None else None)
    registry = observer_registry or (getattr(context, "observer_registry", None)
                                     if context is not None else None)

    context_cm = context.activate() if context else nullcontext()
    with context_cm:
        for transform in transforms or ():
            name, params = _extract_single_pair(transform, "Transform")
            ep = load_ep(group=group, name=name)
            if isclass(ep):
                inst = _instantiate_entry_point(ep, params, context)
                _bind_context(inst, context)
                eff_observer = observer
                if eff_observer is None and registry:
                    eff_observer = registry.get(
                        name, logging.getLogger(f"{group}.{name}")
                    )
                _attach_observer(inst, eff_observer)
                stream = inst(stream)
            else:
                stream = _call_with_params(ep, stream, params, context)
    return stream


def _attach_observer(transform: Any, observer: Callable[..., None] | None) -> None:
    if observer is None:
        return
    if isinstance(transform, SupportsObserver):
        transform.set_observer(observer)
