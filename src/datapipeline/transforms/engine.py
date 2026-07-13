import logging
from collections.abc import Callable, Iterator, Sequence
from inspect import Parameter, isclass, signature
from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.dag.transform_observability import (
    ObserverRegistry,
    SupportsObserver,
    TransformEvent,
)
from datapipeline.plugins import STREAM_TRANFORMS_EP
from datapipeline.transforms.interfaces import (
    SupportsContextBinding,
    SupportsPartitionBinding,
)
from datapipeline.transforms.spec import TransformSpec
from datapipeline.utils.load import load_ep

logger = logging.getLogger(__name__)


def _close_iterator(iterator: Any) -> None:
    closer = getattr(iterator, "close", None)
    if not callable(closer):
        return
    try:
        closer()
    except Exception:
        logger.debug(
            "Failed to close transform iterator during teardown",
            exc_info=True,
        )


def _with_close_cascade(
    stream: Iterator[Any],
    upstream: Iterator[Any],
) -> Iterator[Any]:
    if stream is upstream:
        return stream

    return _CloseCascadeIterator(stream, upstream)


class _CloseCascadeIterator:
    def __init__(self, stream: Iterator[Any], upstream: Iterator[Any]) -> None:
        self._stream = stream
        self._upstream = upstream
        self._iterator: Iterator[Any] | None = None
        self._closed = False

    def __iter__(self) -> "_CloseCascadeIterator":
        return self

    def __next__(self) -> Any:
        if self._closed:
            raise StopIteration
        try:
            if self._iterator is None:
                self._iterator = iter(self._stream)
            return next(self._iterator)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._iterator is not None:
            _close_iterator(self._iterator)
        if self._stream is not self._iterator:
            _close_iterator(self._stream)
        _close_iterator(self._upstream)


def apply_transforms(
    stream: Iterator[Any],
    group: str,
    transforms: Sequence[TransformSpec] | None,
    context: PipelineContext | None = None,
    observer: Callable[[TransformEvent], None] | None = None,
    observer_registry: ObserverRegistry | None = None,
    partition_by: str | list[str] | None = None,
) -> Iterator[Any]:
    """Instantiate and apply parsed transform specs in order."""

    observer = observer or (
        getattr(context, "transform_observer", None) if context is not None else None
    )
    registry = observer_registry or (
        getattr(context, "observer_registry", None) if context is not None else None
    )

    for spec in transforms or ():
        upstream = stream
        try:
            entrypoint = load_ep(group=group, name=spec.name)
            _validate_entrypoint_contract(
                entrypoint,
                group,
                spec,
                context=context,
                partition_by=partition_by,
            )
            params = dict(spec.params)
            if isclass(entrypoint):
                transform = entrypoint(**params)
                if context is not None and isinstance(
                    transform, SupportsContextBinding
                ):
                    transform.bind_context(context)
                if isinstance(transform, SupportsPartitionBinding):
                    transform.bind_partition_by(partition_by)
                effective_observer = observer
                if effective_observer is None and registry:
                    effective_observer = registry.get(
                        spec.name,
                        logging.getLogger(f"{group}.{spec.name}"),
                    )
                _attach_observer(transform, effective_observer)
                stream = transform(stream)
            else:
                stream = entrypoint(stream, **params)
        except Exception:
            _close_iterator(upstream)
            raise
        stream = _with_close_cascade(stream, upstream)
    return stream


def _validate_entrypoint_contract(
    entrypoint: Callable[..., Any],
    group: str,
    spec: TransformSpec,
    *,
    context: PipelineContext | None,
    partition_by: str | list[str] | None,
) -> None:
    """Reject ambiguous callables and old implicit runtime bindings."""
    try:
        parameters = signature(entrypoint).parameters.values()
    except (TypeError, ValueError):
        return

    keyword_parameter_names = {
        parameter.name
        for parameter in parameters
        if parameter.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY)
    }
    accepts_arbitrary_keywords = any(
        parameter.kind is Parameter.VAR_KEYWORD for parameter in parameters
    )
    if accepts_arbitrary_keywords:
        raise TypeError(
            f"Transform '{spec.name}' accepts arbitrary keyword arguments. "
            "Declare each supported configuration parameter explicitly."
        )

    class_entrypoint = isclass(entrypoint)

    has_context_hook = class_entrypoint and callable(
        getattr(entrypoint, "bind_context", None)
    )
    if (
        context is not None
        and not has_context_hook
        and "context" not in spec.params
        and "context" in keyword_parameter_names
    ):
        raise TypeError(
            f"Transform '{spec.name}' relies on implicit context injection. "
            "Use a class transform with bind_context(context) instead."
        )

    has_partition_hook = class_entrypoint and callable(
        getattr(entrypoint, "bind_partition_by", None)
    )
    implicit_partition_parameters = {"partition_by"}
    if group == STREAM_TRANFORMS_EP:
        implicit_partition_parameters.add("stream_partition_by")
    implicit_partition_parameters.difference_update(spec.params)
    if (
        partition_by is not None
        and not has_partition_hook
        and implicit_partition_parameters & keyword_parameter_names
    ):
        raise TypeError(
            f"Transform '{spec.name}' relies on implicit partition binding. "
            "Use a class transform with bind_partition_by(partition_by) instead."
        )


def _attach_observer(transform: Any, observer: Callable[..., None] | None) -> None:
    if observer is None:
        return
    if isinstance(transform, SupportsObserver):
        transform.set_observer(observer)
