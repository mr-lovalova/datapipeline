from collections.abc import Iterator
from typing import Any

import pytest

from datapipeline.transforms.engine import apply_transforms
from datapipeline.transforms.spec import TransformSpec


class _NonClosingIterator:
    def __init__(self, stream: Iterator[Any]) -> None:
        self._iterator = iter(stream)

    def __iter__(self) -> "_NonClosingIterator":
        return self

    def __next__(self) -> Any:
        return next(self._iterator)

    def close(self) -> None:
        # Intentionally does not close upstream.
        return


def test_apply_transforms_closes_upstream_for_function_entrypoint(monkeypatch) -> None:
    closed: list[str] = []

    def _upstream() -> Iterator[int]:
        try:
            yield 1
            yield 2
        finally:
            closed.append("upstream")

    def _broken_transform(stream: Iterator[Any]) -> Iterator[Any]:
        return _NonClosingIterator(stream)

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: _broken_transform,
    )

    transformed = apply_transforms(
        _upstream(),
        group="test.group",
        transforms=[TransformSpec(name="broken", params={})],
    )
    assert next(transformed) == 1
    transformed.close()
    assert closed == ["upstream"]


def test_apply_transforms_closes_upstream_for_class_entrypoint(monkeypatch) -> None:
    closed: list[str] = []

    def _upstream() -> Iterator[int]:
        try:
            yield 1
            yield 2
        finally:
            closed.append("upstream")

    class _BrokenTransform:
        def __call__(self, stream: Iterator[Any]) -> Iterator[Any]:
            return _NonClosingIterator(stream)

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: _BrokenTransform,
    )

    transformed = apply_transforms(
        _upstream(),
        group="test.group",
        transforms=[TransformSpec(name="broken", params={})],
    )
    assert next(transformed) == 1
    transformed.close()
    assert closed == ["upstream"]


def test_function_transform_receives_only_configured_parameters(monkeypatch) -> None:
    received: dict[str, Any] = {}

    def transform(
        stream: Iterator[Any],
        *,
        value: int,
    ) -> Iterator[Any]:
        received["value"] = value
        return stream

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: transform,
    )

    result = apply_transforms(
        iter([1]),
        group="test.group",
        transforms=[TransformSpec(name="explicit", params={"value": 3})],
        context=object(),  # type: ignore[arg-type]
        partition_by="security_id",
    )

    assert list(result) == [1]
    assert received == {"value": 3}


def test_class_transform_receives_explicit_lifecycle_bindings(monkeypatch) -> None:
    calls: list[tuple[str, Any]] = []
    context = object()

    class BoundTransform:
        def __init__(self, *, value: int) -> None:
            calls.append(("init", value))

        def bind_context(self, bound_context) -> None:
            calls.append(("context", bound_context))

        def bind_partition_by(self, partition_by) -> None:
            calls.append(("partition", partition_by))

        def __call__(self, stream: Iterator[Any]) -> Iterator[Any]:
            calls.append(("call", None))
            return stream

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: BoundTransform,
    )

    result = apply_transforms(
        iter([1]),
        group="test.group",
        transforms=[TransformSpec(name="bound", params={"value": 3})],
        context=context,  # type: ignore[arg-type]
        partition_by="security_id",
    )

    assert list(result) == [1]
    assert calls == [
        ("init", 3),
        ("context", context),
        ("partition", "security_id"),
        ("call", None),
    ]


def test_class_without_custom_init_does_not_receive_hidden_kwargs(monkeypatch) -> None:
    class TransformWithoutInit:
        def __call__(self, stream: Iterator[Any]) -> Iterator[Any]:
            return stream

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: TransformWithoutInit,
    )

    result = apply_transforms(
        iter([1]),
        group="test.group",
        transforms=[TransformSpec(name="plain", params={})],
        context=object(),  # type: ignore[arg-type]
        partition_by="security_id",
    )

    assert list(result) == [1]


def test_legacy_function_context_injection_fails_loudly(monkeypatch) -> None:
    def legacy_transform(stream: Iterator[Any], context=None) -> Iterator[Any]:
        return stream

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: legacy_transform,
    )

    with pytest.raises(TypeError, match="bind_context"):
        apply_transforms(
            iter([1]),
            group="test.group",
            transforms=[TransformSpec(name="legacy", params={})],
            context=object(),  # type: ignore[arg-type]
        )


def test_legacy_class_partition_injection_fails_loudly(monkeypatch) -> None:
    class LegacyTransform:
        def __init__(self, partition_by=None) -> None:
            self.partition_by = partition_by

        def __call__(self, stream: Iterator[Any]) -> Iterator[Any]:
            return stream

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: LegacyTransform,
    )

    with pytest.raises(TypeError, match="bind_partition_by"):
        apply_transforms(
            iter([1]),
            group="test.group",
            transforms=[TransformSpec(name="legacy", params={})],
            partition_by="security_id",
        )


def test_explicit_runtime_named_parameters_remain_configurable(monkeypatch) -> None:
    received: dict[str, Any] = {}

    def transform(
        stream: Iterator[Any],
        *,
        context: str,
        partition_by: str,
    ) -> Iterator[Any]:
        received.update(context=context, partition_by=partition_by)
        return stream

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: transform,
    )

    result = apply_transforms(
        iter([1]),
        group="test.group",
        transforms=[
            TransformSpec(
                name="explicit",
                params={"context": "configured", "partition_by": "configured"},
            )
        ],
        context=object(),  # type: ignore[arg-type]
        partition_by="security_id",
    )

    assert list(result) == [1]
    assert received == {"context": "configured", "partition_by": "configured"}


def test_arbitrary_transform_kwargs_are_rejected(monkeypatch) -> None:
    def transform(stream: Iterator[Any], **kwargs) -> Iterator[Any]:
        return stream

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: transform,
    )

    with pytest.raises(TypeError, match="Declare each supported"):
        apply_transforms(
            iter([1]),
            group="test.group",
            transforms=[TransformSpec(name="sloppy", params={})],
        )


def test_binding_failure_closes_upstream(monkeypatch) -> None:
    closed: list[str] = []

    def upstream() -> Iterator[int]:
        try:
            yield 1
        finally:
            closed.append("upstream")

    source = upstream()
    next(source)

    class BrokenBindingTransform:
        def bind_context(self, context) -> None:
            raise RuntimeError("binding failed")

        def __call__(self, stream: Iterator[Any]) -> Iterator[Any]:
            return stream

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: BrokenBindingTransform,
    )

    with pytest.raises(RuntimeError, match="binding failed"):
        apply_transforms(
            source,
            group="test.group",
            transforms=[TransformSpec(name="broken", params={})],
            context=object(),  # type: ignore[arg-type]
        )

    assert closed == ["upstream"]


def test_second_transform_setup_failure_closes_unstarted_chain(monkeypatch) -> None:
    closed: list[str] = []

    def upstream() -> Iterator[int]:
        try:
            yield 1
        finally:
            closed.append("upstream")

    source = upstream()
    next(source)

    class FirstOutput(_NonClosingIterator):
        def close(self) -> None:
            closed.append("first output")

    class FirstTransform:
        def __call__(self, stream: Iterator[Any]) -> Iterator[Any]:
            return FirstOutput(stream)

    class LegacySecondTransform:
        def __init__(self, context=None) -> None:
            self.context = context

        def __call__(self, stream: Iterator[Any]) -> Iterator[Any]:
            return stream

    entrypoints = iter([FirstTransform, LegacySecondTransform])
    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: next(entrypoints),
    )

    with pytest.raises(TypeError, match="bind_context"):
        apply_transforms(
            source,
            group="test.group",
            transforms=[
                TransformSpec(name="first", params={}),
                TransformSpec(name="legacy", params={}),
            ],
            context=object(),  # type: ignore[arg-type]
        )

    assert closed == ["first output", "upstream"]
