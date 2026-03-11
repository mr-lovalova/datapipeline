from collections.abc import Iterator
from typing import Any

from datapipeline.transforms.engine import apply_transforms


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

    def _broken_transform(stream: Iterator[Any], *args, **kwargs) -> Iterator[Any]:
        return _NonClosingIterator(stream)

    monkeypatch.setattr(
        "datapipeline.transforms.engine.load_ep",
        lambda group, name: _broken_transform,
    )

    transformed = apply_transforms(
        _upstream(),
        group="test.group",
        transforms=[{"broken": None}],
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
        transforms=[{"broken": None}],
    )
    assert next(transformed) == 1
    transformed.close()
    assert closed == ["upstream"]
