from __future__ import annotations

from collections.abc import Iterator
from typing import Literal

from datapipeline.domain.sample import Sample

from .horizontal import VectorDropHorizontalTransform
from .vertical import VectorDropVerticalTransform

Axis = Literal["horizontal", "vertical"]


class VectorDropTransform:
    """Drop vectors or features based on coverage thresholds.

    Thin orchestrator that delegates to horizontal or vertical strategies based
    on the configured axis.
    """

    def __init__(
        self,
        *,
        axis: Axis = "horizontal",
        threshold: float,
        payload: Literal["features", "targets", "both"] = "features",
        only: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> None:
        if axis not in {"horizontal", "vertical"}:
            raise ValueError("axis must be 'horizontal' or 'vertical'")
        if axis == "vertical" and payload == "both":
            raise ValueError("axis='vertical' does not support payload='both'")
        if axis == "horizontal":
            self._impl: object = VectorDropHorizontalTransform(
                threshold=threshold,
                payload=payload,
                only=only,
                exclude=exclude,
            )
        else:
            # Vertical drop is partition/feature-oriented and does not support
            # payload='both'. Payload is validated above.
            self._impl = VectorDropVerticalTransform(
                payload=payload if payload != "both" else "features",
                threshold=threshold,
            )

    def bind_context(self, context) -> None:
        binder = getattr(self._impl, "bind_context", None)
        if binder is not None:
            binder(context)

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return getattr(self._impl, "apply")(stream)

