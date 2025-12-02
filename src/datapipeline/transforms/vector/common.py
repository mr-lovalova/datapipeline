from typing import Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.context import (
    PipelineContext,
    try_get_current_context,
)


def select_vector(sample: Sample, payload: Literal["features", "targets"]) -> Vector | None:
    if payload == "targets":
        return sample.targets
    return sample.features


def replace_vector(sample: Sample, payload: Literal["features", "targets"], vector: Vector) -> Sample:
    if payload == "targets":
        return sample.with_targets(vector)
    return sample.with_features(vector)


class ContextExpectedMixin:
    def __init__(self, payload: Literal["features", "targets"] = "features") -> None:
        if payload not in {"features", "targets"}:
            raise ValueError("payload must be 'features' or 'targets'")
        self._context: PipelineContext | None = None
        self._payload = payload

    def bind_context(self, context: PipelineContext) -> None:
        self._context = context

    def _expected_ids(self) -> list[str]:
        ctx = self._context or try_get_current_context()
        if not ctx:
            return []
        schema = ctx.load_schema(payload=self._payload) or []
        ids = [
            entry.get("id")
            for entry in schema
            if isinstance(entry, dict) and isinstance(entry.get("id"), str)
        ]
        return ids or []
