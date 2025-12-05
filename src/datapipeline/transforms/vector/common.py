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


class VectorContextMixin:
    def __init__(self, payload: Literal["features", "targets"] = "features") -> None:
        if payload not in {"features", "targets"}:
            raise ValueError("payload must be 'features' or 'targets'")
        self._context: PipelineContext | None = None
        self._payload = payload

    def bind_context(self, context: PipelineContext) -> None:
        self._context = context

    def _expected_ids(self, payload: str | None = None) -> list[str]:
        """Return expected feature/target ids for the given payload.

        When `payload` is omitted, the instance default is used.
        """
        ctx = self._context or try_get_current_context()
        if not ctx:
            return []
        kind = payload or self._payload
        if kind not in {"features", "targets"}:
            return []
        schema = ctx.load_schema(payload=kind) or []
        ids = [
            entry.get("id")
            for entry in schema
            if isinstance(entry, dict) and isinstance(entry.get("id"), str)
        ]
        return ids or []


class VectorPostprocessBase(VectorContextMixin):
    """Shared envelope for vector postprocess transforms.

    Provides a consistent contract for payload selection and id filtering:
    - payload: features | targets | both
    - only: optional allow-list of ids
    - exclude: optional deny-list of ids
    """

    def __init__(
        self,
        *,
        payload: Literal["features", "targets", "both"] = "features",
        only: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> None:
        if payload not in {"features", "targets", "both"}:
            raise ValueError(
                "payload must be 'features', 'targets', or 'both'")
        base_payload = "features" if payload == "both" else payload
        super().__init__(payload=base_payload)
        self._payload_mode: Literal["features", "targets", "both"] = payload
        self._only = {str(fid) for fid in (only or [])} or None
        self._exclude = {str(fid) for fid in (exclude or [])} or None
        self._baseline_cache: dict[str, list[str]] = {}

    def _payload_kinds(self) -> list[Literal["features", "targets"]]:
        mode = self._payload_mode
        kinds: list[Literal["features", "targets"]] = []
        if mode in {"features", "both"}:
            kinds.append("features")
        if mode in {"targets", "both"}:
            kinds.append("targets")
        return kinds

    def _ids_for(self, payload: Literal["features", "targets"]) -> list[str]:
        cached = self._baseline_cache.get(payload)
        if cached is not None:
            return list(cached)
        ids = self._expected_ids(payload=payload)
        if self._only is not None:
            ids = [fid for fid in ids if fid in self._only]
        if self._exclude is not None:
            ids = [fid for fid in ids if fid not in self._exclude]
        self._baseline_cache[payload] = list(ids)
        return list(ids)
