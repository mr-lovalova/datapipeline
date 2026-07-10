from collections.abc import Iterator
from typing import Any, Literal

from datapipeline.dag.context import PipelineContext
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector

from .common import VectorContextMixin, replace_vector, select_vector

MissingPolicy = Literal["error", "drop", "fill"]
ExtraPolicy = Literal["error", "drop", "keep"]


class VectorEnsureSchemaTransform(VectorContextMixin):
    """Ensure vectors conform to the vector schema (`build/schema.json`) artifact.

    Options allow filling or dropping rows with missing identifiers and
    pruning/raising on unexpected identifiers.
    """

    def __init__(
        self,
        *,
        payload: Literal["features", "targets"] = "features",
        on_missing: MissingPolicy = "error",
        fill_value: Any = None,
        on_extra: ExtraPolicy = "error",
    ) -> None:
        super().__init__(payload=payload)
        if on_missing not in {"error", "drop", "fill"}:
            raise ValueError("on_missing must be one of: 'error', 'drop', 'fill'")
        if on_extra not in {"error", "drop", "keep"}:
            raise ValueError("on_extra must be one of: 'error', 'drop', 'keep'")
        self._on_missing = on_missing
        self._fill_value = fill_value
        self._on_extra = on_extra
        self._baseline: list[str] | None = None
        self._list_lengths: dict[str, int] = {}

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def bind_context(self, context: PipelineContext) -> None:
        super().bind_context(context)
        # Snapshot schema entries when the transform is wired up so lazy stream
        # iteration cannot observe a later artifact/context mismatch.
        schema = context.load_schema()
        entries = schema.targets if self._payload == "targets" else schema.features
        self._baseline = [entry.id for entry in entries]
        self._list_lengths = {
            entry.id: entry.cadence.target
            for entry in entries
            if entry.kind == "list" and entry.cadence is not None
        }

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        baseline = self._baseline
        if not baseline:
            raise RuntimeError(
                "Vector schema artifact is empty or unavailable; run `jerry build` "
                "to materialize `build/schema.json` via the `vector_schema` task."
            )
        baseline_set = set(baseline)

        for sample in stream:
            vector = select_vector(sample, self._payload)
            if vector is None:
                yield sample
                continue

            values = vector.values

            missing = [fid for fid in baseline if fid not in values]
            if missing:
                if self._on_missing == "error":
                    raise ValueError(
                        f"Vector missing required identifiers {missing} "
                        f"for payload '{self._payload}'."
                    )
                if self._on_missing == "drop":
                    continue

            extras = [fid for fid in values if fid not in baseline_set]
            if extras:
                if self._on_extra == "error":
                    raise ValueError(
                        f"Vector contains unexpected identifiers {extras} "
                        f"for payload '{self._payload}'."
                    )

            ordered = {
                fid: values[fid] if fid in values else self._fill_value
                for fid in baseline
            }
            if self._on_extra == "keep":
                ordered.update((fid, values[fid]) for fid in extras)

            normalized = self._enforce_list_lengths(ordered)
            if normalized is None:
                continue

            yield replace_vector(sample, self._payload, Vector(values=normalized))

    def _enforce_list_lengths(
        self,
        values: dict[str, Any],
    ) -> dict[str, Any] | None:
        for fid, target_len in self._list_lengths.items():
            value = values[fid]
            current_len = len(value) if isinstance(value, list) else None
            if current_len == target_len:
                continue
            if self._on_missing == "error":
                actual = (
                    f"list with length {current_len}"
                    if current_len is not None
                    else type(value).__name__
                )
                raise ValueError(
                    f"Value '{fid}' must be a list with length {target_len}; "
                    f"got {actual}."
                )
            if self._on_missing == "drop":
                return None
            if isinstance(value, list):
                seq = value[:target_len]
            elif value is None:
                seq = []
            else:
                seq = [value]
            if len(seq) < target_len:
                seq = seq + [self._fill_value] * (target_len - len(seq))
            values[fid] = seq
        return values
