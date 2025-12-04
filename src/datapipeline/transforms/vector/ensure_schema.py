from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator
from typing import Any, Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import clone

from .common import VectorContextMixin, replace_vector, select_vector

MissingPolicy = Literal["error", "drop", "fill"]
ExtraPolicy = Literal["error", "drop", "keep"]


class VectorEnsureSchemaTransform(VectorContextMixin):
    """Ensure vectors conform to the vector schema (`schema.json`) artifact.

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
        self._schema_entries: list[dict[str, Any]] | None = None
        self._schema_meta: dict[str, dict[str, Any]] = {}

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        baseline = self._schema_ids()
        baseline_set = set(baseline)

        for sample in stream:
            vector = select_vector(sample, self._payload)
            if vector is None:
                yield sample
                continue

            values = vector.values
            working = None

            missing = [fid for fid in baseline if fid not in values]
            if missing:
                decision = self._on_missing
                if decision == "error":
                    raise ValueError(
                        f"Vector missing required identifiers {missing} "
                        f"for payload '{self._payload}'."
                    )
                if decision == "drop":
                    continue
                working = clone(values)
                for fid in missing:
                    working[fid] = self._fill_value

            extras = [fid for fid in values if fid not in baseline_set]
            if extras:
                decision = self._on_extra
                if decision == "error":
                    raise ValueError(
                        f"Vector contains unexpected identifiers {extras} "
                        f"for payload '{self._payload}'."
                    )
                if decision == "drop":
                    working = working or clone(values)
                    for fid in extras:
                        working.pop(fid, None)

            current_values = working or values

            # Optionally enforce per-id cadence from schema metadata
            current_values = self._enforce_cadence(current_values)

            ordered = OrderedDict()
            for fid in baseline:
                ordered[fid] = current_values.get(fid)
            if self._on_extra == "keep":
                for fid, value in current_values.items():
                    if fid not in baseline_set:
                        ordered[fid] = value
            current_values = ordered

            if current_values is not values:
                updated_vector = Vector(values=dict(current_values))
                sample = replace_vector(sample, self._payload, updated_vector)

            yield sample

    def _schema_ids(self) -> list[str]:
        if self._baseline is None:
            entries = self._load_schema_entries()
            ordered = [entry["id"] for entry in entries if isinstance(entry.get("id"), str)]
            if not ordered:
                raise RuntimeError(
                    "Vector schema artifact is empty or unavailable; run `jerry build` "
                    "to materialize `schema.json` via the `vector_schema` task."
                )
            self._baseline = ordered
            self._schema_meta = {
                entry["id"]: entry for entry in entries if isinstance(entry.get("id"), str)
            }
        return list(self._baseline)

    def _load_schema_entries(self) -> list[dict[str, Any]]:
        if self._schema_entries is None:
            context = getattr(self, "_context", None)
            if not context:
                entries = []
            else:
                entries = context.load_schema(payload=self._payload)
            self._schema_entries = entries or []
        return self._schema_entries

    def _enforce_cadence(self, values: dict[str, Any]) -> dict[str, Any]:
        if not values or not self._schema_meta:
            return values
        adjusted = None
        for fid, value in values.items():
            meta = self._schema_meta.get(fid)
            if not meta or meta.get("kind") != "list":
                continue
            expected = self._expected_lengths(meta)
            if not expected:
                continue
            current_len = len(value) if isinstance(value, list) else (0 if value is None else 1)
            if current_len in expected:
                continue
            decision = self._on_missing
            if decision == "error":
                raise ValueError(
                    f"List feature '{fid}' length {current_len} violates schema cadence {sorted(expected)}"
                )
            if decision == "drop":
                return {}
            # fill: pad or truncate to the closest expected length
            target_len = expected[0]
            adjusted = adjusted or clone(values)
            if isinstance(value, list):
                seq = value[:target_len]
            elif value is None:
                seq = []
            else:
                seq = [value]
            if len(seq) < target_len:
                seq = seq + [self._fill_value] * (target_len - len(seq))
            adjusted[fid] = seq
        return adjusted or values

    def _expected_lengths(self, meta: dict[str, Any]) -> list[int]:
        cadence = meta.get("cadence")
        if isinstance(cadence, dict):
            target = cadence.get("target")
            if isinstance(target, (int, float)) and target > 0:
                return [int(target)]
        modes = meta.get("list_length", {}).get("modes")
        if isinstance(modes, (list, tuple)) and modes:
            ints = [int(m) for m in modes if isinstance(m, (int, float))]
            if ints:
                return sorted(ints)
        expected = meta.get("expected_length")
        if isinstance(expected, (int, float)):
            return [int(expected)]
        max_len = meta.get("list_length", {}).get("max")
        if isinstance(max_len, (int, float)) and max_len > 0:
            return [int(max_len)]
        return []
