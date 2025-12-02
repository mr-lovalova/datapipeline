import hashlib
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.config.split import (
    SplitConfig,
    TimeSplitConfig,
)

from datapipeline.transforms.vector_utils import clone


class BaseLabeler:
    """Strategy: decide the split label for a vector."""

    def label(self, group_key: Any, vector: Vector) -> str:  # pragma: no cover - interface
        raise NotImplementedError


class HashLabeler(BaseLabeler):
    """Deterministic hash-based label selection.

    ratios: mapping label -> fraction; fractions in (0,1], sum <= 1.0
    key: "group" or "feature:<id>"
    seed: integer for deterministic hashing
    """

    def __init__(self, *, ratios: Mapping[str, float], key: str = "group", seed: int = 0) -> None:
        total = 0.0
        thresholds: list[tuple[float, str]] = []
        for label, frac in ratios.items():
            f = float(frac)
            if not (0.0 < f <= 1.0):
                raise ValueError(f"Invalid ratio for {label!r}: {frac!r}")
            total += f
            thresholds.append((total, str(label)))
        if total > 1.0 + 1e-9:
            raise ValueError("Sum of ratios must be <= 1.0")
        self._thresholds = thresholds
        self._seed = int(seed)
        self._key = str(key)

    @staticmethod
    def _hash_token(token: str, seed: int) -> float:
        b = (str(seed) + "|" + token).encode("utf-8")
        digest = hashlib.sha256(b).digest()
        num = int.from_bytes(digest[:8], "big")
        return (num % (1 << 53)) / float(1 << 53)

    def label(self, group_key: Any, vector: Vector) -> str:
        if self._key == "group":
            token = repr(group_key)
        elif self._key.startswith("feature:"):
            fid = self._key.split(":", 1)[1]
            val = vector.values.get(fid)
            token = repr(val) if val is not None else repr(group_key)
        else:
            token = repr(group_key)

        r = self._hash_token(token, self._seed)
        for thresh, label in self._thresholds:
            if r < thresh:
                return label
        return self._thresholds[-1][1]


class TimeLabeler(BaseLabeler):
    """Time-based label selection using ascending boundaries and labels."""

    def __init__(self, *, boundaries: Sequence[str], labels: Sequence[str]) -> None:
        if len(labels) != len(boundaries) + 1:
            raise ValueError("labels length must equal len(boundaries)+1")
        self._boundaries = [self._parse_iso(ts) for ts in boundaries]
        self._labels = [str(x) for x in labels]

    @staticmethod
    def _parse_iso(text: str) -> datetime:
        t = text.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(t)

    def label(self, group_key: Any, vector: Vector) -> str:  # noqa: ARG002 - vector not used
        key = group_key[0] if isinstance(
            group_key, (list, tuple)) else group_key
        if isinstance(key, datetime):
            ts = key
        else:
            ts = self._parse_iso(str(key))
        for idx, bound in enumerate(self._boundaries):
            if ts < bound:
                return self._labels[idx]
        return self._labels[-1]


class VectorSplitApplicator:
    """Apply a labeler to either filter or tag vector streams."""

    def __init__(
        self,
        *,
        labeler: BaseLabeler,
        output: Literal["filter", "tag"] = "filter",
        keep: str | None = None,
        field: str = "__split__",
    ) -> None:
        self._labeler = labeler
        self._output = output
        self._keep = keep
        self._field = field

        # Enable pass-through when filter mode but keep unset or placeholder
        self._keep_placeholder = False
        if isinstance(self._keep, str):
            s = self._keep.strip()
            if s.startswith("${") and s.endswith("}"):
                self._keep_placeholder = True
        self._filter_enabled = not (
            self._output == "filter" and (
                self._keep is None or self._keep_placeholder)
        )

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            group_key, vector = sample.key, sample.features
            label = self._labeler.label(group_key, vector)
            if self._output == "filter":
                if not self._filter_enabled:
                    yield sample
                    continue
                if label == self._keep:
                    yield sample
                else:
                    continue
            else:
                data = clone(vector.values)
                data[self._field] = label
                yield sample.with_features(Vector(values=data))


def build_labeler(cfg: SplitConfig) -> BaseLabeler:
    if isinstance(cfg, TimeSplitConfig):
        return TimeLabeler(boundaries=cfg.boundaries, labels=cfg.labels)
    return HashLabeler(ratios=cfg.ratios, key=cfg.key, seed=cfg.seed)


def build_applicator(cfg: SplitConfig, keep: str | None = None) -> VectorSplitApplicator:
    labeler = build_labeler(cfg)
    selected = keep if keep is not None else getattr(cfg, "keep", None)
    return VectorSplitApplicator(labeler=labeler, output="filter", keep=selected)


def apply_split_stage(runtime, stream: Iterator[Sample]) -> Iterator[Sample]:
    """Apply project-configured split at the end of the vector pipeline.

    Reads `runtime.split` (set during bootstrap from project.globals.split) and,
    when configured, applies a VectorSplitApplicator. When not configured,
    passes stream through.
    """
    try:
        cfg = getattr(runtime, "split", None)
        if not cfg:
            return stream
        keep = getattr(runtime, "split_keep", None)
        applicator = build_applicator(cfg, keep=keep)
        return applicator(stream)
    except Exception:
        return stream
