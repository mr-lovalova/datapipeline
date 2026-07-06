import hashlib
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.config.split import (
    HashSplitConfig,
    HASH_SPLIT_FEATURE_PREFIX,
    HASH_SPLIT_GROUP_KEY,
    SplitConfig,
    TimeSplitConfig,
)

from datapipeline.transforms.vector_utils import clone


class HashLabeler:
    """Deterministic hash-based label selection.

    ratios: mapping label -> fraction from validated split config.
    key: "group" or "feature:<id>"
    seed: integer for deterministic hashing
    """

    def __init__(
        self,
        *,
        ratios: Mapping[str, float],
        key: str = "group",
        seed: int = 0,
    ) -> None:
        total = 0.0
        thresholds: list[tuple[float, str]] = []
        for label, frac in ratios.items():
            total += float(frac)
            thresholds.append((total, str(label)))
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
        token = repr(group_key)
        if self._key.startswith(HASH_SPLIT_FEATURE_PREFIX):
            fid = self._key.removeprefix(HASH_SPLIT_FEATURE_PREFIX)
            if fid not in vector.values:
                raise KeyError(f"hash split feature key {fid!r} not found")
            token = repr(vector.values[fid])

        r = self._hash_token(token, self._seed)
        for thresh, label in self._thresholds:
            if r < thresh:
                return label
        return self._thresholds[-1][1]


class TimeLabeler:
    """Time-based label selection using ascending boundaries and labels."""

    def __init__(self, *, boundaries: Sequence[str], labels: Sequence[str]) -> None:
        self._boundaries = [self._parse_iso(ts) for ts in boundaries]
        self._labels = [str(x) for x in labels]

    @staticmethod
    def _parse_iso(text: str) -> datetime:
        t = text.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(t)

    def label(self, group_key: Any, vector: Vector) -> str:  # noqa: ARG002 - vector not used
        key = group_key[0] if isinstance(group_key, (list, tuple)) else group_key
        ts = key if isinstance(key, datetime) else self._parse_iso(str(key))
        for idx, bound in enumerate(self._boundaries):
            if ts < bound:
                return self._labels[idx]
        return self._labels[-1]


class VectorSplitApplicator:
    """Apply a labeler to either filter or tag vector streams."""

    def __init__(
        self,
        *,
        labeler: HashLabeler | TimeLabeler,
        output: Literal["filter", "tag"] = "filter",
        keep: str | None = None,
        field: str = "__split__",
    ) -> None:
        self._labeler = labeler
        self._output = output
        self._keep = keep
        self._field = field

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        if self._output == "filter":
            yield from self._filter(stream)
            return
        yield from self._tag(stream)

    def _filter(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        if self._keep is None or _is_placeholder(self._keep):
            yield from stream
            return

        for sample in stream:
            label = self._labeler.label(sample.key, sample.features)
            if label == self._keep:
                yield sample

    def _tag(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            label = self._labeler.label(sample.key, sample.features)
            data = clone(sample.features.values)
            data[self._field] = label
            yield sample.with_features(Vector(values=data))


def _is_placeholder(value: str) -> bool:
    text = value.strip()
    return text.startswith("${") and text.endswith("}")


def build_labeler(cfg: SplitConfig) -> HashLabeler | TimeLabeler:
    if isinstance(cfg, TimeSplitConfig):
        return TimeLabeler(boundaries=cfg.boundaries, labels=cfg.labels)
    return HashLabeler(ratios=cfg.ratios, key=cfg.key, seed=cfg.seed)


def build_applicator(
    cfg: SplitConfig,
    keep: str | None = None,
) -> VectorSplitApplicator:
    labeler = build_labeler(cfg)
    selected = keep if keep is not None else getattr(cfg, "keep", None)
    return VectorSplitApplicator(labeler=labeler, output="filter", keep=selected)


def apply_split_stage(runtime, stream: Iterator[Sample]) -> Iterator[Sample]:
    """Apply project-configured split at the end of the vector pipeline.

    Reads `runtime.split` (set during bootstrap from project.split) and,
    when configured, applies a VectorSplitApplicator. When not configured,
    passes stream through.
    """
    cfg = getattr(runtime, "split", None)
    if not cfg:
        return stream
    keep = getattr(runtime, "split_keep", None)
    applicator = build_applicator(cfg, keep=keep)
    return applicator(stream)
