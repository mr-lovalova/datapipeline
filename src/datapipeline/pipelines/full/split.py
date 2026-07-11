import hashlib
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from datapipeline.domain.vector import Vector
from datapipeline.config.split import (
    HASH_SPLIT_FEATURE_PREFIX,
    SplitConfig,
    TimeSplitConfig,
)


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


def build_labeler(cfg: SplitConfig) -> HashLabeler | TimeLabeler:
    if isinstance(cfg, TimeSplitConfig):
        return TimeLabeler(boundaries=cfg.boundaries, labels=cfg.labels)
    return HashLabeler(ratios=cfg.ratios, key=cfg.key, seed=cfg.seed)
