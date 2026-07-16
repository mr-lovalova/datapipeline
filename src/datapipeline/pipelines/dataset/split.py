import hashlib
from bisect import bisect_right
from datetime import datetime
from typing import Any

from datapipeline.config.dataset.split import (
    HashSplitConfig,
    SplitConfig,
    TimeSplitConfig,
)
from datapipeline.utils.time import parse_datetime


class HashLabeler:
    """Assign deterministic labels from a validated hash split."""

    def __init__(self, config: HashSplitConfig) -> None:
        total = 0.0
        thresholds: list[tuple[float, str]] = []
        for label, frac in config.ratios.items():
            total += frac
            thresholds.append((total, label))
        self._thresholds = thresholds
        self._seed = config.seed

    @staticmethod
    def _hash_token(token: str, seed: int) -> float:
        b = (str(seed) + "|" + token).encode("utf-8")
        digest = hashlib.sha256(b).digest()
        num = int.from_bytes(digest[:8], "big")
        return (num % (1 << 53)) / float(1 << 53)

    def label(self, group_key: Any) -> str:
        token = repr(group_key)
        r = self._hash_token(token, self._seed)
        for thresh, label in self._thresholds:
            if r < thresh:
                return label
        return self._thresholds[-1][1]


class TimeLabeler:
    """Assign the interval containing a timestamp."""

    def __init__(self, config: TimeSplitConfig) -> None:
        self._boundaries = tuple(
            parse_datetime(interval.until)
            for interval in config.intervals
            if interval.until is not None
        )
        self._labels = tuple(interval.id for interval in config.intervals)

    def label(self, group_key: Any) -> str:
        key = group_key[0] if isinstance(group_key, (list, tuple)) else group_key
        if isinstance(key, datetime):
            timestamp = (
                key if key.tzinfo is not None else parse_datetime(key.isoformat())
            )
        elif isinstance(key, str):
            timestamp = parse_datetime(key)
        else:
            raise TypeError("time split keys must be datetimes or ISO-8601 strings")
        return self._labels[bisect_right(self._boundaries, timestamp)]


def build_labeler(config: SplitConfig) -> HashLabeler | TimeLabeler:
    if isinstance(config, TimeSplitConfig):
        return TimeLabeler(config)
    return HashLabeler(config)
