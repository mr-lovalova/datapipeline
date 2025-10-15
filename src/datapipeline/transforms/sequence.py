from __future__ import annotations

from collections import deque
import logging
from itertools import groupby
from typing import Iterator

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.utils.time import parse_timecode

logger = logging.getLogger(__name__)


class WindowTransformer:
    def __init__(
        self,
        size: int,
        stride: int = 1,
        *,
        tick: str | None = None,
    ) -> None:
        """Sliding windows over time-ordered feature streams.

        Parameters
        - size: window length in steps (int).
        - stride: step between windows (int number of steps).
        - tick: duration string denoting the expected cadence of the stream.
                Supports 's', 'm', 'h', 'd'. When provided, enforce completeness: only emit windows if
                consecutive records are exactly one tick apart; gaps reset the
                window. Examples: "1h", "10m". Optional.
        """

        self.size = int(size)
        self._tick_seconds: int | None = (
            int(parse_timecode(tick).total_seconds()) if tick else None
        )

        self.stride = int(stride)

        if self.size <= 0 or self.stride <= 0:
            raise ValueError("size and stride must be positive")

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return self.apply(stream)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        """Assumes input is pre-sorted by (feature_id, record.time).

        Produces sliding windows per feature_id. Each output carries a
        list[Record] in ``records``.
        """

        grouped = groupby(stream, key=lambda fr: fr.id)

        for id, records in grouped:
            window = deque(maxlen=self.size)
            step = 0
            last_time = None
            for fr in records:
                # Enforce completeness when configured and tick is known
                if self._tick_seconds is not None:
                    t = getattr(fr.record, "time", None)
                    if t is not None and last_time is not None:
                        delta = int((t - last_time).total_seconds())
                        if delta != self._tick_seconds:
                            logger.debug(
                                "sequence gap: feature_id=%s expected=%ss delta=%ss last=%s now=%s",
                                id,
                                self._tick_seconds,
                                delta,
                                last_time,
                                t,
                            )
                            window.clear()
                            step = 0
                    last_time = t

                window.append(fr)
                if len(window) == self.size and step % self.stride == 0:
                    yield FeatureRecordSequence(
                        records=[r.record for r in window],
                        id=id,
                    )
                step += 1
