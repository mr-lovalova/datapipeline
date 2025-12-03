import logging
from datetime import timedelta
from itertools import groupby
from typing import Iterator

from datapipeline.domain.feature import FeatureRecord
from datapipeline.utils.time import parse_timecode


logger = logging.getLogger(__name__)


class StreamLint:
    """Validate structural properties of a feature stream (order, cadence, duplicates).

    Parameters
    - mode: 'warn' (default) logs warnings; 'error' raises on first violation
    - tick: optional cadence (e.g. '1h', '10m'); when set, check regularity
    """

    def __init__(
        self,
        *,
        mode: str = "warn",
        tick: str | None = None,
    ) -> None:
        self.mode = mode
        self.tick = tick

        # Pre-compute tick step in seconds when provided to avoid repeated parsing.
        self._tick_seconds: int | None = None
        if self.tick:
            try:
                self._tick_seconds = int(parse_timecode(self.tick).total_seconds())
            except Exception:
                logger.warning(
                    "StreamLint: invalid tick %r (cadence checks disabled)", self.tick
                )
                self._tick_seconds = None

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return self.apply(stream)

    def _violation(self, msg: str) -> None:
        if self.mode == "error":
            raise ValueError(msg)
        logger.warning(msg)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        # Group by base feature id to keep state local
        for fid, records in groupby(stream, key=lambda fr: fr.id):
            last_time = None
            seen_times: set = set()
            for fr in records:
                t = getattr(fr.record, "time", None)

                # Check ordering
                if last_time is not None and t is not None and t < last_time:
                    self._violation(
                        f"out-of-order timestamp for feature '{fid}': {t} < {last_time}. "
                        f"Consider sorting upstream or fixing loader."
                    )

                # Check duplicates
                if t in seen_times:
                    self._violation(
                        f"duplicate timestamp for feature '{fid}' at {t}. "
                        f"Consider a granularity transform (first/last/mean/median)."
                    )
                seen_times.add(t)

                # Regularity check requires explicit tick; done at stream layer via ensure_cadence normally
                if (
                    self._tick_seconds
                    and last_time is not None
                    and t is not None
                ):
                    expect = last_time + timedelta(seconds=self._tick_seconds)
                    if t != expect and t > expect:
                        self._violation(
                            f"skipped tick(s) for feature '{fid}': expected {expect}, got {t}. "
                            f"Consider using ensure_cadence."
                        )

                last_time = t
                yield fr
