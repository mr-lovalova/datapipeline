import logging
import math
from itertools import groupby
from typing import Iterator

from datapipeline.domain.feature import FeatureRecord
from datapipeline.transforms.utils import is_missing
logger = logging.getLogger(__name__)


class StreamLint:
    """Validate a feature stream and emit actionable hints.

    Parameters
    - mode: 'warn' (default) logs warnings; 'error' raises on first violation
    - tick: optional cadence (e.g. '1h', '10m'); when set, check regularity
    - check_missing: flag missing values (value is None/NaN)
    - check_regular: flag gaps vs. expected tick
    - check_duplicates: flag multiple records with same timestamp
    - check_order: flag out-of-order timestamps within a feature stream
    - check_finite: flag non-finite values (NaN/Inf)
    """

    def __init__(
        self,
        *,
        mode: str = "warn",
        tick: str | None = None,
        check_missing: bool = True,
        check_regular: bool = True,
        check_duplicates: bool = True,
        check_order: bool = True,
        check_finite: bool = True,
    ) -> None:
        self.mode = mode
        self.tick = tick
        self.check_missing = check_missing
        self.check_regular = check_regular
        self.check_duplicates = check_duplicates
        self.check_order = check_order
        self.check_finite = check_finite

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
                v = getattr(fr.record, "value", None)

                # Check ordering
                if self.check_order and last_time is not None and t is not None and t < last_time:
                    self._violation(
                        f"out-of-order timestamp for feature '{fid}': {t} < {last_time}. "
                        f"Consider sorting upstream or fixing loader."
                    )

                # Check duplicates
                if self.check_duplicates and t in seen_times:
                    self._violation(
                        f"duplicate timestamp for feature '{fid}' at {t}. "
                        f"Consider a granularity transform (first/last/mean/median)."
                    )
                seen_times.add(t)

                # Check missing / non-finite
                if self.check_missing and is_missing(v):
                    self._violation(
                        f"missing value for feature '{fid}' at {t}. "
                        f"Consider using a fill transform."
                    )
                if self.check_finite and isinstance(v, float) and not math.isfinite(v):
                    self._violation(
                        f"non-finite value for feature '{fid}' at {t}: {v}. "
                        f"Consider filtering or scaling."
                    )

                # Regularity check requires explicit tick; done at stream layer via ensure_ticks normally
                if self.check_regular and self.tick and last_time is not None and t is not None:
                    # Lazy import to avoid cycle
                    from datapipeline.utils.time import parse_timecode

                    step = parse_timecode(self.tick)
                    expect = last_time + step
                    if t != expect and t > expect:
                        self._violation(
                            f"skipped tick(s) for feature '{fid}': expected {expect}, got {t}. "
                            f"Consider using ensure_ticks."
                        )

                last_time = t
                yield fr
