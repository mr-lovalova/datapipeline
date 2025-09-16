from typing import Iterator, Dict, Tuple, Any
from datapipeline.domain.record import Record


def canonical_key(tr: Any) -> Tuple:
    """
    Orders by (group_key, feature_id, kind_flag, time_key)

    kind_flag:
      0 = non-temporal scalar
      1 = temporal scalar (uses record.time)
      2 = temporal sequence (list of Records; uses last.time)
    """
    rec = getattr(tr, "record", None)
    kind_flag = 0
    time_key = 0

    # Sequence: list of Records — use the last element's time if present
    if isinstance(rec, list) and rec:
        last = rec[-1]
        t = getattr(last, "time", None)
        if t is not None:
            kind_flag = 2
            time_key = t
        else:
            kind_flag = 0
            time_key = 0
    else:
        t = getattr(rec, "time", None)
        if t is not None:
            kind_flag = 1
            time_key = t

    return (getattr(tr, "group_key", None), getattr(tr, "feature_id", None), kind_flag, time_key)


def assert_monotonic_time(stream) -> Iterator:
    """
    Dev-only guardrail: within each (group_key, feature_id),
    timestamps must be non-decreasing when present.
    """
    last_time: Dict[Tuple[str, str], object] = {}
    for r in stream:
        t = getattr(r.record, "time", None)
        if t is not None:
            k = (r.group_key, r.feature_id)
            prev = last_time.get(k)
            if prev is not None and t < prev:
                raise AssertionError(
                    f"time went backwards for {k}: {prev} -> {t}")
            last_time[k] = t
        yield r
