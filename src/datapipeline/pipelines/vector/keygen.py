from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from datapipeline.artifacts.models import SampleDomainEntry
from datapipeline.domain.variable import VariableRecord, VariableSequence
from datapipeline.utils.time import (
    floor_time_to_cadence,
    parse_cadence,
)


_SampleWindow = tuple[tuple[Any, ...], datetime, datetime]


@dataclass(frozen=True)
class RectangularKeyPlan:
    start: datetime
    end: datetime
    step: timedelta
    windows: tuple[_SampleWindow, ...]

    @property
    def total(self) -> int | None:
        if not self.windows:
            return 0
        if not self._has_consistent_time_lattice():
            return None

        last_plan_index = (self.end - self.start) // self.step
        total = 0
        for _, window_start, window_end in self.windows:
            first_index, remainder = divmod(window_start - self.start, self.step)
            if remainder:
                first_index += 1
            first_index = max(0, first_index)
            last_index = min(
                last_plan_index,
                (window_end - self.start) // self.step,
            )
            total += max(0, last_index - first_index + 1)
        return total

    def keys(self) -> Iterator[tuple]:
        current = self.start
        while current <= self.end:
            for key_values, window_start, window_end in self.windows:
                if window_start <= current <= window_end:
                    yield (current, *key_values)
            current += self.step

    def _has_consistent_time_lattice(self) -> bool:
        origin_timezone = self.start.tzinfo
        # Fixed-offset origins advance in absolute time. Dynamic zones advance in
        # wall time, so their bounds must share the origin's timezone object.
        if isinstance(origin_timezone, timezone):
            return self.end.tzinfo is not None and all(
                window_start.tzinfo is not None and window_end.tzinfo is not None
                for _, window_start, window_end in self.windows
            )
        return self.end.tzinfo is origin_timezone and all(
            window_start.tzinfo is origin_timezone
            and window_end.tzinfo is origin_timezone
            for _, window_start, window_end in self.windows
        )


def group_key_for(
    item: VariableRecord | VariableSequence,
    cadence: timedelta,
) -> tuple:
    return (floor_time_to_cadence(item.time, cadence), *item.entity_key)


def window_key_plan(
    start: datetime | None,
    end: datetime | None,
    cadence: str | None,
) -> RectangularKeyPlan | None:
    if start is None or end is None or cadence is None:
        return None
    step = parse_cadence(cadence)
    window_start = floor_time_to_cadence(start, step)
    window_end = floor_time_to_cadence(end, step)
    return RectangularKeyPlan(
        start=window_start,
        end=window_end,
        step=step,
        windows=(((), window_start, window_end),),
    )


def sample_domain_key_plan(
    start: datetime | None,
    end: datetime | None,
    cadence: str,
    sample_keys: Sequence[str],
    domain: Sequence[SampleDomainEntry],
) -> RectangularKeyPlan | None:
    plan = window_key_plan(start, end, cadence)
    if plan is None:
        return None
    if not sample_keys:
        return plan

    prepared: list[_SampleWindow] = []
    for entry in domain:
        if len(entry.key) != len(sample_keys):
            raise ValueError(
                "Vector metadata sample-domain key length does not match sample.keys."
            )
        domain_start = max(
            plan.start,
            floor_time_to_cadence(entry.start, plan.step),
        )
        domain_end = min(
            plan.end,
            floor_time_to_cadence(entry.end, plan.step),
        )
        if domain_start <= domain_end:
            prepared.append((tuple(entry.key), domain_start, domain_end))
    prepared.sort(key=lambda item: item[0])
    return RectangularKeyPlan(
        start=plan.start,
        end=plan.end,
        step=plan.step,
        windows=tuple(prepared),
    )
