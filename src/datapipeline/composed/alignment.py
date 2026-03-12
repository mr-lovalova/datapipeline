from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TypeAlias

from datapipeline.domain.record import TemporalRecord


JoinMode: TypeAlias = str


@dataclass(frozen=True)
class AlignedRow:
    time: datetime
    values: dict[str, TemporalRecord | None]


def align_many(
    inputs: Mapping[str, Iterator[TemporalRecord]],
    *,
    driver: str | None = None,
    join: JoinMode = "inner",
) -> Iterator[AlignedRow]:
    if join not in {"inner", "left"}:
        raise ValueError(f"Unsupported join mode '{join}'. Use 'inner' or 'left'.")
    if not inputs:
        return

    aliases = list(inputs.keys())
    driver_alias = driver or aliases[0]
    if driver_alias not in inputs:
        raise ValueError(
            f"Unknown driver alias '{driver_alias}'. Available: {aliases}"
        )

    if join == "inner":
        yield from _align_inner(inputs, aliases)
        return
    yield from _align_left(inputs, aliases, driver_alias)


def _align_inner(
    inputs: Mapping[str, Iterator[TemporalRecord]],
    aliases: list[str],
) -> Iterator[AlignedRow]:
    iterators = {alias: iter(inputs[alias]) for alias in aliases}
    current: dict[str, TemporalRecord | None] = {
        alias: next(iterators[alias], None) for alias in aliases
    }

    while all(record is not None for record in current.values()):
        assert all(record is not None for record in current.values())
        times = {alias: record.time for alias, record in current.items() if record is not None}
        min_time = min(times.values())
        max_time = max(times.values())
        if min_time == max_time:
            yield AlignedRow(
                time=min_time,
                values={alias: current[alias] for alias in aliases},
            )
            for alias in aliases:
                current[alias] = next(iterators[alias], None)
            continue

        for alias in aliases:
            record = current[alias]
            if record is not None and record.time < max_time:
                current[alias] = next(iterators[alias], None)


def _align_left(
    inputs: Mapping[str, Iterator[TemporalRecord]],
    aliases: list[str],
    driver_alias: str,
) -> Iterator[AlignedRow]:
    driver_iterator = iter(inputs[driver_alias])
    other_aliases = [alias for alias in aliases if alias != driver_alias]
    iterators = {alias: iter(inputs[alias]) for alias in other_aliases}
    current: dict[str, TemporalRecord | None] = {
        alias: next(iterators[alias], None) for alias in other_aliases
    }

    for driver_record in driver_iterator:
        row: dict[str, TemporalRecord | None] = {driver_alias: driver_record}
        t = driver_record.time
        for alias in other_aliases:
            record = current[alias]
            while record is not None and record.time < t:
                record = next(iterators[alias], None)
            if record is not None and record.time == t:
                row[alias] = record
                current[alias] = next(iterators[alias], None)
            else:
                row[alias] = None
                current[alias] = record
        yield AlignedRow(time=t, values=row)
