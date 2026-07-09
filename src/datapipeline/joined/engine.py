from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import product
from typing import Literal

from datapipeline.joined.model import JoinRecord, JoinedRow


_MISSING = object()


@dataclass(frozen=True)
class JoinInput:
    alias: str
    stream_id: str


@dataclass(frozen=True)
class JoinSpec:
    output_stream_id: str
    primary: JoinInput
    secondary_inputs: tuple[JoinInput, ...]
    fields: tuple[str, ...]
    mode: Literal["inner", "left"]


@dataclass
class JoinInputMetrics:
    rows: int
    matched_primary_rows: int = 0
    missed_primary_rows: int = 0

    @property
    def match_rate(self) -> float | None:
        primary_rows = self.matched_primary_rows + self.missed_primary_rows
        if primary_rows == 0:
            return None
        return self.matched_primary_rows / primary_rows


@dataclass
class JoinMetrics:
    primary_rows: int = 0
    output_rows: int = 0
    inputs: dict[str, JoinInputMetrics] = field(default_factory=dict)


def join_rows(
    inputs: Mapping[str, Iterator[JoinRecord]],
    spec: JoinSpec,
    metrics: JoinMetrics,
) -> Iterator[JoinedRow]:
    indexes: dict[str, dict[tuple[object, ...], list[JoinRecord]]] = {}
    for join_input in spec.secondary_inputs:
        index, row_count = _index_records(
            inputs[join_input.alias],
            spec=spec,
            join_input=join_input,
        )
        indexes[join_input.alias] = index
        metrics.inputs[join_input.alias] = JoinInputMetrics(
            rows=row_count,
        )

    for row_number, primary_record in enumerate(
        inputs[spec.primary.alias],
        start=1,
    ):
        metrics.primary_rows += 1
        key = _record_key(
            primary_record,
            spec=spec,
            join_input=spec.primary,
            row_number=row_number,
        )
        primary_time = _required_time(
            primary_record,
            spec=spec,
            row_number=row_number,
        )
        matches: list[Sequence[JoinRecord | None]] = []
        missing_match = False

        for join_input in spec.secondary_inputs:
            input_metrics = metrics.inputs[join_input.alias]
            records = indexes[join_input.alias].get(key)
            if records is None:
                missing_match = True
                input_metrics.missed_primary_rows += 1
                matches.append([None])
            else:
                input_metrics.matched_primary_rows += 1
                matches.append(records)

        if missing_match and spec.mode == "inner":
            continue

        for joined_records in product(*matches):
            values: dict[str, JoinRecord | None] = {
                spec.primary.alias: primary_record,
            }
            for join_input, joined_record in zip(
                spec.secondary_inputs,
                joined_records,
                strict=True,
            ):
                values[join_input.alias] = joined_record
            metrics.output_rows += 1
            yield JoinedRow(time=primary_time, values=values)


def _index_records(
    records: Iterator[JoinRecord],
    *,
    spec: JoinSpec,
    join_input: JoinInput,
) -> tuple[dict[tuple[object, ...], list[JoinRecord]], int]:
    index: dict[tuple[object, ...], list[JoinRecord]] = {}
    row_count = 0
    for row_count, record in enumerate(records, start=1):
        key = _record_key(
            record,
            spec=spec,
            join_input=join_input,
            row_number=row_count,
        )
        index.setdefault(key, []).append(record)
    return index, row_count


def _record_key(
    record: JoinRecord,
    *,
    spec: JoinSpec,
    join_input: JoinInput,
    row_number: int,
) -> tuple[object, ...]:
    return tuple(
        _required_field(
            record,
            spec=spec,
            join_input=join_input,
            row_number=row_number,
            field=field,
        )
        for field in spec.fields
    )


def _required_field(
    record: JoinRecord,
    *,
    spec: JoinSpec,
    join_input: JoinInput,
    row_number: int,
    field: str,
) -> object:
    if isinstance(record, Mapping):
        value = record.get(field, _MISSING)
    else:
        value = getattr(record, field, _MISSING)
    if value is _MISSING:
        raise ValueError(
            f"Joined stream '{spec.output_stream_id}' input "
            f"'{join_input.alias}' from '{join_input.stream_id}' row {row_number} "
            f"is missing join field '{field}'"
        )
    if field == "time":
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError(
                f"Joined stream '{spec.output_stream_id}' input "
                f"'{join_input.alias}' from '{join_input.stream_id}' row {row_number} "
                "join field 'time' must be a timezone-aware datetime"
            )
        return value.astimezone(timezone.utc)
    return value


def _required_time(
    record: JoinRecord,
    *,
    spec: JoinSpec,
    row_number: int,
) -> datetime:
    if isinstance(record, Mapping):
        value = record.get("time", _MISSING)
    else:
        value = getattr(record, "time", _MISSING)
    if value is _MISSING:
        raise ValueError(
            f"Joined stream '{spec.output_stream_id}' primary input "
            f"'{spec.primary.alias}' from '{spec.primary.stream_id}' row {row_number} "
            "is missing required time field"
        )
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(
            f"Joined stream '{spec.output_stream_id}' primary input "
            f"'{spec.primary.alias}' from '{spec.primary.stream_id}' row {row_number} "
            "must have a timezone-aware datetime time field"
        )
    return value.astimezone(timezone.utc)
