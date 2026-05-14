from collections.abc import Iterator
from dataclasses import dataclass, field
from itertools import product
import logging
from typing import Any

from datapipeline.cli.visuals.execution import emit_source_info
from datapipeline.cli.visuals.execution_context import current_dag_depth
from datapipeline.config.catalog import StreamConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.stream import RecordStream
from datapipeline.joined.model import JoinedRow
from datapipeline.pipelines.record.inputs import open_input_records
from datapipeline.transforms.utils import get_field

logger = logging.getLogger(__name__)


class JoinedStream(RecordStream[JoinedRow]):
    def __init__(self, runtime, stream_id: str, spec: StreamConfig):
        self._runtime = runtime
        self._stream_id = stream_id
        self._spec = spec

    def stream(self):
        context = PipelineContext(self._runtime)
        join = self._spec.from_
        primary = join.primary
        if primary is None:
            raise ValueError(f"Joined stream '{self._stream_id}' requires from.primary")

        refs = self._spec.input_refs()
        input_refs = {alias: ref for alias, ref in refs.items() if alias != primary}

        with open_input_records(context, refs, owner=self._stream_id) as record_iters:
            stats = _JoinStats(
                stream_id=self._stream_id,
                primary=primary,
                mode=join.mode,
                on=join.on,
            )
            rows = _aligned_rows(
                record_iters,
                primary=primary,
                input_refs=input_refs,
                mode=join.mode,
                join_fields=_join_fields(join.on),
                stats=stats,
            )
            yield from rows
            stats.emit()


def build_joined_stream(
    stream_id: str, spec: StreamConfig, runtime
) -> RecordStream[JoinedRow]:
    return JoinedStream(runtime=runtime, stream_id=stream_id, spec=spec)


@dataclass
class _InputJoinStats:
    stream_id: str
    rows: int
    matched: int = 0
    missed: int = 0


@dataclass
class _JoinStats:
    stream_id: str
    primary: str
    mode: str
    on: str
    primary_rows: int = 0
    output_rows: int = 0
    inputs: dict[str, _InputJoinStats] = field(default_factory=dict)

    def add_input(self, alias: str, *, stream_id: str, rows: int) -> None:
        self.inputs[alias] = _InputJoinStats(
            stream_id=stream_id,
            rows=rows,
        )

    def emit(self) -> None:
        depth = current_dag_depth()
        emit_source_info(
            self.stream_id,
            (
                f"join: primary={self.primary} on={self.on} mode={self.mode} "
                f"primary_rows={self.primary_rows} output_rows={self.output_rows}"
            ),
            logger=logger,
            depth=depth,
        )
        for alias, stats in self.inputs.items():
            total = stats.matched + stats.missed
            match_rate = stats.matched / total if total else 1.0
            emit_source_info(
                self.stream_id,
                (
                    f"join.input: {alias}={stats.stream_id} rows={stats.rows} "
                    f"matched={stats.matched} missed={stats.missed} "
                    f"match_rate={match_rate:.1%}"
                ),
                logger=logger,
                depth=depth,
            )


def _aligned_rows(
    inputs: dict[str, Iterator[TemporalRecord]],
    *,
    primary: str,
    input_refs: dict[str, str],
    mode: str,
    join_fields: tuple[str, ...],
    stats: _JoinStats,
) -> Iterator[JoinedRow]:
    if mode not in {"inner", "left"}:
        raise ValueError(f"Unsupported join mode '{mode}'. Use 'inner' or 'left'.")

    indexes: dict[str, dict[tuple[Any, ...], list[TemporalRecord]]] = {}
    for alias, stream_id in input_refs.items():
        index, row_count = _index_records(inputs[alias], fields=join_fields)
        indexes[alias] = index
        stats.add_input(
            alias,
            stream_id=stream_id,
            rows=row_count,
        )

    for primary_record in inputs[primary]:
        stats.primary_rows += 1
        key = _record_key(primary_record, fields=join_fields)
        aliases: list[str] = []
        matches: list[list[TemporalRecord | None]] = []
        missing = False

        for alias in input_refs:
            records = indexes[alias].get(key)
            aliases.append(alias)
            if records is None:
                missing = True
                stats.inputs[alias].missed += 1
                matches.append([None])
            else:
                stats.inputs[alias].matched += len(records)
                matches.append(records)

        if missing and mode == "inner":
            continue

        row_values: dict[str, TemporalRecord | None]
        for joined_records in product(*matches):
            row_values = {primary: primary_record}
            row_values.update(zip(aliases, joined_records, strict=True))
            stats.output_rows += 1
            yield JoinedRow(time=primary_record.time, values=row_values)


def _index_records(
    records: Iterator[TemporalRecord],
    *,
    fields: tuple[str, ...],
) -> tuple[dict[tuple[Any, ...], list[TemporalRecord]], int]:
    out: dict[tuple[Any, ...], list[TemporalRecord]] = {}
    count = 0
    for record in records:
        count += 1
        key = _record_key(record, fields=fields)
        out.setdefault(key, []).append(record)
    return out, count


def _record_key(record: TemporalRecord, *, fields: tuple[str, ...]) -> tuple[Any, ...]:
    return tuple(get_field(record, field) for field in fields)


def _join_fields(on: str | list[str]) -> tuple[str, ...]:
    if isinstance(on, str):
        return (on,)
    return tuple(on)
