from collections.abc import Iterator
from dataclasses import dataclass, field
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
from datapipeline.services.streams.join_plan import (
    JoinInputPlan,
    build_join_input_plans,
)
from datapipeline.transforms.utils import get_field, partition_key

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
        input_plans = build_join_input_plans(
            stream_id=self._stream_id,
            input_refs=refs,
            primary=primary,
            broadcast=set(join.broadcast),
            partition_by=self._runtime.registries.partition_by,
        )

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
                input_plans=input_plans,
                mode=join.mode,
                time_field=join.on,
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
    broadcast: bool
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

    def add_input(
        self, alias: str, *, stream_id: str, rows: int, broadcast: bool
    ) -> None:
        self.inputs[alias] = _InputJoinStats(
            stream_id=stream_id,
            rows=rows,
            broadcast=broadcast,
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
                    f"match_rate={match_rate:.1%} broadcast={str(stats.broadcast).lower()}"
                ),
                logger=logger,
                depth=depth,
            )


def _aligned_rows(
    inputs: dict[str, Iterator[TemporalRecord]],
    *,
    primary: str,
    input_plans: dict[str, JoinInputPlan],
    mode: str,
    time_field: str,
    stats: _JoinStats,
) -> Iterator[JoinedRow]:
    if mode not in {"inner", "left"}:
        raise ValueError(f"Unsupported join mode '{mode}'. Use 'inner' or 'left'.")

    primary_iter = inputs[primary]
    other_indexes = {}
    for alias, input_plan in input_plans.items():
        index, row_count = _index_records(
            inputs[alias],
            fields=input_plan.fields,
            time_field=time_field,
        )
        other_indexes[alias] = index
        stats.add_input(
            alias,
            stream_id=input_plan.stream_id,
            rows=row_count,
            broadcast=input_plan.broadcast,
        )

    for primary_record in primary_iter:
        stats.primary_rows += 1
        row_values: dict[str, TemporalRecord | None] = {primary: primary_record}
        missing = False
        for alias, index in other_indexes.items():
            key = _record_key(
                primary_record,
                fields=input_plans[alias].fields,
                time_field=time_field,
            )
            match = index.get(key)
            if match is None:
                missing = True
                stats.inputs[alias].missed += 1
            else:
                stats.inputs[alias].matched += 1
            row_values[alias] = match
        if missing and mode == "inner":
            continue
        stats.output_rows += 1
        yield JoinedRow(
            time=get_field(primary_record, time_field),
            values=row_values,
        )


def _index_records(
    records: Iterator[TemporalRecord],
    *,
    fields: tuple[str, ...],
    time_field: str,
) -> tuple[dict[tuple[Any, ...], TemporalRecord], int]:
    out: dict[tuple[Any, ...], TemporalRecord] = {}
    count = 0
    for record in records:
        count += 1
        out[_record_key(record, fields=fields, time_field=time_field)] = record
    return out, count


def _record_key(
    record: TemporalRecord,
    *,
    fields: tuple[str, ...],
    time_field: str,
) -> tuple[Any, ...]:
    return (get_field(record, time_field), *partition_key(record, list(fields)))
