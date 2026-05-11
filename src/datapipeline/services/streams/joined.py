from collections.abc import Iterator
from dataclasses import dataclass, field
import logging
from typing import Any

from datapipeline.cli.visuals.execution import emit_source_info
from datapipeline.cli.visuals.execution_context import current_dag_depth
from datapipeline.config.catalog import ContractConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.stream import RecordStream
from datapipeline.joined.model import JoinedRow
from datapipeline.transforms.utils import get_field, partition_key

from .common import close_iterator, resolve_input_streams, unwrap_records

logger = logging.getLogger(__name__)


class JoinedStream(RecordStream[JoinedRow]):
    def __init__(self, runtime, stream_id: str, spec: ContractConfig):
        self._runtime = runtime
        self._stream_id = stream_id
        self._spec = spec

    def stream(self):
        context = PipelineContext(self._runtime)
        join = self._spec.join
        if join is None:
            raise ValueError(f"Joined stream '{self._stream_id}' requires join config")

        input_refs = _input_refs(self._spec)
        if join.primary not in input_refs:
            raise ValueError(
                f"Joined stream '{self._stream_id}' join.primary '{join.primary}' "
                f"is not an input alias"
            )

        resolved_inputs = resolve_input_streams(context, self._spec)
        upstream_iters: list[Iterator[Any]] = []
        record_iters: dict[str, Iterator[Any]] = {}
        for alias, iterator in resolved_inputs.items():
            upstream_iter = iter(iterator)
            upstream_iters.append(upstream_iter)
            record_iters[alias] = unwrap_records(upstream_iter)

        try:
            stats = _JoinStats(
                stream_id=self._stream_id,
                primary=join.primary,
                mode=join.mode,
                on=join.on,
            )
            rows = _aligned_rows(
                record_iters,
                input_refs=input_refs,
                partition_by=self._runtime.registries.partition_by,
                primary=join.primary,
                mode=join.mode,
                broadcast=set(join.broadcast),
                time_field=join.on,
                stats=stats,
            )
            yield from rows
            stats.emit()
        finally:
            for iterator in upstream_iters:
                close_iterator(iterator)


def build_joined_stream(
    stream_id: str, spec: ContractConfig, runtime
) -> RecordStream[JoinedRow]:
    return JoinedStream(runtime=runtime, stream_id=stream_id, spec=spec)


def _input_refs(spec: ContractConfig) -> dict[str, str]:
    return {
        alias: ref
        for alias, ref in (
            ContractConfig.parse_input_spec(item) for item in (spec.inputs or [])
        )
    }


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
    input_refs: dict[str, str],
    partition_by,
    primary: str,
    mode: str,
    broadcast: set[str],
    time_field: str,
    stats: _JoinStats | None = None,
) -> Iterator[JoinedRow]:
    if mode not in {"inner", "left"}:
        raise ValueError(f"Unsupported join mode '{mode}'. Use 'inner' or 'left'.")

    primary_partition = partition_by.get(input_refs[primary])
    primary_iter = inputs[primary]
    other_indexes = {}
    other_fields = {}
    for alias in inputs:
        if alias == primary:
            continue
        fields = _join_fields(
            primary_partition=primary_partition,
            other_partition=partition_by.get(input_refs[alias]),
            broadcast=alias in broadcast,
        )
        index, row_count = _index_records(
            inputs[alias],
            fields=fields,
            time_field=time_field,
        )
        other_indexes[alias] = index
        other_fields[alias] = fields
        if stats is not None:
            stats.add_input(
                alias,
                stream_id=input_refs[alias],
                rows=row_count,
                broadcast=alias in broadcast,
            )

    for primary_record in primary_iter:
        if stats is not None:
            stats.primary_rows += 1
        row_values: dict[str, TemporalRecord | None] = {primary: primary_record}
        missing = False
        for alias, index in other_indexes.items():
            key = _record_key(
                primary_record,
                fields=other_fields[alias],
                time_field=time_field,
            )
            match = index.get(key)
            if match is None:
                missing = True
                if stats is not None:
                    stats.inputs[alias].missed += 1
            elif stats is not None:
                stats.inputs[alias].matched += 1
            row_values[alias] = match
        if missing and mode == "inner":
            continue
        if stats is not None:
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


def _partition_tuple(value: str | list[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _join_fields(
    *,
    primary_partition: str | list[str] | None,
    other_partition: str | list[str] | None,
    broadcast: bool,
) -> tuple[str, ...]:
    primary_fields = _partition_tuple(primary_partition)
    other_fields = _partition_tuple(other_partition)
    if broadcast:
        return other_fields
    if other_fields != primary_fields:
        raise ValueError(
            f"Joined input partition_by={list(other_fields)} is incompatible "
            f"with primary partition_by={list(primary_fields)}"
        )
    return primary_fields
