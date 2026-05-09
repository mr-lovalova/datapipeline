from collections.abc import Iterator
from typing import Any

from datapipeline.config.catalog import ContractConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.record import TemporalRecord
from datapipeline.joined.model import JoinedRow
from datapipeline.parsers.identity import IdentityParser
from datapipeline.plugins import MAPPERS_EP
from datapipeline.sources.models.loader import BaseDataLoader
from datapipeline.sources.models.source import Source
from datapipeline.transforms.utils import get_field, partition_key
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args

from .common import close_iterator, resolve_input_streams, unwrap_records


class JoinedLoader(BaseDataLoader):
    def __init__(self, runtime, stream_id: str, spec: ContractConfig):
        self._runtime = runtime
        self._stream_id = stream_id
        self._spec = spec

    def load(self):
        context = PipelineContext(self._runtime)
        join = self._spec.join
        mapper = self._spec.mapper
        if join is None:
            raise ValueError(f"Joined stream '{self._stream_id}' requires join config")
        if not mapper or not mapper.entrypoint:
            raise ValueError(
                f"Joined stream '{self._stream_id}' requires mapper.entrypoint"
            )

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

        entrypoint = load_ep(MAPPERS_EP, mapper.entrypoint)
        kwargs = normalize_args(mapper.args)

        try:
            rows = _aligned_rows(
                record_iters,
                input_refs=input_refs,
                partition_by=self._runtime.registries.partition_by,
                primary=join.primary,
                mode=join.mode,
                broadcast=set(join.broadcast),
                time_field=join.on,
            )
            for row in rows:
                mapped = entrypoint(row, context=context, **kwargs)
                yield from _iter_mapped(mapped)
        finally:
            for iterator in upstream_iters:
                close_iterator(iterator)

    def count(self):
        return None

    def progress_visible(self) -> bool:
        return False


def build_joined_source(stream_id: str, spec: ContractConfig, runtime) -> Source:
    return Source(
        loader=JoinedLoader(runtime=runtime, stream_id=stream_id, spec=spec),
        parser=IdentityParser(),
    )


def _iter_mapped(value: Any) -> Iterator[Any]:
    if value is None:
        return
    if isinstance(value, TemporalRecord):
        yield value
        return
    if isinstance(value, Iterator):
        for item in value:
            if item is not None:
                yield getattr(item, "record", item)
        return
    yield getattr(value, "record", value)


def _input_refs(spec: ContractConfig) -> dict[str, str]:
    return {
        alias: ref
        for alias, ref in (
            ContractConfig.parse_input_spec(item) for item in (spec.inputs or [])
        )
    }


def _aligned_rows(
    inputs: dict[str, Iterator[TemporalRecord]],
    *,
    input_refs: dict[str, str],
    partition_by,
    primary: str,
    mode: str,
    broadcast: set[str],
    time_field: str,
) -> Iterator[JoinedRow]:
    if mode not in {"inner", "left"}:
        raise ValueError(f"Unsupported join mode '{mode}'. Use 'inner' or 'left'.")

    primary_partition = partition_by.get(input_refs[primary])
    primary_iter = inputs[primary]
    other_indexes = {
        alias: _index_records(
            inputs[alias],
            fields=_join_fields(
                primary_partition=primary_partition,
                other_partition=partition_by.get(input_refs[alias]),
                broadcast=alias in broadcast,
            ),
            time_field=time_field,
        )
        for alias in inputs
        if alias != primary
    }

    for primary_record in primary_iter:
        row_values: dict[str, TemporalRecord | None] = {primary: primary_record}
        missing = False
        for alias, index in other_indexes.items():
            fields = _join_fields(
                primary_partition=primary_partition,
                other_partition=partition_by.get(input_refs[alias]),
                broadcast=alias in broadcast,
            )
            key = _record_key(primary_record, fields=fields, time_field=time_field)
            match = index.get(key)
            if match is None:
                missing = True
            row_values[alias] = match
        if missing and mode == "inner":
            continue
        yield JoinedRow(
            time=get_field(primary_record, time_field),
            values=row_values,
        )


def _index_records(
    records: Iterator[TemporalRecord],
    *,
    fields: tuple[str, ...],
    time_field: str,
) -> dict[tuple[Any, ...], TemporalRecord]:
    out: dict[tuple[Any, ...], TemporalRecord] = {}
    for record in records:
        out[_record_key(record, fields=fields, time_field=time_field)] = record
    return out


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
