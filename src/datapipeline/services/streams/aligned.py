from collections.abc import Iterator
from typing import Any

from datapipeline.alignment.engine import align_streams
from datapipeline.config.catalog import StreamConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.stream import RecordStream
from datapipeline.pipelines.record.streams import open_record_stream
from datapipeline.plugins import MAPPERS_EP
from datapipeline.runtime import Runtime
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args


class AlignedStream(RecordStream[tuple[Any, ...]]):
    def __init__(
        self,
        runtime: Runtime,
        input_streams: tuple[str, ...],
        partition_by: str | list[str] | None,
    ) -> None:
        self._runtime = runtime
        self._input_streams = input_streams
        self._partition_by = partition_by

    def stream(self) -> Iterator[tuple[Any, ...]]:
        context = PipelineContext(self._runtime)
        records = [
            open_record_stream(context, stream_id) for stream_id in self._input_streams
        ]
        try:
            inputs = list(zip(self._input_streams, records, strict=True))
            yield from align_streams(
                inputs,
                partition_by=self._partition_by,
                buffer_bytes=self._runtime.execution.sort_buffer_bytes,
            )
        finally:
            for stream in records:
                stream.close()


def build_aligned_mapper(spec: StreamConfig):
    mapper = spec.map
    if mapper is None:
        raise ValueError(f"Aligned stream '{spec.id}' requires map.entrypoint")
    function = load_ep(MAPPERS_EP, mapper.entrypoint)
    args = normalize_args(mapper.args)

    def map_records(rows: Iterator[tuple[Any, ...]]) -> Iterator[Any]:
        for records in rows:
            record = function(*records, **args)
            if record is not None:
                yield record

    return map_records
