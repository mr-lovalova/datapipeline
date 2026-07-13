from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.domain.stream import RecordStream
from datapipeline.pipelines.record.streams import open_record_stream


class PreparedStreamRef(RecordStream[Any]):
    def __init__(self, runtime, upstream_id: str):
        self._runtime = runtime
        self._upstream_id = upstream_id

    def stream(self):
        return open_record_stream(PipelineContext(self._runtime), self._upstream_id)


def build_prepared_stream_ref(upstream_id: str, runtime) -> RecordStream[Any]:
    return PreparedStreamRef(runtime=runtime, upstream_id=upstream_id)
