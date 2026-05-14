from typing import Any

from datapipeline.cache import cached_record_stream
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.stream import RecordStream


class PreparedStreamRef(RecordStream[Any]):
    def __init__(self, runtime, upstream_id: str):
        self._runtime = runtime
        self._upstream_id = upstream_id

    def stream(self):
        return cached_record_stream(PipelineContext(self._runtime), self._upstream_id)


def build_prepared_stream_ref(upstream_id: str, runtime) -> RecordStream[Any]:
    return PreparedStreamRef(runtime=runtime, upstream_id=upstream_id)
