from typing import Any

from datapipeline.config.catalog import StreamConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.stream import RecordStream
from datapipeline.pipelines.record.inputs import close_iterator, open_input_records
from datapipeline.plugins import MAPPERS_EP
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args


class ManualStream(RecordStream[Any]):
    def __init__(self, runtime, stream_id: str, spec: StreamConfig):
        self._runtime = runtime
        self._stream_id = stream_id
        self._spec = spec

    def stream(self):
        context = PipelineContext(self._runtime)
        entrypoint, kwargs = _load_manual_mapper(self._spec, self._stream_id)
        with open_input_records(
            context, self._spec.input_refs(), owner=self._stream_id
        ) as input_iters:
            driver_key = _manual_driver_key(
                input_keys=list(input_iters.keys()),
                configured=kwargs.pop("driver", None),
                stream_id=self._stream_id,
            )
            records = entrypoint(
                inputs=input_iters,
                context=context,
                driver=driver_key,
                **kwargs,
            )
            try:
                for rec in records:
                    yield getattr(rec, "record", rec)
            finally:
                close_iterator(records)


def build_manual_stream(
    stream_id: str, spec: StreamConfig, runtime
) -> RecordStream[Any]:
    return ManualStream(runtime=runtime, stream_id=stream_id, spec=spec)


def _load_manual_mapper(spec: StreamConfig, stream_id: str):
    mapper = spec.map
    if not mapper or not mapper.entrypoint:
        raise ValueError(f"Manual stream '{stream_id}' requires map.entrypoint")
    return load_ep(MAPPERS_EP, mapper.entrypoint), normalize_args(mapper.args)


def _manual_driver_key(
    input_keys: list[str],
    configured: str | None,
    stream_id: str,
) -> str:
    if not input_keys:
        raise ValueError(f"Manual stream '{stream_id}' requires at least one input")
    driver_key = configured or input_keys[0]
    if driver_key not in input_keys:
        raise ValueError(
            f"Unknown manual driver '{driver_key}' for stream "
            f"'{stream_id}'. Available: {input_keys}"
        )
    return driver_key
