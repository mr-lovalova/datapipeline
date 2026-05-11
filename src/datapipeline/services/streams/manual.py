from collections.abc import Iterator
from typing import Any

from datapipeline.config.catalog import ContractConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.stream import RecordStream
from datapipeline.plugins import MAPPERS_EP
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import normalize_args

from .common import close_iterator, resolve_input_streams, unwrap_records


class ManualStream(RecordStream[Any]):
    def __init__(self, runtime, stream_id: str, spec: ContractConfig):
        self._runtime = runtime
        self._stream_id = stream_id
        self._spec = spec

    def stream(self):
        context = PipelineContext(self._runtime)
        resolved_inputs = resolve_input_streams(context, self._spec)
        upstream_iters: list[Iterator[Any]] = []
        input_iters: dict[str, Iterator[Any]] = {}
        for alias, iterator in resolved_inputs.items():
            upstream_iter = iter(iterator)
            upstream_iters.append(upstream_iter)
            input_iters[alias] = unwrap_records(upstream_iter)

        mapper = self._spec.mapper
        if not mapper or not mapper.entrypoint:
            raise ValueError(
                f"Manual stream '{self._stream_id}' requires mapper.entrypoint"
            )
        entrypoint = load_ep(MAPPERS_EP, mapper.entrypoint)
        kwargs = normalize_args(mapper.args)

        input_keys = list(input_iters.keys())
        if not input_keys:
            return
        driver_key = kwargs.pop("driver", None) or input_keys[0]
        if driver_key not in input_iters:
            raise ValueError(
                f"Unknown manual driver '{driver_key}' for stream "
                f"'{self._stream_id}'. Available: {input_keys}"
            )

        try:
            records = entrypoint(
                inputs=input_iters,
                context=context,
                driver=driver_key,
                **kwargs,
            )
        except TypeError as exc:
            raise TypeError(
                "Manual mapper must use signature "
                "`mapper(inputs, *, context, driver, **params)`"
            ) from exc

        try:
            for rec in records:
                yield getattr(rec, "record", rec)
        finally:
            close_iterator(records)
            for iterator in upstream_iters:
                close_iterator(iterator)


def build_manual_stream(
    stream_id: str, spec: ContractConfig, runtime
) -> RecordStream[Any]:
    return ManualStream(runtime=runtime, stream_id=stream_id, spec=spec)
