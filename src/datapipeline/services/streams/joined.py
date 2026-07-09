import logging
from collections.abc import Iterator

from datapipeline.cli.visuals.execution import emit_source_info
from datapipeline.cli.visuals.execution_context import current_dag_depth
from datapipeline.config.catalog import StreamConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.stream import RecordStream
from datapipeline.joined import JoinedRow
from datapipeline.joined.engine import JoinInput, JoinMetrics, JoinSpec, join_rows
from datapipeline.pipelines.record.inputs import open_input_records
from datapipeline.runtime import Runtime


logger = logging.getLogger(__name__)


class JoinedStream(RecordStream[JoinedRow]):
    def __init__(
        self,
        runtime: Runtime,
        stream_id: str,
        spec: StreamConfig,
    ) -> None:
        self._runtime = runtime
        self._spec = _join_spec(stream_id, spec)

    def stream(self) -> Iterator[JoinedRow]:
        context = PipelineContext(self._runtime)
        input_refs = {
            self._spec.primary.alias: self._spec.primary.stream_id,
        }
        for join_input in self._spec.secondary_inputs:
            input_refs[join_input.alias] = join_input.stream_id
        with open_input_records(
            context,
            input_refs,
            owner=self._spec.output_stream_id,
        ) as record_iters:
            metrics = JoinMetrics()
            yield from join_rows(record_iters, self._spec, metrics)
            _emit_join_metrics(self._spec, metrics)


def build_joined_stream(
    stream_id: str,
    spec: StreamConfig,
    runtime: Runtime,
) -> RecordStream[JoinedRow]:
    return JoinedStream(runtime=runtime, stream_id=stream_id, spec=spec)


def _join_spec(stream_id: str, config: StreamConfig) -> JoinSpec:
    input_refs = config.from_.join
    if input_refs is None:
        raise ValueError(f"Joined stream '{stream_id}' requires from.join")
    primary_alias = config.from_.primary
    if primary_alias is None:
        raise ValueError(f"Joined stream '{stream_id}' requires from.primary")
    fields = config.from_.on
    normalized_fields = (fields,) if isinstance(fields, str) else tuple(fields)
    primary = JoinInput(
        alias=primary_alias,
        stream_id=input_refs[primary_alias],
    )
    secondary_inputs = tuple(
        JoinInput(alias=alias, stream_id=input_stream_id)
        for alias, input_stream_id in input_refs.items()
        if alias != primary_alias
    )
    return JoinSpec(
        output_stream_id=stream_id,
        primary=primary,
        secondary_inputs=secondary_inputs,
        fields=normalized_fields,
        mode=config.from_.mode,
    )


def _emit_join_metrics(spec: JoinSpec, metrics: JoinMetrics) -> None:
    depth = current_dag_depth()
    emit_source_info(
        spec.output_stream_id,
        (
            f"join: primary={spec.primary.alias} on={','.join(spec.fields)} "
            f"mode={spec.mode} primary_rows={metrics.primary_rows} "
            f"output_rows={metrics.output_rows}"
        ),
        logger=logger,
        depth=depth,
    )
    for join_input in spec.secondary_inputs:
        input_metrics = metrics.inputs[join_input.alias]
        match_rate = input_metrics.match_rate
        match_rate_text = "n/a" if match_rate is None else f"{match_rate:.1%}"
        emit_source_info(
            spec.output_stream_id,
            (
                f"join.input: {join_input.alias}={join_input.stream_id} "
                f"rows={input_metrics.rows} "
                f"matched_primary_rows={input_metrics.matched_primary_rows} "
                f"missed_primary_rows={input_metrics.missed_primary_rows} "
                f"match_rate={match_rate_text}"
            ),
            logger=logger,
            depth=depth,
        )
