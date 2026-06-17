from pathlib import Path

from datapipeline.config.tasks import OperationTask
from datapipeline.operations.persistence import RuntimeOutput, RuntimeOutputBatch
from datapipeline.services.materialize import (
    materialized_stream_rows,
    materialized_order,
    materialized_stream_config_paths,
    write_materialized_stream_config,
)


def materialize_stream_with_runtime(
    *,
    runtime,
    dataset,
    target,
    operation_task: OperationTask | None = None,
    visuals: str | None = None,
    **_,
) -> RuntimeOutputBatch:
    if target is None or target.destination is None:
        raise ValueError("materialize stream requires fs output")
    if target.format != "jsonl":
        raise ValueError("materialize stream supports only jsonl output")
    options = dict(operation_task.options if operation_task is not None else {})
    stream_id = str(options.get("stream") or "").strip()
    if not stream_id:
        raise ValueError("materialize stream requires options.stream")
    as_stream_id = str(options.get("as") or "").strip() or None
    runtime.sample_keys = dataset.sample_keys
    if as_stream_id is not None and not bool(options.get("force", False)):
        source_path, ingest_path = materialized_stream_config_paths(
            runtime=runtime,
            stream_id=as_stream_id,
            source_id=f"{as_stream_id}.source",
        )
        for path in (source_path, ingest_path):
            if path.exists():
                raise FileExistsError(f"{path} already exists; set options.force to overwrite")

    def on_complete(success: bool) -> None:
        if not success or as_stream_id is None:
            return
        write_materialized_stream_config(
            runtime=runtime,
            stream_id=as_stream_id,
            source_id=f"{as_stream_id}.source",
            output=Path(target.destination),
            ordered_by=materialized_order(runtime, stream_id),
            force=bool(options.get("force", False)),
        )

    return RuntimeOutputBatch(
        outputs=(
            RuntimeOutput(
                rows=materialized_stream_rows(runtime, stream_id),
                target=target,
                materialized_key=stream_id,
            ),
        ),
        on_complete=on_complete,
    )
