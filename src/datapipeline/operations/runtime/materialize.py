from dataclasses import replace
from pathlib import Path

from datapipeline.config.tasks import MaterializeStreamTask
from datapipeline.operations.persistence import RuntimeOutput, RuntimeOutputBatch
from datapipeline.services.materialize import (
    materialized_stream_rows,
    materialized_order,
    materialized_feature_id_by,
    materialized_partition_by,
    materialized_metadata_path,
    materialized_stream_config_paths,
    write_materialized_stream_metadata,
    write_materialized_stream_config,
)


class _CountingRows:
    def __init__(self, rows):
        self._rows = rows
        self.count = 0

    def __iter__(self):
        for row in self._rows:
            self.count += 1
            yield row


def materialize_stream_with_runtime(
    *,
    runtime,
    dataset,
    target,
    operation_task: MaterializeStreamTask,
    visuals: str | None = None,
    **_,
) -> RuntimeOutputBatch:
    if target is None or target.destination is None:
        raise ValueError("materialize stream requires fs output")
    if target.format != "jsonl":
        raise ValueError("materialize stream supports only jsonl output")
    options = operation_task.options
    stream_id = options.stream
    as_stream_id = options.as_stream_id
    force = options.force
    runtime.sample_keys = dataset.sample_keys
    output_path = Path(target.destination)
    metadata_path = materialized_metadata_path(output_path)
    if not force and output_path.exists():
        raise FileExistsError(
            f"{output_path} already exists; set options.force to overwrite"
        )
    if not force and metadata_path.exists():
        raise FileExistsError(
            f"{metadata_path} already exists; set options.force to overwrite"
        )
    if not force and as_stream_id is not None:
        source_path, ingest_path = materialized_stream_config_paths(
            runtime=runtime,
            stream_id=as_stream_id,
            source_id=f"{as_stream_id}.source",
        )
        for path in (source_path, ingest_path):
            if path.exists():
                raise FileExistsError(
                    f"{path} already exists; set options.force to overwrite"
                )
    ordered_by = materialized_order(runtime, stream_id)
    rows = _CountingRows(materialized_stream_rows(runtime, stream_id))
    materialize_target = replace(target, overwrite=force)

    def on_complete(success: bool) -> None:
        if not success:
            return
        write_materialized_stream_metadata(
            output=output_path,
            rows=rows.count,
            partition_by=materialized_partition_by(runtime, stream_id),
            feature_id_by=materialized_feature_id_by(runtime, stream_id),
            ordered_by=ordered_by,
            force=force,
        )
        if as_stream_id is None:
            return
        write_materialized_stream_config(
            runtime=runtime,
            stream_id=as_stream_id,
            source_id=f"{as_stream_id}.source",
            output=output_path,
            partition_by=materialized_partition_by(runtime, stream_id),
            feature_id_by=materialized_feature_id_by(runtime, stream_id),
            ordered_by=ordered_by,
            force=force,
        )

    return RuntimeOutputBatch(
        outputs=(
            RuntimeOutput(
                rows=rows,
                target=materialize_target,
                materialized_key=stream_id,
            ),
        ),
        on_complete=on_complete,
    )
