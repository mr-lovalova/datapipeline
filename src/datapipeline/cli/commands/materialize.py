import logging
from pathlib import Path

from datapipeline.cli.visuals.runner import run_with_backend
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.materialize import (
    materialize_stream_to_path,
    validate_materialize_output_path,
)
from datapipeline.services.path_policy import resolve_workspace_path

logger = logging.getLogger(__name__)


def handle(
    *,
    project: str,
    stream_id: str,
    output: str,
    as_stream_id: str | None,
    force: bool,
    visuals: str | None,
    heartbeat_interval_seconds: float | None,
    workspace: WorkspaceContext | None,
) -> None:
    project_path = Path(project).resolve()
    output_path = resolve_workspace_path(
        output,
        workspace.root if workspace is not None else None,
    )
    validate_materialize_output_path(output_path)
    runtime = bootstrap(project_path)
    if heartbeat_interval_seconds is not None:
        runtime.heartbeat_interval_seconds = heartbeat_interval_seconds
    dataset = load_dataset(project_path, "vectors")
    result = run_with_backend(
        visuals=visuals or "on",
        runtime=runtime,
        level=logger.getEffectiveLevel(),
        work=lambda: materialize_stream_to_path(
            runtime=runtime,
            stream_id=stream_id,
            output=output_path,
            as_stream_id=as_stream_id,
            force=force,
            dataset=dataset,
        ),
    )
    print(f"Materialized {stream_id}: {result.output} ({result.count} records)")
    print(f"Wrote metadata: {result.metadata}")
    if result.source_config is not None:
        print(f"Wrote source config: {result.source_config}")
    if result.ingest_config is not None:
        print(f"Wrote ingest config: {result.ingest_config}")
