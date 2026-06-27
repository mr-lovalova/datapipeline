from pathlib import Path

from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.services.bootstrap import bootstrap
from datapipeline.services.materialize import (
    materialize_stream_to_path,
    validate_materialize_output_path,
)
from datapipeline.services.path_policy import resolve_workspace_path


def handle(
    *,
    project: str,
    stream_id: str,
    output: str,
    as_stream_id: str | None,
    force: bool,
    visuals: str | None,
    workspace: WorkspaceContext | None,
) -> None:
    project_path = Path(project).resolve()
    output_path = resolve_workspace_path(
        output,
        workspace.root if workspace is not None else None,
    )
    validate_materialize_output_path(output_path)
    runtime = bootstrap(project_path)
    dataset = load_dataset(project_path, "vectors")
    result = materialize_stream_to_path(
        runtime=runtime,
        stream_id=stream_id,
        output=output_path,
        as_stream_id=as_stream_id,
        force=force,
        visuals=visuals,
        dataset=dataset,
    )
    print(f"Materialized {stream_id}: {result.output} ({result.count} records)")
    print(f"Wrote metadata: {result.metadata}")
    if result.source_config is not None:
        print(f"Wrote source config: {result.source_config}")
    if result.ingest_config is not None:
        print(f"Wrote ingest config: {result.ingest_config}")
