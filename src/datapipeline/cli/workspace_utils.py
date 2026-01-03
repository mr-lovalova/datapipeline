from __future__ import annotations

from pathlib import Path

from datapipeline.config.workspace import WorkspaceContext


def resolve_default_project_yaml(workspace: WorkspaceContext | None) -> Path | None:
    """Resolve default_dataset from jerry.yaml into a project.yaml path.

    Returns None when no workspace context or no default_dataset is configured.
    Raises SystemExit when default_dataset is set but missing from datasets:.
    """
    if workspace is None:
        return None
    alias = workspace.config.default_dataset
    if not alias:
        return None
    resolved = workspace.resolve_dataset_alias(alias)
    if resolved is None:
        raise SystemExit(
            f"Unknown default_dataset '{alias}'. Define it under datasets: in jerry.yaml."
        )
    return resolved

