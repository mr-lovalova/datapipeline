from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from datapipeline.services.bootstrap import artifacts_root


@dataclass(frozen=True)
class ExecutionPaths:
    execution_id: str
    root: Path
    logs_dir: Path
    meta_dir: Path
    metadata_path: Path


def make_execution_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%fZ")


def execution_root(project_yaml: Path, execution_id: str | None = None) -> Path:
    root_id = execution_id or make_execution_id()
    return (artifacts_root(project_yaml) / "_system" / "executions" / root_id).resolve()


def get_execution_paths(
    project_yaml: Path,
    execution_id: str | None = None,
) -> ExecutionPaths:
    root = execution_root(project_yaml, execution_id=execution_id)
    return ExecutionPaths(
        execution_id=root.name,
        root=root,
        logs_dir=root / "logs",
        meta_dir=root / "meta",
        metadata_path=root / "execution.json",
    )


def start_execution(
    paths: ExecutionPaths,
    *,
    project_yaml: Path,
    command: str,
) -> ExecutionPaths:
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    paths.meta_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "execution_id": paths.execution_id,
        "command": command,
        "project": str(project_yaml.resolve()),
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    with paths.metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)
    return paths


__all__ = [
    "ExecutionPaths",
    "execution_root",
    "get_execution_paths",
    "make_execution_id",
    "start_execution",
]
