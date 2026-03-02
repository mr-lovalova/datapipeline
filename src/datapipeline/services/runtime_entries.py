import logging
from pathlib import Path
from typing import Literal, Optional

from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.loaders.profiles import profile_specs
from datapipeline.services.run_entries import RunEntry

logger = logging.getLogger(__name__)
RuntimeKind = Literal["serve", "inspect"]

def _matches_runtime_kind(kind: RuntimeKind, runtime_kind: str) -> bool:
    return str(runtime_kind).strip().lower() == kind


def resolve_runtime_entries(
    project_path: Path,
    run_name: Optional[str],
    *,
    kind: RuntimeKind,
) -> list[RunEntry]:
    try:
        serve_specs, _, inspect_specs = profile_specs(project_path)
        raw_entries = serve_specs if kind == "serve" else inspect_specs
        _, operations = operation_specs(project_path)
    except FileNotFoundError:
        raw_entries = []
        operations = []
    except Exception as exc:
        logger.error("Failed to load %s configuration: %s", kind, exc)
        raise SystemExit(2) from exc

    entries: list[RunEntry] = []

    if raw_entries:
        operations_by_id = {task.id: task for task in operations}
        if not run_name:
            raw_entries = [task for task in raw_entries if task.enabled]
        if run_name:
            raw_entries = [
                task
                for task in raw_entries
                if task.name == run_name
            ]
            if not raw_entries:
                logger.error("Unknown %s profile '%s'", kind, run_name)
                raise SystemExit(2)
        for task in raw_entries:
            operation = operations_by_id.get(task.target)
            if operation is None:
                logger.error(
                    "%s profile '%s' references unknown %s operation '%s'.",
                    kind.capitalize(),
                    task.name,
                    kind,
                    task.target,
                )
                raise SystemExit(2)
            if not _matches_runtime_kind(kind, operation.runtime_kind):
                logger.error(
                    "%s profile '%s' targets operation '%s' with runtime_kind '%s', "
                    "which is not a %s operation kind.",
                    kind.capitalize(),
                    task.name,
                    task.target,
                    operation.runtime_kind,
                    kind,
                )
                raise SystemExit(2)
            entries.append(
                RunEntry(
                    name=task.name,
                    config=task,
                    operation=operation,
                    path=getattr(task, "source_path", None),
                )
            )
    else:
        logger.error("Project does not define %s profiles.", kind)
        raise SystemExit(2)
    return entries
