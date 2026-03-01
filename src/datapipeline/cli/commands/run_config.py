import logging
import os
from pathlib import Path
from typing import Callable, Literal, Optional

from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.loaders.profiles import profile_specs
from datapipeline.services.run_entries import RunEntry

logger = logging.getLogger(__name__)
RuntimeKind = Literal["serve", "inspect"]


_RUNTIME_ENTRYPOINT_MATCHERS: dict[RuntimeKind, Callable[[str], bool]] = {
    "serve": lambda entrypoint: entrypoint.startswith("core.serve.")
    or entrypoint.startswith("core.serve_"),
    "inspect": lambda entrypoint: entrypoint.startswith("core.inspect."),
}


def _matches_runtime_kind(kind: RuntimeKind, entrypoint: str) -> bool:
    normalized = str(entrypoint or "").strip().lower()
    return _RUNTIME_ENTRYPOINT_MATCHERS[kind](normalized)


def resolve_runtime_entries(
    project_path: Path,
    run_name: Optional[str],
    *,
    kind: RuntimeKind,
) -> tuple[list[RunEntry], Optional[Path]]:
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
    entry_paths: list[Path] = []

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
            if not _matches_runtime_kind(kind, operation.entrypoint):
                logger.error(
                    "%s profile '%s' targets operation '%s' with entrypoint '%s', "
                    "which is not a %s operation.",
                    kind.capitalize(),
                    task.name,
                    task.target,
                    operation.entrypoint,
                    kind,
                )
                raise SystemExit(2)
            path = getattr(task, "source_path", None)
            if path is not None:
                entry_paths.append(path.parent)
            entries.append(
                RunEntry(
                    name=task.name,
                    config=task,
                    operation=operation,
                    path=path,
                )
            )
    else:
        logger.error("Project does not define %s profiles.", kind)
        raise SystemExit(2)
    if not entry_paths:
        return entries, None
    try:
        root_path = Path(os.path.commonpath([str(path) for path in entry_paths]))
    except ValueError:
        root_path = entry_paths[0]
    return entries, root_path
