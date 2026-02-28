import logging
from pathlib import Path
from typing import Optional

from datapipeline.config.tasks import (
    load_task_catalog,
)
from datapipeline.services.run_entries import RunEntry

logger = logging.getLogger(__name__)


def resolve_run_entries(project_path: Path, run_name: Optional[str]) -> tuple[list[RunEntry], Optional[Path]]:
    try:
        catalog = load_task_catalog(project_path)
        raw_entries = list(catalog.serve_profiles)
        operations = list(catalog.serve_operation_tasks)
    except FileNotFoundError:
        raw_entries = []
        operations = []
    except Exception as exc:
        logger.error("Failed to load serve configuration: %s", exc)
        raise SystemExit(2) from exc

    entries: list[RunEntry] = []
    root_path: Optional[Path] = None

    if raw_entries:
        operations_by_name = {task.effective_name(): task for task in operations}
        if not operations_by_name:
            logger.error("Project does not define serve operations.")
            raise SystemExit(2)
        if not run_name:
            raw_entries = [task for task in raw_entries if task.enabled]
        if run_name:
            raw_entries = [
                task
                for task in raw_entries
                if task.effective_name() == run_name
            ]
            if not raw_entries:
                logger.error("Unknown run profile '%s'", run_name)
                raise SystemExit(2)
        for task in raw_entries:
            operation = operations_by_name.get(task.target)
            if operation is None:
                logger.error(
                    "Serve profile '%s' references unknown serve operation '%s'.",
                    task.effective_name(),
                    task.target,
                )
                raise SystemExit(2)
            path = getattr(task, "source_path", None)
            if root_path is None and path is not None:
                root_path = path.parent
            entries.append(
                RunEntry(
                    name=task.effective_name(),
                    config=task,
                    operation=operation,
                    path=path,
                )
            )
    else:
        logger.error("Project does not define serve profiles.")
        raise SystemExit(2)
    return entries, root_path
