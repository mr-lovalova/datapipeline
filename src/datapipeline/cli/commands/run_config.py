from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, List, NamedTuple, Optional, Sequence

from datapipeline.config.tasks import ServeTask, serve_tasks
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap

logger = logging.getLogger(__name__)


class RunEntry(NamedTuple):
    name: Optional[str]
    config: Optional[ServeTask]
    path: Optional[Path]


def resolve_run_entries(project_path: Path, run_name: Optional[str]) -> tuple[List[RunEntry], Optional[Path]]:
    try:
        raw_entries = serve_tasks(project_path)
    except FileNotFoundError:
        raw_entries = []
    except Exception as exc:
        logger.error("Failed to load serve tasks: %s", exc)
        raise SystemExit(2) from exc

    entries: List[RunEntry] = []
    root_path: Optional[Path] = None

    if raw_entries:
        if not run_name:
            raw_entries = [task for task in raw_entries if task.enabled]
        if run_name:
            raw_entries = [
                task
                for task in raw_entries
                if task.effective_name() == run_name
            ]
            if not raw_entries:
                logger.error("Unknown run task '%s'", run_name)
                raise SystemExit(2)
        for task in raw_entries:
            path = getattr(task, "source_path", None)
            if root_path is None and path is not None:
                root_path = path.parent
            entries.append(
                RunEntry(
                    name=task.effective_name(),
                    config=task,
                    path=path,
                )
            )
    else:
        if run_name:
            logger.error("Project does not define serve tasks.")
            raise SystemExit(2)
        entries = [RunEntry(name=None, config=None, path=None)]
    return entries, root_path


def iter_runtime_runs(
    project_path: Path,
    run_entries: Sequence[RunEntry],
    keep_override: Optional[str],
) -> Iterator[tuple[int, int, RunEntry, Runtime]]:
    total_runs = len(run_entries)
    for idx, entry in enumerate(run_entries, start=1):
        run_cfg = entry.config
        runtime = bootstrap(project_path)
        if run_cfg is not None:
            runtime.run = run_cfg
            split_keep = getattr(runtime.split, "keep", None)
            runtime.split_keep = run_cfg.keep or split_keep
        if keep_override:
            runtime.split_keep = keep_override
        yield idx, total_runs, entry, runtime


def determine_preview_stage(
    cli_stage: Optional[int],
    run_entries: Sequence[RunEntry],
) -> tuple[Optional[int], Optional[str]]:
    if cli_stage is not None:
        return cli_stage, "CLI flag"

    stages: List[int] = []
    for entry in run_entries:
        run_cfg = entry.config
        cfg_stage = getattr(run_cfg, "stage", None) if run_cfg else None
        if cfg_stage is None:
            return None, None
        stages.append(cfg_stage)

    if not stages or any(stage > 5 for stage in stages):
        return None, None

    if len(set(stages)) == 1:
        return stages[0], "run config"
    return min(stages), "run configs"
