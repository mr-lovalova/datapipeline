from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Tuple

from datapipeline.config.run import RunConfig, load_named_run_configs
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap

logger = logging.getLogger(__name__)


RunEntry = Tuple[Optional[str], Optional[RunConfig]]


def resolve_run_entries(project_path: Path, run_name: Optional[str]) -> List[RunEntry]:
    try:
        entries = load_named_run_configs(project_path)
    except FileNotFoundError:
        entries = []
    except Exception as exc:
        logger.error("Failed to load run configs: %s", exc)
        raise SystemExit(2) from exc

    if entries:
        if run_name:
            entries = [entry for entry in entries if entry[0] == run_name]
            if not entries:
                logger.error("Unknown run config '%s'", run_name)
                raise SystemExit(2)
    else:
        if run_name:
            logger.error("Project does not define run configs.")
            raise SystemExit(2)
        entries = [(None, None)]
    return entries


def iter_runtime_runs(
    project_path: Path,
    run_entries: Sequence[RunEntry],
    keep_override: Optional[str],
) -> Iterator[tuple[int, int, Optional[str], Runtime]]:
    total_runs = len(run_entries)
    for idx, (entry_name, run_cfg) in enumerate(run_entries, start=1):
        runtime = bootstrap(project_path)
        if run_cfg is not None:
            runtime.run = run_cfg
            split_keep = getattr(runtime.split, "keep", None)
            runtime.split_keep = run_cfg.keep or split_keep
        if keep_override:
            runtime.split_keep = keep_override
        yield idx, total_runs, entry_name, runtime


def determine_preview_stage(
    cli_stage: Optional[int],
    run_entries: Sequence[RunEntry],
) -> tuple[Optional[int], Optional[str]]:
    if cli_stage is not None:
        return cli_stage, "CLI flag"

    stages: List[int] = []
    for _, run_cfg in run_entries:
        cfg_stage = getattr(run_cfg, "stage", None) if run_cfg else None
        if cfg_stage is None:
            return None, None
        stages.append(cfg_stage)

    if not stages or any(stage > 5 for stage in stages):
        return None, None

    if len(set(stages)) == 1:
        return stages[0], "run config"
    return min(stages), "run configs"
