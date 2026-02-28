from __future__ import annotations

from pathlib import Path
from typing import Iterator, NamedTuple, Optional, Sequence

from datapipeline.config.tasks import ServeOperationTask, ServeTask
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap


class RunEntry(NamedTuple):
    name: Optional[str]
    config: Optional[ServeTask]
    operation: ServeOperationTask
    path: Optional[Path] = None


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
