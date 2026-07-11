from pathlib import Path
from typing import Iterator, NamedTuple, Optional, Sequence

from datapipeline.config.profiles import Profile
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap_build_runtime


class RunEntry(NamedTuple):
    name: Optional[str]
    config: Optional[Profile]
    target_id: str
    path: Optional[Path] = None


def iter_runtime_runs(
    project_path: Path,
    run_entries: Sequence[RunEntry],
) -> Iterator[tuple[int, int, RunEntry, Runtime]]:
    total_runs = len(run_entries)
    for idx, entry in enumerate(run_entries, start=1):
        run_cfg = entry.config
        runtime = bootstrap_build_runtime(project_path)
        if run_cfg is not None:
            runtime.run = run_cfg
        yield idx, total_runs, entry, runtime
