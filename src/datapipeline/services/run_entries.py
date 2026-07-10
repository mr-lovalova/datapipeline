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
    keep_override: Optional[str],
) -> Iterator[tuple[int, int, RunEntry, Runtime]]:
    total_runs = len(run_entries)
    for idx, entry in enumerate(run_entries, start=1):
        run_cfg = entry.config
        runtime = bootstrap_build_runtime(project_path)
        if run_cfg is not None:
            runtime.run = run_cfg
            if getattr(run_cfg, "cmd", None) == "serve":
                split_keep = getattr(runtime.split, "keep", None)
                run_keep = getattr(run_cfg, "keep", None)
                runtime.split_keep = run_keep or split_keep
                if keep_override:
                    runtime.split_keep = keep_override
            else:
                runtime.split_keep = None
        yield idx, total_runs, entry, runtime
