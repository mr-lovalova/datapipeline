from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple

import json
import shutil


@dataclass(frozen=True)
class RunPaths:
    """Resolved filesystem paths for a single run rooted at a serve directory.

    The serve directory is typically the user-configured `directory` for the
    filesystem transport (e.g. `data/processed/...`).

    Layout:

        serve_root/
          runs/
            <run_id>/
              dataset/        # main output for this run
              run.json        # metadata for this run
          latest/             # symlink or copy pointing at the current live run
          current_run.json    # pointer to the run currently marked as "latest"
    """

    serve_root: Path
    runs_root: Path
    run_id: str
    run_root: Path
    dataset_dir: Path
    metadata_path: Path


@dataclass
class RunMetadata:
    """Metadata describing a single run."""

    run_id: str
    started_at: str
    finished_at: str | None = None
    status: str | None = None  # e.g. "running", "success", "failed"
    notes: str | None = None
    stage: int | None = None


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_run_id() -> str:
    """Create a filesystem-safe, sortable run identifier."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def get_serve_root(directory: str | Path) -> Path:
    """Resolve the user-configured serve directory to an absolute path."""
    return Path(directory).expanduser().resolve()


def get_run_paths(serve_root: Path, run_id: str | None = None) -> RunPaths:
    """Build RunPaths for a run rooted at the given serve directory."""
    if run_id is None:
        run_id = make_run_id()

    runs_root = serve_root / "runs"
    run_root = runs_root / run_id
    dataset_dir = run_root / "dataset"
    metadata_path = run_root / "run.json"

    return RunPaths(
        serve_root=serve_root,
        runs_root=runs_root,
        run_id=run_id,
        run_root=run_root,
        dataset_dir=dataset_dir,
        metadata_path=metadata_path,
    )


def _write_run_metadata(meta: RunMetadata, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(asdict(meta), f, indent=2, sort_keys=True)


def _load_run_metadata(path: Path) -> RunMetadata:
    with path.open("r", encoding="utf-8") as f:
        data: dict[str, Any] = json.load(f)
    return RunMetadata(**data)


def start_run_for_directory(
    directory: str | Path,
    run_id: str | None = None,
    *,
    stage: int | None = None,
) -> Tuple[RunPaths, RunMetadata]:
    """Initialise a new run rooted at the given directory.

    This will create the run's dataset directory and an initial metadata file
    with status set to "running".
    """
    serve_root = get_serve_root(directory)
    paths = get_run_paths(serve_root, run_id)

    # Ensure the run directories exist
    paths.dataset_dir.mkdir(parents=True, exist_ok=True)

    meta = RunMetadata(
        run_id=paths.run_id,
        started_at=_now_utc_iso(),
        finished_at=None,
        status="running",
        notes=None,
        stage=stage,
    )
    _write_run_metadata(meta, paths.metadata_path)
    return paths, meta


def finish_run(paths: RunPaths, status: str, notes: str | None = None) -> RunMetadata:
    """Mark an existing run as finished with the given status."""
    if paths.metadata_path.exists():
        meta = _load_run_metadata(paths.metadata_path)
    else:
        # Fallback: create a minimal metadata record if none exists yet
        meta = RunMetadata(
            run_id=paths.run_id,
            started_at=_now_utc_iso(),
        )

    meta.finished_at = _now_utc_iso()
    meta.status = status
    if notes is not None:
        meta.notes = notes

    _write_run_metadata(meta, paths.metadata_path)
    return meta


def finish_run_success(paths: RunPaths, notes: str | None = None) -> RunMetadata:
    """Convenience wrapper to mark a run as successful."""
    return finish_run(paths, status="success", notes=notes)


def finish_run_failed(paths: RunPaths, notes: str | None = None) -> RunMetadata:
    """Convenience wrapper to mark a run as failed."""
    return finish_run(paths, status="failed", notes=notes)


def set_latest_run(paths: RunPaths) -> None:
    """Mark the given run as the latest/live run for its serve directory.

    This updates two things under the serve root:

      * `latest/` – a symlink (or copied directory as a fallback) pointing to
        this run's root directory, so consumers can read from
        `<directory>/latest/dataset`.

      * `current_run.json` – a small pointer file recording which run is
        currently live and when this pointer was updated.
    """
    serve_root = paths.serve_root
    latest_root = serve_root / "latest"

    # Ensure serve_root exists so that the layout is predictable
    serve_root.mkdir(parents=True, exist_ok=True)

    # Remove any existing "latest" pointer
    if latest_root.is_symlink() or latest_root.is_file():
        latest_root.unlink()
    elif latest_root.is_dir():
        shutil.rmtree(latest_root)

    # Prefer a symlink for efficiency; fall back to copying if symlinks fail
    try:
        latest_root.symlink_to(paths.run_root, target_is_directory=True)
    except OSError:
        shutil.copytree(paths.run_root, latest_root)

    # Write/update current_run.json with a simple pointer
    current_meta_path = serve_root / "current_run.json"
    current_data: dict[str, Any] = {
        "run_id": paths.run_id,
        "run_root": str(paths.run_root),
        "dataset_dir": str(paths.dataset_dir),
        "updated_at": _now_utc_iso(),
    }
    with current_meta_path.open("w", encoding="utf-8") as f:
        json.dump(current_data, f, indent=2, sort_keys=True)


__all__ = [
    "RunPaths",
    "RunMetadata",
    "make_run_id",
    "get_serve_root",
    "get_run_paths",
    "start_run_for_directory",
    "finish_run",
    "finish_run_success",
    "finish_run_failed",
    "set_latest_run",
]
