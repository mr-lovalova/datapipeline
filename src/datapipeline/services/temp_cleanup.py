import shutil
import tempfile
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from time import time


TEMP_DIR_PREFIXES = ("datapipeline-sort-",)


@dataclass(frozen=True)
class TempDirCandidate:
    path: Path
    size_bytes: int
    age_seconds: float


@dataclass(frozen=True)
class CleanResult:
    candidates: tuple[TempDirCandidate, ...]
    removed: tuple[Path, ...]
    dry_run: bool

    @property
    def total_bytes(self) -> int:
        return sum(item.size_bytes for item in self.candidates)


def parse_age(value: str | None) -> timedelta:
    if value is None:
        return timedelta(0)
    text = str(value).strip().lower()
    if not text:
        return timedelta(0)
    unit = text[-1]
    number_text = text[:-1] if unit in {"m", "h", "d"} else text
    try:
        amount = float(number_text)
    except ValueError as exc:
        raise ValueError("age must be a number with optional m, h, or d suffix") from exc
    if amount < 0:
        raise ValueError("age must not be negative")
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "d":
        return timedelta(days=amount)
    return timedelta(hours=amount)


def find_temp_dirs(
    *,
    root: Path | None = None,
    older_than: timedelta = timedelta(0),
) -> tuple[TempDirCandidate, ...]:
    temp_root = _temp_root(root)
    if not temp_root.exists():
        return ()
    cutoff_seconds = max(0.0, older_than.total_seconds())
    now = time()
    candidates: list[TempDirCandidate] = []
    for path in temp_root.iterdir():
        if not _is_jerry_temp_dir(path):
            continue
        age_seconds = max(0.0, now - path.stat().st_mtime)
        if age_seconds < cutoff_seconds:
            continue
        candidates.append(
            TempDirCandidate(
                path=path,
                size_bytes=_directory_size(path),
                age_seconds=age_seconds,
            )
        )
    return tuple(sorted(candidates, key=lambda item: str(item.path)))


def clean_temp_dirs(
    *,
    yes: bool,
    root: Path | None = None,
    older_than: timedelta = timedelta(0),
) -> CleanResult:
    candidates = find_temp_dirs(root=root, older_than=older_than)
    if not yes:
        return CleanResult(candidates=candidates, removed=(), dry_run=True)
    removed: list[Path] = []
    for item in candidates:
        shutil.rmtree(item.path)
        removed.append(item.path)
    return CleanResult(candidates=candidates, removed=tuple(removed), dry_run=False)


def format_bytes(value: int) -> str:
    amount = float(max(0, value))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if amount < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(amount)}B"
            return f"{amount:.1f}{unit}"
        amount /= 1024.0
    return f"{amount:.1f}TB"


def format_age(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def _temp_root(root: Path | None) -> Path:
    return (root or Path(tempfile.gettempdir())).resolve()


def _is_jerry_temp_dir(path: Path) -> bool:
    return (
        path.is_dir()
        and not path.is_symlink()
        and any(path.name.startswith(prefix) for prefix in TEMP_DIR_PREFIXES)
    )


def _directory_size(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file() and not child.is_symlink():
            total += child.stat().st_size
    return total


__all__ = [
    "CleanResult",
    "TempDirCandidate",
    "clean_temp_dirs",
    "find_temp_dirs",
    "format_age",
    "format_bytes",
    "parse_age",
]
