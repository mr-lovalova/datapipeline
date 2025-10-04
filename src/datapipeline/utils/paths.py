from __future__ import annotations

from pathlib import Path

DEFAULT_BUILD_DIR = Path("build")


def default_build_path(filename: str, base: Path | str | None = None) -> Path:
    """Return the path to *filename* inside a build directory.

    When *base* is provided (typically the directory that houses a recipe or
    project configuration), the build folder is resolved relative to that
    directory. Otherwise we fall back to ``build/`` under the current working
    directory.
    """

    if base is not None:
        base_path = Path(base)
        return base_path / "build" / filename
    return DEFAULT_BUILD_DIR / filename


def ensure_parent(path: Path) -> None:
    """Create parent directories for *path* if they do not exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
