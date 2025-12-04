from __future__ import annotations

from pathlib import Path
import shutil

import pytest


@pytest.fixture
def copy_fixture(tmp_path: Path):
    """Return a helper that copies a fixture directory into a temp location."""

    def _copy(name: str) -> Path:
        src = Path(__file__).parent / "fixtures" / name
        dest = tmp_path / name
        shutil.copytree(src, dest)
        return dest

    return _copy
