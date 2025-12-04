from __future__ import annotations

from pathlib import Path
import shutil

import pytest

from datapipeline.utils import load as dp_load

# Test-only entrypoint overrides for fixture parsers.
dp_load._EP_OVERRIDES.update({
    ("datapipeline.parsers", "core.temporal.csv"): "tests.parsers.temporal_csv:TemporalCsvValueParser",
    ("datapipeline.parsers", "core.temporal.jsonpath"): "tests.parsers.temporal_json:TemporalJsonPathParser",
})


@pytest.fixture
def copy_fixture(tmp_path: Path):
    """Return a helper that copies a fixture directory into a temp location."""

    def _copy(name: str) -> Path:
        src = Path(__file__).parent / "fixtures" / name
        dest = tmp_path / name
        shutil.copytree(src, dest)
        return dest

    return _copy
