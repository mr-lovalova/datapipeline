from pathlib import Path
import shutil
from functools import lru_cache
import importlib

import pytest

from datapipeline.utils import load as dp_load

# Test-only entrypoint declarations.
_TEST_EP_TARGETS = {
    (
        "datapipeline.parsers",
        "core.temporal.csv",
    ): "tests.parsers.temporal_csv:TemporalCsvValueParser",
    (
        "datapipeline.parsers",
        "core.temporal.jsonpath",
    ): "tests.parsers.temporal_json:TemporalJsonPathParser",
}
_ORIGINAL_LOAD_EP = dp_load.load_ep


@lru_cache(maxsize=None)
def _load_ep_with_test_targets(group: str, name: str):
    target = _TEST_EP_TARGETS.get((group, name))
    if target:
        module, attr = target.split(":")
        return getattr(importlib.import_module(module), attr)
    return _ORIGINAL_LOAD_EP(group, name)


dp_load.load_ep = _load_ep_with_test_targets


@pytest.fixture
def copy_fixture(tmp_path: Path):
    """Return a helper that copies a fixture directory into a temp location."""

    def _copy(name: str) -> Path:
        src = Path(__file__).parent / "fixtures" / name
        dest = tmp_path / name
        shutil.copytree(
            src,
            dest,
            ignore=shutil.ignore_patterns(
                "build",
                "__pycache__",
                "*.pyc",
                "*.pyo",
                ".DS_Store",
            ),
        )
        return dest

    return _copy
