from pathlib import Path
from types import SimpleNamespace

from datapipeline.operations.inspect import _resolve_output_path


def test_inspect_default_output_path_is_namespaced_under_inspect(tmp_path: Path):
    runtime = SimpleNamespace(artifacts_root=tmp_path / "artifacts")

    path = _resolve_output_path(
        runtime,
        target=SimpleNamespace(destination=None),
        options={},
        default_filename="matrix.html",
    )

    assert path == (runtime.artifacts_root / "inspect" / "matrix.html").resolve()


def test_inspect_relative_option_output_is_resolved_under_artifacts_root(tmp_path: Path):
    runtime = SimpleNamespace(artifacts_root=tmp_path / "artifacts")

    path = _resolve_output_path(
        runtime,
        target=SimpleNamespace(destination=None),
        options={"output": "inspect/custom/matrix.html"},
        default_filename="matrix.html",
    )

    assert path == (runtime.artifacts_root / "inspect" / "custom" / "matrix.html").resolve()


def test_inspect_absolute_option_output_is_used_as_is(tmp_path: Path):
    runtime = SimpleNamespace(artifacts_root=tmp_path / "artifacts")
    absolute = (tmp_path / "external" / "matrix.html").resolve()

    path = _resolve_output_path(
        runtime,
        target=SimpleNamespace(destination=None),
        options={"output": str(absolute)},
        default_filename="matrix.html",
    )

    assert path == absolute
