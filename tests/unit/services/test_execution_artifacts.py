import re

from datapipeline.services.execution_artifacts import (
    profile_artifact_path,
    write_profile_artifact,
)
from datapipeline.services.executions import ExecutionPaths, make_execution_id


def _paths(tmp_path):
    root = tmp_path / "_system" / "executions" / "e1"
    return ExecutionPaths(
        execution_id="e1",
        root=root,
        logs_dir=root / "logs",
        meta_dir=root / "meta",
        metadata_path=root / "execution.json",
    )


def test_profile_artifact_path_uses_kind_and_name(tmp_path):
    paths = _paths(tmp_path)
    path = profile_artifact_path(paths, "serve", "val")
    assert path == (tmp_path / "_system" / "executions" / "e1" / "meta" / "serve.val.profile.json")


def test_write_profile_artifact_persists_json(tmp_path):
    paths = _paths(tmp_path)
    payload = {"label": "val", "idx": 1}
    path = write_profile_artifact(
        execution=paths,
        profile_kind="serve",
        profile_name="val",
        payload=payload,
    )
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert '"label": "val"' in content
    assert '"idx": 1' in content


def test_make_execution_id_includes_subsecond_precision():
    execution_id = make_execution_id()
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{6}Z", execution_id)
