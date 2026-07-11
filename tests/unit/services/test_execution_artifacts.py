from datapipeline.services.execution_artifacts import write_profile_artifact


def _execution_dir(tmp_path):
    return tmp_path / "_system" / "executions" / "e1"


def test_write_profile_artifact_persists_json(tmp_path):
    payload = {"label": "val", "idx": 1}
    path = write_profile_artifact(
        execution_dir=_execution_dir(tmp_path),
        profile_kind="serve",
        profile_name="val",
        payload=payload,
    )
    assert path.exists()
    assert path.name == "serve.val.profile.json"
    content = path.read_text(encoding="utf-8")
    assert '"label": "val"' in content
    assert '"idx": 1' in content
    assert {child.name for child in _execution_dir(tmp_path).iterdir()} == {"meta"}
