from datapipeline.services.run_artifacts import profile_artifact_path, write_profile_artifact
from datapipeline.services.runs import get_run_paths


def test_profile_artifact_path_uses_kind_and_name(tmp_path):
    paths = get_run_paths(tmp_path, run_id="r1")
    path = profile_artifact_path(paths, "serve", "val")
    assert path == (tmp_path / "runs" / "r1" / "meta" / "serve.val.profile.json")


def test_write_profile_artifact_persists_json(tmp_path):
    paths = get_run_paths(tmp_path, run_id="r1")
    payload = {"label": "val", "idx": 1}
    path = write_profile_artifact(
        paths=paths,
        profile_kind="serve",
        profile_name="val",
        payload=payload,
    )
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert '"label": "val"' in content
    assert '"idx": 1' in content
