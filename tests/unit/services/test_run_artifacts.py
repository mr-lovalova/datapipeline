from datapipeline.services.run_artifacts import serve_profile_path, write_serve_profile
from datapipeline.services.runs import get_run_paths


def test_serve_profile_path_uses_task_label(tmp_path):
    paths = get_run_paths(tmp_path, run_id="r1")
    path = serve_profile_path(paths, "val")
    assert path == (tmp_path / "runs" / "r1" / "meta" / "serve.val.profile.json")


def test_write_serve_profile_persists_json(tmp_path):
    paths = get_run_paths(tmp_path, run_id="r1")
    payload = {"label": "val", "idx": 1}
    path = write_serve_profile(paths=paths, task_label="val", payload=payload)
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert '"label": "val"' in content
    assert '"idx": 1' in content

