from pathlib import Path

from datapipeline.services.artifacts import ArtifactRecord
from datapipeline.services.path_policy import (
    relative_to_workspace,
    resolve_project_path,
    resolve_relative_to_base,
    resolve_workspace_path,
    workspace_roots_for,
)


def test_resolve_workspace_path_prefers_workspace_root(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    nested = workspace_root / "nested" / "cwd"
    nested.mkdir(parents=True)

    resolved = resolve_workspace_path("data/out.jsonl", workspace_root, cwd=nested)
    assert resolved == (workspace_root / "data" / "out.jsonl").resolve()


def test_resolve_project_path_uses_project_directory(tmp_path: Path) -> None:
    project_dir = tmp_path / "plugin" / "dataset"
    project_dir.mkdir(parents=True)
    project_yaml = project_dir / "project.yaml"
    project_yaml.write_text("version: 1\nname: x\npaths: {}\n", encoding="utf-8")

    assert resolve_project_path(project_yaml, "sources").resolve() == (project_dir / "sources").resolve()
    absolute = tmp_path / "absolute" / "target"
    assert resolve_project_path(project_yaml, absolute) == absolute.resolve()


def test_resolve_relative_to_base_can_skip_final_resolve(tmp_path: Path) -> None:
    base = tmp_path / "artifacts"
    relative = resolve_relative_to_base("runs/latest/schema.json", base, resolve=False)
    assert relative == base / "runs" / "latest" / "schema.json"


def test_workspace_roots_for_returns_all_ancestors_with_jerry_yaml(tmp_path: Path) -> None:
    outer = tmp_path / "outer"
    inner = outer / "inner"
    target = inner / "project"
    target.mkdir(parents=True)
    (outer / "jerry.yaml").write_text("datasets: {}\n", encoding="utf-8")
    (inner / "jerry.yaml").write_text("datasets: {}\n", encoding="utf-8")

    roots = workspace_roots_for(target)
    assert roots == [inner.resolve(), outer.resolve()]


def test_relative_to_workspace_handles_outside_workspace(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    other = tmp_path / "other" / "plugin"
    workspace.mkdir()
    other.mkdir(parents=True)

    rel = relative_to_workspace(other, workspace)
    assert rel.as_posix().startswith("..")


def test_artifact_record_resolve_uses_relative_policy(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    rec = ArtifactRecord(key="schema", relative_path="schema.json", meta={})
    assert rec.resolve(root) == root / "schema.json"
