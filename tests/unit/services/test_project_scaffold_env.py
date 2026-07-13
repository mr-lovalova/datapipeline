from pathlib import Path

from datapipeline.services.project_paths import ensure_project_scaffold


def test_ensure_project_scaffold_creates_dotenv_example(tmp_path: Path) -> None:
    project_yaml = tmp_path / "example" / "project.yaml"

    ensure_project_scaffold(project_yaml)

    dotenv_example = project_yaml.parent / ".env.example"
    assert dotenv_example.exists()
    text = dotenv_example.read_text(encoding="utf-8")
    assert "RAW_ROOT=" in text
