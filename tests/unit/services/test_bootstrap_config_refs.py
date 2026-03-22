from pathlib import Path

import pytest

from datapipeline.build.config_hash import compute_config_hash
from datapipeline.services.bootstrap.config import _globals, _load_by_key, artifacts_root
from datapipeline.services.bootstrap.core import _load_sources_from_dir
from datapipeline.services.project_paths import sources_dir


def _write_project_yaml(
    project_root: Path,
    *,
    artifacts_value: str = "build",
    globals_lines: list[str] | None = None,
) -> Path:
    project_yaml = project_root / "project.yaml"
    lines = [
        "version: 1",
        "name: sample",
        "paths:",
        "  streams: contracts",
        "  sources: sources",
        "  dataset: dataset.yaml",
        "  postprocess: postprocess.yaml",
        f"  artifacts: {artifacts_value}",
        "  tasks: tasks",
    ]
    if globals_lines:
        lines.append("globals:")
        lines.extend(f"  {line}" for line in globals_lines)
    else:
        lines.append("globals: {}")

    project_yaml.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return project_yaml


def _write_project_files(project_root: Path) -> None:
    (project_root / "contracts").mkdir(parents=True, exist_ok=True)
    (project_root / "sources").mkdir(parents=True, exist_ok=True)
    (project_root / "tasks").mkdir(parents=True, exist_ok=True)
    (project_root / "dataset.yaml").write_text("value: default\n", encoding="utf-8")
    (project_root / "postprocess.yaml").write_text("[]\n", encoding="utf-8")


def test_globals_resolve_env_refs_from_project_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RAW_ROOT", raising=False)
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    (project_root / ".env").write_text("RAW_ROOT=../shared-data\n", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        artifacts_value="${raw_root}/artifacts",
        globals_lines=["raw_root: ${env:RAW_ROOT}"],
    )

    assert _globals(project_yaml)["raw_root"] == "../shared-data"
    assert artifacts_root(project_yaml) == (project_root / "../shared-data" / "artifacts").resolve()


def test_process_env_overrides_project_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    (project_root / ".env").write_text("RAW_ROOT=../shared-data\n", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_root: ${env:RAW_ROOT}"],
    )
    monkeypatch.setenv("RAW_ROOT", "/runtime/raw")

    assert _globals(project_yaml)["raw_root"] == "/runtime/raw"


def test_load_by_key_resolves_direct_env_refs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(project_root)
    (project_root / "dataset.yaml").write_text("value: ${env:DATASET_VALUE}\n", encoding="utf-8")
    monkeypatch.setenv("DATASET_VALUE", "records")

    loaded = _load_by_key(project_yaml, "dataset")

    assert loaded["value"] == "records"


def test_load_sources_resolve_env_backed_globals_before_fs_path_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RAW_GLOB", raising=False)
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    (project_root / ".env").write_text("RAW_GLOB=data/*.jsonl\n", encoding="utf-8")
    (project_root / "data").mkdir(parents=True, exist_ok=True)
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_glob: ${env:RAW_GLOB}"],
    )
    (project_root / "sources" / "sample.yaml").write_text(
        "\n".join(
            [
                "id: sample.fs",
                "parser:",
                "  entrypoint: identity",
                "  args: {}",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                "    path: ${raw_glob}",
                "    glob: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    loaded = _load_sources_from_dir(project_yaml, vars_=_globals(project_yaml))

    assert loaded["sample.fs"]["loader"]["args"]["path"] == str(
        (project_root / "data" / "*.jsonl").resolve()
    )


def test_missing_env_ref_raises_clear_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_ROOT", raising=False)
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True)
    _write_project_files(project_root)
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_root: ${env:MISSING_ROOT}"],
    )

    with pytest.raises(ValueError, match="MISSING_ROOT"):
        _globals(project_yaml)


def test_sources_dir_resolves_env_backed_project_path_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RAW_ROOT", raising=False)
    project_root = tmp_path / "project"
    raw_root = tmp_path / "shared"
    (raw_root / "sources").mkdir(parents=True)
    _write_project_files(project_root)
    (project_root / ".env").write_text(f"RAW_ROOT={raw_root}\n", encoding="utf-8")
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_root: ${env:RAW_ROOT}"],
    )
    project_yaml.write_text(
        project_yaml.read_text(encoding="utf-8").replace(
            "  sources: sources",
            "  sources: ${raw_root}/sources",
        ),
        encoding="utf-8",
    )

    assert sources_dir(project_yaml) == (raw_root / "sources").resolve()


def test_compute_config_hash_changes_when_env_value_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RAW_ROOT", raising=False)
    project_root = tmp_path / "project"
    _write_project_files(project_root)
    (project_root / "sources" / "sample.yaml").write_text(
        "\n".join(
            [
                "id: sample.fs",
                "parser:",
                "  entrypoint: identity",
                "  args: {}",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                "    path: ${raw_root}/rows.jsonl",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    project_yaml = _write_project_yaml(
        project_root,
        globals_lines=["raw_root: ${env:RAW_ROOT}"],
    )

    monkeypatch.setenv("RAW_ROOT", "/tmp/raw-one")
    hash_one = compute_config_hash(project_yaml, project_root / "tasks")

    monkeypatch.setenv("RAW_ROOT", "/tmp/raw-two")
    hash_two = compute_config_hash(project_yaml, project_root / "tasks")

    assert hash_one != hash_two
