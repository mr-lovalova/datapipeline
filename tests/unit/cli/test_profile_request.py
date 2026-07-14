from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.config.execution import ExecutionConfig
from datapipeline.execution.settings import LogOutputTarget
from datapipeline.profiles.request_builder import (
    build_materialize_run_request,
    build_profile_run_request,
)


def _write_project(tmp_path: Path) -> Path:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "artifact_revision: 1",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  artifacts: artifacts",
                "  operations: operations",
                "  profiles: profiles",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "dataset.yaml").write_text(
        "sample:\n  cadence: 1h\n",
        encoding="utf-8",
    )
    for directory in ("ingests", "streams", "sources", "operations"):
        (tmp_path / directory).mkdir(parents=True, exist_ok=True)
    return project_yaml


def test_build_request_requires_declared_build_profiles(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    (tmp_path / "operations").mkdir(parents=True, exist_ok=True)
    (tmp_path / "profiles").mkdir(parents=True, exist_ok=True)

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(
            kind="build",
            project=str(project_yaml),
        )
    assert exc.value.code == 2


def test_inspect_request_requires_declared_inspect_profiles(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    (tmp_path / "operations").mkdir(parents=True, exist_ok=True)
    (tmp_path / "profiles").mkdir(parents=True, exist_ok=True)

    with pytest.raises(SystemExit) as exc:
        build_profile_run_request(
            kind="inspect",
            project=str(project_yaml),
        )
    assert exc.value.code == 2


def test_inspect_request_materializes_execution_scoped_log_output(
    tmp_path: Path, monkeypatch
):
    execution_dir = tmp_path / "execution"
    monkeypatch.setattr(
        "datapipeline.profiles.request_builder.execution_root",
        lambda _project: execution_dir,
    )
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)
    (ops / "coverage.yaml").write_text(
        "{}\n",
        encoding="utf-8",
    )
    (profiles / "inspect.coverage.yaml").write_text(
        ("operation: coverage\nenabled: true\nartifact_mode: AUTO\n"),
        encoding="utf-8",
    )
    (tmp_path / "sources").mkdir(parents=True, exist_ok=True)

    request = build_profile_run_request(
        kind="inspect",
        project=str(project_yaml),
        cli_log_outputs=[LogOutputTarget(transport="fs", scope="execution")],
    )
    assert request is not None
    job = request.jobs[0]
    assert job.observability.log_output.outputs[0].scope == "global"
    assert job.observability.log_output.outputs[0].destination == (
        execution_dir / "logs" / "inspect.coverage.log"
    )
    assert not execution_dir.exists()


def test_disabled_profiles_do_not_create_execution_directory(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    ops = tmp_path / "operations"
    profiles = tmp_path / "profiles"
    ops.mkdir(parents=True, exist_ok=True)
    profiles.mkdir(parents=True, exist_ok=True)
    (ops / "coverage.yaml").write_text(
        "{}\n",
        encoding="utf-8",
    )
    (profiles / "inspect.coverage.yaml").write_text(
        ("operation: coverage\nenabled: false\n"),
        encoding="utf-8",
    )

    request = build_profile_run_request(
        kind="inspect",
        project=str(project_yaml),
    )

    assert request is None
    assert not (tmp_path / "artifacts" / "_system" / "executions").exists()


def test_serve_request_uses_dataset_output_splits_by_default(tmp_path: Path):
    project_yaml = _write_project(tmp_path)
    (tmp_path / "dataset.yaml").write_text(
        """\
sample: {cadence: 1h}
split:
  mode: hash
  ratios: {train: 0.8, test: 0.2}
  output_labels: [train, test]
""",
        encoding="utf-8",
    )
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "serve.defaults.yaml").write_text(
        "output: {transport: fs, format: jsonl, directory: output}\n",
        encoding="utf-8",
    )
    (profiles / "serve.dataset.yaml").write_text(
        "operation: dataset\n",
        encoding="utf-8",
    )

    request = build_profile_run_request(kind="serve", project=str(project_yaml))

    assert request is not None
    assert request.jobs[0].output_splits == ("train", "test")


def test_materialize_request_uses_shared_resolution_snapshot(
    monkeypatch,
    tmp_path: Path,
) -> None:
    project_yaml = _write_project(tmp_path)
    (tmp_path / "operations").mkdir(parents=True, exist_ok=True)
    profiles = tmp_path / "profiles"
    profiles.mkdir(parents=True, exist_ok=True)
    (profiles / "materialize.defaults.yaml").write_text(
        "artifact_mode: FORCE\nexecution:\n  sort_buffer_mb: 32\n",
        encoding="utf-8",
    )
    (profiles / "materialize.adv-20.yaml").write_text(
        "stream: adv.20\noutput: outputs/adv-20.jsonl\n",
        encoding="utf-8",
    )
    runtime = SimpleNamespace(execution=ExecutionConfig())
    compiled_definitions = []

    def compile_runtime(definition):
        compiled_definitions.append(definition)
        return runtime

    monkeypatch.setattr(
        "datapipeline.profiles.request_builder.compile_runtime",
        compile_runtime,
    )
    execution_dir = tmp_path / "execution"
    execution_root_calls: list[Path] = []

    def shared_execution_root(path: Path) -> Path:
        execution_root_calls.append(path)
        return execution_dir

    monkeypatch.setattr(
        "datapipeline.profiles.request_builder.execution_root",
        shared_execution_root,
    )
    request = build_materialize_run_request(
        project=str(project_yaml),
        run_name=None,
        overwrite=None,
        output=None,
        artifact_mode=None,
        cli_log_level=None,
        cli_log_outputs=[LogOutputTarget(transport="fs", scope="execution")],
        base_log_level="INFO",
        cli_visuals=None,
        cli_heartbeat_interval_seconds=None,
    )

    assert request is not None
    assert request.definition.definition_hash
    assert request.definition.artifact_hashes.values
    assert compiled_definitions == [request.definition]
    assert request.execution.sort_buffer_mb == 32
    assert request.artifact_settings.mode == "FORCE"
    assert request.runtime is runtime
    assert request.jobs[0].name == "adv-20"
    assert request.jobs[0].stream == "adv.20"
    assert request.jobs[0].output == tmp_path / "outputs" / "adv-20.jsonl"
    assert request.jobs[0].observability.log_output.outputs[0].destination == (
        execution_dir / "logs" / "materialize.adv-20.log"
    )
    assert (
        request.artifact_settings.observability.log_output.outputs[0].destination
        == execution_dir / "logs" / "materialize.artifacts.log"
    )
    assert execution_root_calls == [tmp_path / "artifacts"]
