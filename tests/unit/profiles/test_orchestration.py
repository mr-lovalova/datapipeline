from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from datapipeline.build.state import BuildState, save_build_state
from datapipeline.config.tasks import MetadataTask, OperationTask, SchemaTask
from datapipeline.io.output import OutputTarget
from datapipeline.profiles.models import ExecutionProfile, ProfileRunRequest
from datapipeline.profiles.orchestration import run_profiles
from datapipeline.runtime import Runtime
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.bootstrap import build_state_path
from datapipeline.services.constants import VECTOR_SCHEMA
from datapipeline.services.runs import RunPaths


def _log_config():
    return (
        SimpleNamespace(name="INFO", value=20),
        SimpleNamespace(outputs=()),
    )


def _run_paths(tmp_path):
    run_root = tmp_path / "runs" / "r1"
    return RunPaths(
        serve_root=tmp_path,
        runs_root=tmp_path / "runs",
        run_id="r1",
        run_root=run_root,
        dataset_dir=run_root / "dataset",
        metadata_path=run_root / "run.json",
    )


def test_run_profiles_executes_profile_target(monkeypatch, tmp_path):
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata")
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "core.runtime.pipeline",
        }
    )

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        tasks=[schema, metadata, serve],
        artifact_task_configs=[schema, metadata],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
            )
        ],
    )

    calls = {"build": 0, "dispatch": 0}

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        lambda *args, **kwargs: calls.__setitem__("build", calls["build"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: calls.__setitem__("dispatch", calls["dispatch"] + 1),
    )

    run_profiles(request)

    assert calls["build"] == 0
    assert calls["dispatch"] == 1


def test_run_profiles_parent_scope_does_not_announce(monkeypatch, tmp_path):
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "core.runtime.pipeline",
        }
    )

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        tasks=[serve],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
            )
        ],
    )

    captured: dict[str, object] = {}

    @contextmanager
    def _capture_scope(**kwargs):
        captured.update(kwargs)
        yield

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execution_scope",
        _capture_scope,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: None,
    )

    run_profiles(request)

    assert captured.get("announce") is False


def test_run_profiles_can_skip_artifact_build(monkeypatch, tmp_path):
    schema = SchemaTask(id="schema")
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "core.runtime.pipeline",
        }
    )

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        tasks=[schema, serve],
        artifact_task_configs=[schema],
        skip_build=True,
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
            )
        ],
    )

    calls = {"build": 0, "dispatch": 0}
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        lambda *args, **kwargs: calls.__setitem__("build", calls["build"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: calls.__setitem__("dispatch", calls["dispatch"] + 1),
    )

    run_profiles(request)

    assert calls["build"] == 0
    assert calls["dispatch"] == 1


def test_run_profiles_syncs_runtime_artifacts_after_build(monkeypatch, tmp_path):
    (tmp_path / "project.yaml").write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  ingests: ./ingests",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: ./dataset.yaml",
                "  postprocess: ./postprocess.yaml",
                "  artifacts: ./artifacts",
                "  tasks: ./tasks",
                "  profiles: ./profiles",
            ]
        ),
        encoding="utf-8",
    )

    schema = SchemaTask(id="schema")
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "core.runtime.pipeline",
        }
    )

    log_decision, log_output = _log_config()
    runtime = SimpleNamespace(artifacts=ArtifactManager(tmp_path / "artifacts"))
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        tasks=[schema, serve],
        artifact_task_configs=[schema],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=runtime,
                dataset=object(),
            )
        ],
    )

    state = BuildState(config_hash="hash-1")
    state.register(
        VECTOR_SCHEMA,
        "build/metadata.json",
        meta={"_config_hash": "hash-1"},
    )
    save_build_state(state, build_state_path(request.project_path))

    seen = {"synced": False}

    def _capture_dispatch(**kwargs):
        seen["synced"] = kwargs["runtime"].artifacts.has(VECTOR_SCHEMA)

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        _capture_dispatch,
    )

    run_profiles(request)

    assert seen["synced"] is True


def test_run_profiles_forward_runtime_build_settings(monkeypatch, tmp_path):
    schema = SchemaTask(id="schema")

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        tasks=[schema],
        artifact_task_configs=[schema],
        profiles=[
            ExecutionProfile(
                name="serve",
                target_id="schema",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
                build_mode="FORCE",
            )
        ],
    )

    seen: dict[str, object] = {}
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        lambda *args, **kwargs: seen.update(kwargs),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: None,
    )

    run_profiles(request)

    settings = seen["settings"]
    assert settings.mode == "FORCE"
    assert settings.force is True
    assert settings.profile_name == "serve"


def test_runtime_dependency_build_scope_isolated_from_parent_profile(monkeypatch, tmp_path):
    schema = SchemaTask(id="schema")
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "core.runtime.pipeline",
        }
    )

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        command="inspect",
        project_path=tmp_path / "project.yaml",
        tasks=[schema, serve],
        artifact_task_configs=[schema],
        profiles=[
            ExecutionProfile(
                name="coverage",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=SimpleNamespace(artifacts=ArtifactManager(tmp_path / "artifacts")),
                dataset=object(),
            )
        ],
    )

    captured = {"count": 0}

    def _capture_selected_artifacts(**_kwargs):
        captured["count"] += 1

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_selected_artifacts",
        _capture_selected_artifacts,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.sync_runtime_artifacts_from_state",
        lambda *_args, **_kwargs: None,
    )

    run_profiles(request)

    assert captured["count"] == 0


def test_run_profiles_share_ephemeral_cache_root_across_runtime_profiles(
    monkeypatch,
    tmp_path,
):
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "core.runtime.pipeline",
        }
    )
    project_yaml = tmp_path / "project.yaml"
    runtime_one = Runtime(project_yaml=project_yaml, artifacts_root=tmp_path / "artifacts")
    runtime_two = Runtime(project_yaml=project_yaml, artifacts_root=tmp_path / "artifacts")
    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        command="serve",
        project_path=project_yaml,
        tasks=[serve],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="train",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=runtime_one,
                dataset=object(),
            ),
            ExecutionProfile(
                name="val",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=runtime_two,
                dataset=object(),
            ),
        ],
    )

    seen_roots = []

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.execute_operation",
        lambda **kwargs: seen_roots.append(kwargs["runtime"].cache_root),
    )

    run_profiles(request)

    assert len(seen_roots) == 2
    assert seen_roots[0] == seen_roots[1]
    assert not seen_roots[0].exists()


def test_run_profiles_finalize_shared_serve_run_once(monkeypatch, tmp_path):
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "core.runtime.pipeline",
        }
    )
    run_paths = _run_paths(tmp_path)
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=run_paths.dataset_dir / "train.jsonl",
        run=run_paths,
    )
    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        tasks=[serve],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="train",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
                output=target,
            ),
            ExecutionProfile(
                name="val",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
                output=target,
            ),
        ],
    )

    calls = {"success": 0, "failed": 0, "latest": 0}
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_success",
        lambda _run: calls.__setitem__("success", calls["success"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_failed",
        lambda _run: calls.__setitem__("failed", calls["failed"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.set_latest_run",
        lambda _run: calls.__setitem__("latest", calls["latest"] + 1),
    )

    run_profiles(request)

    assert calls == {"success": 1, "failed": 0, "latest": 1}


def test_run_profiles_fail_shared_serve_run_once_when_later_profile_errors(monkeypatch, tmp_path):
    serve = OperationTask.model_validate(
        {
            "id": "pipeline",
            "kind": "runtime",
            "entrypoint": "core.runtime.pipeline",
        }
    )
    run_paths = _run_paths(tmp_path)
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=run_paths.dataset_dir / "train.jsonl",
        run=run_paths,
    )
    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        command="serve",
        project_path=tmp_path / "project.yaml",
        tasks=[serve],
        artifact_task_configs=[],
        profiles=[
            ExecutionProfile(
                name="train",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
                output=target,
            ),
            ExecutionProfile(
                name="val",
                target_id="pipeline",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
                output=target,
            ),
        ],
    )

    state = {"calls": 0}
    outcomes = {"success": 0, "failed": 0, "latest": 0}

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )

    def _execute_profile(**kwargs):
        state["calls"] += 1
        if state["calls"] == 2:
            raise RuntimeError("boom")

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.execute_profile",
        _execute_profile,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_success",
        lambda _run: outcomes.__setitem__("success", outcomes["success"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.finish_run_failed",
        lambda _run: outcomes.__setitem__("failed", outcomes["failed"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.set_latest_run",
        lambda _run: outcomes.__setitem__("latest", outcomes["latest"] + 1),
    )

    with pytest.raises(RuntimeError, match="boom"):
        run_profiles(request)

    assert outcomes == {"success": 0, "failed": 1, "latest": 0}
