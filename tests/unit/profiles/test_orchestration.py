from types import SimpleNamespace

from datapipeline.build.state import BuildState, save_build_state
from datapipeline.config.tasks import MetadataTask, OperationTask, SchemaTask
from datapipeline.profiles.models import ExecutionProfile, ProfileRunRequest
from datapipeline.profiles.orchestration import run_profiles
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.bootstrap import build_state_path
from datapipeline.services.constants import VECTOR_SCHEMA


def _log_config():
    return (
        SimpleNamespace(name="INFO", value=20),
        SimpleNamespace(outputs=()),
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


def test_run_profiles_forward_runtime_build_mode(monkeypatch, tmp_path):
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

    assert seen["runtime_build_mode"] == "FORCE"
    assert seen["profile_name_override"] == "serve"


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
