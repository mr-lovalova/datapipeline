from types import SimpleNamespace

from datapipeline.build.state import BuildState, save_build_state
from datapipeline.config.tasks import MetadataTask, OperationTask, SchemaTask
from datapipeline.profiles.models import ProfileRunRequest, RuntimeExecutionProfile
from datapipeline.profiles.orchestration import run_profiles
from datapipeline.services.artifacts import ArtifactManager
from datapipeline.services.bootstrap import build_state_path
from datapipeline.services.constants import VECTOR_SCHEMA_METADATA
from datapipeline.cli.visuals.execution_context import current_execution_scope


def _log_config():
    return (
        SimpleNamespace(name="INFO", value=20),
        SimpleNamespace(outputs=()),
    )


def test_run_profiles_executes_dependencies_then_target(monkeypatch, tmp_path):
    schema = SchemaTask(id="schema")
    metadata = MetadataTask(id="metadata", dependencies=["schema"])
    serve = OperationTask.model_validate(
        {
            "id": "serve",
            "entrypoint": "core.serve_pipeline",
            "dependencies": ["metadata"],
        }
    )

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        project_path=tmp_path / "project.yaml",
        tasks=[schema, metadata, serve],
        artifact_task_configs=[schema, metadata],
        profiles=[
            RuntimeExecutionProfile(
                kind="serve",
                name="serve",
                target_id="serve",
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
        "datapipeline.profiles.execution.dispatch_operation",
        lambda **kwargs: calls.__setitem__("dispatch", calls["dispatch"] + 1),
    )

    run_profiles(request)

    assert calls["build"] == 1
    assert calls["dispatch"] == 1


def test_run_profiles_can_skip_artifact_dependencies(monkeypatch, tmp_path):
    schema = SchemaTask(id="schema")
    serve = OperationTask.model_validate(
        {
            "id": "serve",
            "entrypoint": "core.serve_pipeline",
            "dependencies": ["schema"],
        }
    )

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        project_path=tmp_path / "project.yaml",
        tasks=[schema, serve],
        artifact_task_configs=[schema],
        profiles=[
            RuntimeExecutionProfile(
                kind="serve",
                name="serve",
                target_id="serve",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
                skip_artifacts=True,
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
        "datapipeline.profiles.execution.dispatch_operation",
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
            "id": "serve",
            "entrypoint": "core.serve_pipeline",
            "dependencies": ["schema"],
        }
    )

    log_decision, log_output = _log_config()
    runtime = SimpleNamespace(artifacts=ArtifactManager(tmp_path / "artifacts"))
    request = ProfileRunRequest(
        project_path=tmp_path / "project.yaml",
        tasks=[schema, serve],
        artifact_task_configs=[schema],
        profiles=[
            RuntimeExecutionProfile(
                kind="serve",
                name="serve",
                target_id="serve",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=runtime,
                dataset=object(),
            )
        ],
    )

    def _fake_build(*args, **kwargs):
        state = BuildState(config_hash="hash-1")
        state.register(
            VECTOR_SCHEMA_METADATA,
            "build/metadata.json",
            meta={"_config_hash": "hash-1"},
        )
        save_build_state(state, build_state_path(request.project_path))

    seen = {"synced": False}

    def _capture_dispatch(**kwargs):
        seen["synced"] = kwargs["runtime"].artifacts.has(VECTOR_SCHEMA_METADATA)

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_build_if_needed",
        _fake_build,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.build_state_path",
        lambda _project_path: build_state_path(request.project_path),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.dispatch_operation",
        _capture_dispatch,
    )

    run_profiles(request)

    assert seen["synced"] is True


def test_run_profiles_forward_runtime_build_mode(monkeypatch, tmp_path):
    schema = SchemaTask(id="schema")
    serve = OperationTask.model_validate(
        {
            "id": "serve",
            "entrypoint": "core.serve_pipeline",
            "dependencies": ["schema"],
        }
    )

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        project_path=tmp_path / "project.yaml",
        tasks=[schema, serve],
        artifact_task_configs=[schema],
        profiles=[
            RuntimeExecutionProfile(
                kind="serve",
                name="serve",
                target_id="serve",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
                build_options=SimpleNamespace(
                    build_mode="FORCE",
                    cli_log_level=None,
                    cli_visuals=None,
                    cli_log_outputs=(),
                    workspace=None,
                ),
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
        "datapipeline.profiles.execution.dispatch_operation",
        lambda **kwargs: None,
    )

    run_profiles(request)

    assert seen["runtime_build_mode"] == "FORCE"
    assert seen["profile_name_override"] == "serve.dependencies"


def test_runtime_dependency_build_scope_isolated_from_parent_profile(monkeypatch, tmp_path):
    schema = SchemaTask(id="schema")
    serve = OperationTask.model_validate(
        {
            "id": "serve",
            "entrypoint": "core.serve_pipeline",
            "dependencies": ["schema"],
        }
    )

    log_decision, log_output = _log_config()
    request = ProfileRunRequest(
        project_path=tmp_path / "project.yaml",
        tasks=[schema, serve],
        artifact_task_configs=[schema],
        profiles=[
            RuntimeExecutionProfile(
                kind="inspect",
                name="coverage",
                target_id="serve",
                visuals="on",
                log_decision=log_decision,
                log_output=log_output,
                runtime=object(),
                dataset=object(),
            )
        ],
    )

    captured_scope: dict[str, str] = {}

    def _capture_selected_artifacts(**_kwargs):
        scope = current_execution_scope() or {}
        captured_scope.update({k: str(v) for k, v in scope.items()})

    monkeypatch.setattr(
        "datapipeline.profiles.orchestration.run_profile",
        lambda spec, work: work(),
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.run_selected_artifacts",
        _capture_selected_artifacts,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.dispatch_operation",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        "datapipeline.profiles.execution.sync_runtime_artifacts_from_state",
        lambda *_args, **_kwargs: None,
    )

    run_profiles(request)

    assert captured_scope["profile_kind"] == "build"
    assert captured_scope["profile_name"] == "coverage.dependencies"
    assert captured_scope["phase"] == "dependencies"
    assert captured_scope["target_id"] == "dependencies"
