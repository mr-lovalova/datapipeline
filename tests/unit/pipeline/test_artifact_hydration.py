from datapipeline.artifacts.hydration import (
    hydrate_runtime_artifacts,
    hydrate_runtime_artifacts_for_project,
)
from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.artifacts.validation import NestedTickDependency
from datapipeline.build.config_hash import compute_config_hash
from datapipeline.build.state import BuildState, save_build_state
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    SchemaTask,
    TicksTask,
    VectorInputsTask,
)
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap, build_state_path
from datapipeline.services.constants import (
    VECTOR_INPUTS,
    VECTOR_SCHEMA,
    VECTOR_SCHEMA_METADATA,
)
from datapipeline.services.project_paths import tasks_dir


def test_hydration_replaces_registry_with_dependency_current_artifacts(
    tmp_path,
) -> None:
    custom = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    graph = build_artifact_graph(
        [
            VectorInputsTask(id="vector_inputs"),
            SchemaTask(id="schema"),
            MetadataTask(id="metadata"),
            custom,
        ]
    )
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
    )
    state = BuildState(config_hash="current")
    paths = {
        VECTOR_INPUTS: "build/vector-inputs.json",
        VECTOR_SCHEMA: "build/schema.json",
        VECTOR_SCHEMA_METADATA: "build/missing-metadata.json",
        "custom_snapshot": "build/custom.json",
    }
    for key, relative_path in paths.items():
        state.register(
            key,
            relative_path,
            meta={"_config_hash": "current"},
        )
    state.artifacts[VECTOR_INPUTS].meta["_config_hash"] = "old"

    for relative_path in (
        paths[VECTOR_INPUTS],
        paths[VECTOR_SCHEMA],
        paths["custom_snapshot"],
    ):
        destination = runtime.artifacts_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("{}", encoding="utf-8")

    for key, relative_path in paths.items():
        runtime.artifacts.register(key, relative_path)
    runtime.artifacts.register("orphan", "build/orphan.json")

    hydrated = hydrate_runtime_artifacts(
        runtime=runtime,
        graph=graph,
        state=state,
        config_hash="current",
        artifact_keys=paths,
    )

    assert hydrated == ("custom_snapshot",)
    assert runtime.artifacts.has("custom_snapshot")
    assert not runtime.artifacts.has(VECTOR_INPUTS)
    assert not runtime.artifacts.has(VECTOR_SCHEMA)
    assert not runtime.artifacts.has(VECTOR_SCHEMA_METADATA)
    assert not runtime.artifacts.has("orphan")


def test_hydration_supports_legacy_top_level_config_hash(tmp_path) -> None:
    task = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    graph = build_artifact_graph([task])
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
    )
    output = runtime.artifacts_root / task.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("{}", encoding="utf-8")
    state = BuildState(config_hash="current")
    state.register("custom_snapshot", task.output)

    hydrated = hydrate_runtime_artifacts(
        runtime=runtime,
        graph=graph,
        state=state,
        config_hash="current",
        artifact_keys={"custom_snapshot"},
    )

    assert hydrated == ("custom_snapshot",)
    assert runtime.artifacts.has("custom_snapshot")


def test_hydration_skips_incomplete_unrelated_artifact_chain(tmp_path) -> None:
    custom = ArtifactTask(
        id="custom_snapshot",
        entrypoint="plugin.snapshot",
        output="build/custom.json",
    )
    schema = SchemaTask(id="schema")
    graph = build_artifact_graph([custom, schema])
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
    )
    state = BuildState(config_hash="current")
    paths = {
        "custom_snapshot": "build/custom.json",
        VECTOR_INPUTS: "build/vector-inputs.json",
        VECTOR_SCHEMA: "build/schema.json",
    }
    for key, relative_path in paths.items():
        destination = runtime.artifacts_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("{}", encoding="utf-8")
        state.register(key, relative_path, meta={"_config_hash": "current"})

    hydrated = hydrate_runtime_artifacts(
        runtime=runtime,
        graph=graph,
        state=state,
        config_hash="current",
        artifact_keys=paths,
    )

    assert hydrated == ("custom_snapshot",)
    assert runtime.artifacts.has("custom_snapshot")
    assert not runtime.artifacts.has(VECTOR_INPUTS)
    assert not runtime.artifacts.has(VECTOR_SCHEMA)


def test_project_hydration_excludes_nested_tick_and_dependents(
    monkeypatch,
    tmp_path,
) -> None:
    tick = TicksTask(
        id="derived_ticks",
        stream="derived",
        output="build/derived-ticks.jsonl",
    )
    vector_inputs = VectorInputsTask(id="vector_inputs")
    graph = build_artifact_graph([tick, vector_inputs])
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
    )
    state = BuildState(config_hash="current")
    for key, relative_path in (
        ("derived_ticks", tick.output),
        (VECTOR_INPUTS, vector_inputs.output),
    ):
        destination = runtime.artifacts_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text("{}", encoding="utf-8")
        state.register(key, relative_path, meta={"_config_hash": "current"})
    runtime.artifacts.register("derived_ticks", tick.output)
    runtime.artifacts.register(VECTOR_INPUTS, vector_inputs.output)
    monkeypatch.setattr(
        "datapipeline.artifacts.hydration.load_build_state",
        lambda _state_path: state,
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.hydration.build_state_path",
        lambda _project_path: tmp_path / "state.json",
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.hydration.tasks_dir",
        lambda _project_path: tmp_path,
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.hydration.compute_config_hash",
        lambda *_args: "current",
    )
    monkeypatch.setattr(
        "datapipeline.artifacts.hydration.nested_tick_dependencies",
        lambda *_args: (
            NestedTickDependency(
                task=tick,
                cadence_artifacts=frozenset({"base_ticks"}),
            ),
        ),
    )

    hydrated = hydrate_runtime_artifacts_for_project(
        runtime,
        runtime.project_yaml,
        graph=graph,
        dataset=FeatureDatasetConfig(group_by="1h"),
    )

    assert hydrated == ()
    assert not runtime.artifacts.has("derived_ticks")
    assert not runtime.artifacts.has(VECTOR_INPUTS)


def test_project_hydration_rejects_artifact_after_config_changes(tmp_path) -> None:
    project_path = tmp_path / "project.yaml"
    project_path.write_text(
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
    (tmp_path / "dataset.yaml").write_text("{}\n", encoding="utf-8")
    (tmp_path / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    for directory in ("ingests", "streams", "sources"):
        (tmp_path / directory).mkdir()
    operations = tmp_path / "tasks/operations"
    operations.mkdir(parents=True)
    task_path = operations / "custom.yaml"
    task_path.write_text(
        "\n".join(
            [
                "id: custom_snapshot",
                "kind: artifact",
                "entrypoint: plugin.snapshot",
                "output: build/custom.json",
            ]
        ),
        encoding="utf-8",
    )
    runtime = Runtime(
        project_yaml=project_path,
        artifacts_root=tmp_path / "artifacts",
    )
    output = runtime.artifacts_root / "build/custom.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("{}", encoding="utf-8")
    config_hash = compute_config_hash(project_path, tasks_dir(project_path))
    state = BuildState(config_hash=config_hash)
    state.register(
        "custom_snapshot",
        "build/custom.json",
        meta={"_config_hash": config_hash},
    )
    save_build_state(state, build_state_path(project_path))

    runtime = bootstrap(project_path)
    assert runtime.artifacts.has("custom_snapshot")

    task_path.write_text(task_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    assert hydrate_runtime_artifacts_for_project(runtime, project_path) == ()
    assert not runtime.artifacts.has("custom_snapshot")
