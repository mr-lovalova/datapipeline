from datetime import datetime, timezone
from pathlib import Path

import pytest

from datapipeline.artifacts import fingerprints
from datapipeline.artifacts.fingerprints import calculate_artifact_hashes
from datapipeline.artifacts.specs import (
    VECTOR_INPUTS,
    VECTOR_METADATA,
    VECTOR_SCHEMA,
)
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.streams import StreamsConfig
from datapipeline.config.tasks import ArtifactTask, SchemaTask, VectorInputsTask
from datapipeline.services import config_inventory
from datapipeline.services.pipeline import load_pipeline
from datapipeline.services.runtime_compiler import compile_runtime
from datapipeline.utils import load as yaml_loader


def _write_pipeline(root: Path) -> Path:
    for name in ("sources", "streams", "operations", "profiles"):
        (root / name).mkdir(parents=True)
    project_yaml = root / "project.yaml"
    project_yaml.write_text(
        """schema_version: 2
artifact_revision: 1
name: snapshot
paths:
  sources: sources
  streams: streams
  operations: operations
  profiles: profiles
  dataset: dataset.yaml
  artifacts: artifacts
""",
        encoding="utf-8",
    )
    (root / "dataset.yaml").write_text(
        "sample: {cadence: 1h}\nfeatures: []\ntargets: []\n",
        encoding="utf-8",
    )
    return project_yaml


def test_load_pipeline_parses_each_pipeline_document_once(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_yaml = _write_pipeline(tmp_path)
    (tmp_path / "profiles" / "serve.broken.yaml").write_text(
        "this profile is intentionally: [invalid",
        encoding="utf-8",
    )
    original_load = yaml_loader.yaml.load
    parsed_documents = 0

    def count_parse(content, *, Loader):
        nonlocal parsed_documents
        parsed_documents += 1
        return original_load(content, Loader=Loader)

    monkeypatch.setattr(yaml_loader.yaml, "load", count_parse)

    definition = load_pipeline(project_yaml)

    assert definition.project.path == project_yaml.resolve()
    assert parsed_documents == 2


def test_compile_runtime_uses_the_loaded_definition(tmp_path: Path) -> None:
    project_yaml = _write_pipeline(tmp_path)
    definition = load_pipeline(project_yaml)
    project_yaml.unlink()
    (tmp_path / "dataset.yaml").unlink()

    first = compile_runtime(definition)
    second = compile_runtime(definition)
    first.window_bounds = (
        datetime(2020, 1, 1, tzinfo=timezone.utc),
        datetime(2021, 1, 1, tzinfo=timezone.utc),
    )
    first.dataset.features.append(
        FeatureRecordConfig(id="local", stream="local", field="value")
    )

    assert first is not second
    assert first.dataset != definition.dataset
    assert second.dataset == definition.dataset
    assert first.dataset is not definition.dataset
    assert second.dataset is not definition.dataset
    assert first.dataset is not second.dataset
    assert first.streams == second.streams == {}
    assert first.streams is not second.streams
    assert definition.dataset.features == []
    assert second.dataset.features == []
    assert second.window_bounds is None


def test_pipeline_definition_keeps_resolved_environment_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_yaml = _write_pipeline(tmp_path)
    (tmp_path / "sources" / "prices.yaml").write_text(
        """id: prices
parser: {entrypoint: identity}
loader:
  entrypoint: core.io
  args:
    transport: fs
    format: jsonl
    path: ${env:SOURCE_PATH}
""",
        encoding="utf-8",
    )
    current_environment = {"SOURCE_PATH": "data/first.jsonl"}
    monkeypatch.setattr(
        "datapipeline.services.project.merged_project_env",
        lambda _project_yaml: dict(current_environment),
    )
    first = load_pipeline(project_yaml)

    source = first.streams.sources["prices"]
    assert source.loader.args.path == "data/first.jsonl"

    current_environment["SOURCE_PATH"] = "data/second.jsonl"
    second = load_pipeline(project_yaml)

    assert first.streams.sources["prices"].loader.args.path == "data/first.jsonl"
    assert second.streams.sources["prices"].loader.args.path == "data/second.jsonl"


def test_load_pipeline_canonicalizes_symlinked_dataset_path(tmp_path: Path) -> None:
    project_yaml = _write_pipeline(tmp_path)
    dataset = tmp_path / "dataset.yaml"
    target = tmp_path / "dataset.actual.yaml"
    dataset.rename(target)
    dataset.symlink_to(target)

    definition = load_pipeline(project_yaml)

    assert definition.project.dataset_path == target.resolve()


def test_load_pipeline_canonicalizes_symlinked_config_root(
    tmp_path: Path,
) -> None:
    project_yaml = _write_pipeline(tmp_path)
    shared = tmp_path / "shared"
    (shared / "sources").mkdir(parents=True)
    (tmp_path / "current").symlink_to(shared, target_is_directory=True)
    project_yaml.write_text(
        project_yaml.read_text(encoding="utf-8").replace(
            "sources: sources",
            "sources: current/sources",
        ),
        encoding="utf-8",
    )

    definition = load_pipeline(project_yaml)

    assert definition.project.source_dirs == ((shared / "sources").resolve(),)


def test_load_pipeline_canonicalizes_symlinked_project_path(
    tmp_path: Path,
) -> None:
    actual = tmp_path / "actual"
    project_yaml = _write_pipeline(actual)
    (tmp_path / "current").symlink_to(actual, target_is_directory=True)

    definition = load_pipeline(tmp_path / "current" / project_yaml.name)

    assert definition.project.path == project_yaml.resolve()


def test_pipeline_definition_keeps_retargeted_yaml_symlink_snapshot(
    tmp_path: Path,
) -> None:
    project_yaml = _write_pipeline(tmp_path)
    first = tmp_path / "first-source.yaml"
    first.write_text(
        "id: linked\nparser: {entrypoint: identity}\n"
        "loader: {entrypoint: custom.loader}\n",
        encoding="utf-8",
    )
    second = tmp_path / "second-source.yaml"
    second.write_text(
        "id: linked\nparser: {entrypoint: identity}\n"
        "loader: {entrypoint: other.loader}\n",
        encoding="utf-8",
    )
    linked = tmp_path / "sources" / "linked.yaml"
    linked.symlink_to(first)

    definition = load_pipeline(project_yaml)
    assert definition.streams.sources["linked"].loader.entrypoint == "custom.loader"

    linked.unlink()
    linked.symlink_to(second)
    reloaded = load_pipeline(project_yaml)

    assert definition.streams.sources["linked"].loader.entrypoint == "custom.loader"
    assert reloaded.streams.sources["linked"].loader.entrypoint == "other.loader"


def test_pipeline_yaml_inventory_surfaces_scan_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def denied_walk(root, *, onerror, followlinks):
        assert root == tmp_path
        assert followlinks is False
        onerror(PermissionError("denied"))
        return ()

    monkeypatch.setattr(config_inventory.os, "walk", denied_walk)

    with pytest.raises(PermissionError, match="denied"):
        config_inventory.pipeline_yaml_files(tmp_path)


def test_pipeline_yaml_inventory_rejects_nested_symlink_directories(
    tmp_path: Path,
) -> None:
    shared = tmp_path / "shared"
    shared.mkdir()
    (tmp_path / "linked").symlink_to(shared, target_is_directory=True)

    with pytest.raises(
        ValueError,
        match="configuration directories must not be symlinks",
    ):
        config_inventory.pipeline_yaml_files(tmp_path)


def test_next_pipeline_definition_reloads_project_dotenv(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("SOURCE_PATH", raising=False)
    project_yaml = _write_pipeline(tmp_path)
    (tmp_path / "sources" / "prices.yaml").write_text(
        """id: prices
parser: {entrypoint: identity}
loader:
  entrypoint: core.io
  args:
    transport: fs
    format: jsonl
    path: ${env:SOURCE_PATH}
""",
        encoding="utf-8",
    )
    dotenv = tmp_path / ".env"
    dotenv.write_text("SOURCE_PATH=data/first.jsonl\n", encoding="utf-8")
    first = load_pipeline(project_yaml)

    dotenv.write_text("SOURCE_PATH=data/second.jsonl\n", encoding="utf-8")

    second = load_pipeline(project_yaml)
    assert first.streams.sources["prices"].loader.args.path == "data/first.jsonl"
    assert second.streams.sources["prices"].loader.args.path == "data/second.jsonl"


def test_project_without_name_still_validates_dataset_interpolation(
    tmp_path: Path,
) -> None:
    project_yaml = _write_pipeline(tmp_path)
    project_yaml.write_text(
        project_yaml.read_text(encoding="utf-8").replace(
            "name: snapshot\n",
            "",
        ),
        encoding="utf-8",
    )
    (tmp_path / "dataset.yaml").write_text(
        'sample: {cadence: "${unknown_cadence}"}\n',
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Unknown interpolation variable 'unknown_cadence'",
    ):
        load_pipeline(project_yaml)


def test_project_without_name_still_validates_operation_interpolation(
    tmp_path: Path,
) -> None:
    project_yaml = _write_pipeline(tmp_path)
    project_yaml.write_text(
        project_yaml.read_text(encoding="utf-8").replace(
            "name: snapshot\n",
            "",
        ),
        encoding="utf-8",
    )
    (tmp_path / "operations" / "custom.yaml").write_text(
        'kind: artifact\nentrypoint: plugin.artifact\noutput: "${unknown_output}"\n',
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="Unknown interpolation variable 'unknown_output'",
    ):
        load_pipeline(project_yaml)


def test_runtime_operation_change_does_not_change_artifact_hashes(
    tmp_path: Path,
) -> None:
    project_yaml = _write_pipeline(tmp_path)
    operation = tmp_path / "operations" / "custom.yaml"
    operation.write_text(
        "kind: runtime\nentrypoint: plugin.runtime.custom\noptions: {threshold: 1}\n",
        encoding="utf-8",
    )
    first = load_pipeline(project_yaml)

    operation.write_text(
        "kind: runtime\nentrypoint: plugin.runtime.custom\noptions: {threshold: 2}\n",
        encoding="utf-8",
    )
    second = load_pipeline(project_yaml)

    assert second.runtime_operations != first.runtime_operations
    assert second.artifact_hashes == first.artifact_hashes


def test_artifact_operation_change_changes_artifact_hashes(tmp_path: Path) -> None:
    project_yaml = _write_pipeline(tmp_path)
    operation = tmp_path / "operations" / "custom.yaml"
    operation.write_text(
        "kind: artifact\n"
        "entrypoint: plugin.artifact.custom\n"
        "output: build/first.json\n",
        encoding="utf-8",
    )
    first = load_pipeline(project_yaml)

    operation.write_text(
        "kind: artifact\n"
        "entrypoint: plugin.artifact.custom\n"
        "output: build/second.json\n",
        encoding="utf-8",
    )
    second = load_pipeline(project_yaml)

    assert second.artifact_hashes != first.artifact_hashes


def test_custom_artifact_change_does_not_change_core_artifact_hashes(
    tmp_path: Path,
) -> None:
    definition = load_pipeline(_write_pipeline(tmp_path))
    first_task = ArtifactTask(
        id="snapshot",
        entrypoint="plugin.snapshot",
        output="build/first.json",
    )
    second_task = first_task.model_copy(update={"output": "build/second.json"})

    first = calculate_artifact_hashes(
        definition.project,
        definition.dataset,
        definition.streams,
        (*definition.artifact_operations, first_task),
    )
    second = calculate_artifact_hashes(
        definition.project,
        definition.dataset,
        definition.streams,
        (*definition.artifact_operations, second_task),
    )

    assert first.for_artifact("snapshot") != second.for_artifact("snapshot")
    for key in (VECTOR_INPUTS, VECTOR_METADATA, VECTOR_SCHEMA):
        assert first.for_artifact(key) == second.for_artifact(key)


def test_artifact_hashing_rejects_missing_schema_dependencies(tmp_path: Path) -> None:
    definition = load_pipeline(_write_pipeline(tmp_path))

    with pytest.raises(
        ValueError,
        match="Required artifact operation 'vector_inputs' is not declared",
    ):
        calculate_artifact_hashes(
            definition.project,
            definition.dataset,
            definition.streams,
            (SchemaTask(),),
        )


def test_artifact_hashing_rejects_missing_active_scaler(tmp_path: Path) -> None:
    project_yaml = _write_pipeline(tmp_path)
    (tmp_path / "sources" / "prices.yaml").write_text(
        "id: prices\n"
        "parser: {entrypoint: identity}\n"
        "loader: {entrypoint: custom.loader}\n",
        encoding="utf-8",
    )
    (tmp_path / "streams" / "prices.yaml").write_text(
        "id: prices\nfrom: {source: prices}\nmap: {entrypoint: identity}\n",
        encoding="utf-8",
    )
    definition = load_pipeline(project_yaml)
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[
            FeatureRecordConfig(
                id="price",
                stream="prices",
                field="close",
                scale=True,
            )
        ],
    )

    with pytest.raises(
        ValueError,
        match="Required artifact operation 'scaler' is not declared",
    ):
        calculate_artifact_hashes(
            definition.project,
            dataset,
            definition.streams,
            (VectorInputsTask(),),
        )


def test_core_artifact_hashes_track_only_referenced_source_closure(
    tmp_path: Path,
) -> None:
    definition = load_pipeline(_write_pipeline(tmp_path))
    data = tmp_path / "data"
    data.mkdir()
    used = data / "used.jsonl"
    unused = data / "unused.jsonl"
    used.write_text("{}\n", encoding="utf-8")
    unused.write_text("{}\n", encoding="utf-8")
    dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[FeatureRecordConfig(id="price", stream="used", field="close")],
    )
    streams = StreamsConfig.model_validate(
        {
            "sources": {
                source_id: {
                    "id": source_id,
                    "parser": {"entrypoint": "parse"},
                    "loader": {
                        "entrypoint": "core.io",
                        "args": {
                            "transport": "fs",
                            "format": "jsonl",
                            "path": f"data/{source_id}.jsonl",
                        },
                    },
                }
                for source_id in ("used", "unused")
            },
            "streams": {
                source_id: {
                    "id": source_id,
                    "from": {"source": source_id},
                    "map": {"entrypoint": "map"},
                }
                for source_id in ("used", "unused")
            },
        }
    )

    baseline = calculate_artifact_hashes(
        definition.project,
        dataset,
        streams,
        definition.artifact_operations,
    )
    unused.write_text("unused changed\n", encoding="utf-8")
    after_unused_change = calculate_artifact_hashes(
        definition.project,
        dataset,
        streams,
        definition.artifact_operations,
    )
    assert after_unused_change.for_artifact(VECTOR_INPUTS) == baseline.for_artifact(
        VECTOR_INPUTS
    )

    used.write_text("used changed\n", encoding="utf-8")
    after_used_change = calculate_artifact_hashes(
        definition.project,
        dataset,
        streams,
        definition.artifact_operations,
    )
    for key in (VECTOR_INPUTS, VECTOR_METADATA, VECTOR_SCHEMA):
        assert after_used_change.for_artifact(key) != baseline.for_artifact(key)


def test_artifact_operation_comment_does_not_change_artifact_hashes(
    tmp_path: Path,
) -> None:
    project_yaml = _write_pipeline(tmp_path)
    operation = tmp_path / "operations" / "custom.yaml"
    operation.write_text(
        "kind: artifact\n"
        "entrypoint: plugin.artifact.custom\n"
        "output: build/custom.json\n",
        encoding="utf-8",
    )
    first = load_pipeline(project_yaml)

    operation.write_text(
        operation.read_text(encoding="utf-8") + "# Documentation only.\n",
        encoding="utf-8",
    )
    second = load_pipeline(project_yaml)

    assert second.artifact_hashes == first.artifact_hashes


def test_artifact_revision_change_changes_artifact_hashes(tmp_path: Path) -> None:
    project_yaml = _write_pipeline(tmp_path)
    first = load_pipeline(project_yaml)

    project_yaml.write_text(
        project_yaml.read_text(encoding="utf-8").replace(
            "artifact_revision: 1\n",
            "artifact_revision: 2\n",
        ),
        encoding="utf-8",
    )
    second = load_pipeline(project_yaml)

    assert second.artifact_hashes != first.artifact_hashes


def test_hash_split_ratio_order_does_not_change_artifact_hash(tmp_path: Path) -> None:
    project_yaml = _write_pipeline(tmp_path)
    dataset = tmp_path / "dataset.yaml"
    dataset.write_text(
        """\
sample: {cadence: 1h}
features: []
targets: []
split:
  mode: hash
  ratios: {train: 0.8, test: 0.2}
""",
        encoding="utf-8",
    )
    first = load_pipeline(project_yaml)

    dataset.write_text(
        """\
sample: {cadence: 1h}
features: []
targets: []
split:
  mode: hash
  ratios: {test: 0.2, train: 0.8}
""",
        encoding="utf-8",
    )
    second = load_pipeline(project_yaml)

    assert first.artifact_hashes == second.artifact_hashes


def test_split_output_labels_do_not_change_artifact_hashes(tmp_path: Path) -> None:
    project_yaml = _write_pipeline(tmp_path)
    dataset = tmp_path / "dataset.yaml"
    dataset.write_text(
        """\
sample: {cadence: 1h}
features: []
targets: []
split:
  mode: hash
  ratios: {train: 0.8, test: 0.2}
  output_labels: [train]
""",
        encoding="utf-8",
    )
    first = load_pipeline(project_yaml)

    dataset.write_text(
        dataset.read_text(encoding="utf-8").replace("[train]", "[test]"),
        encoding="utf-8",
    )
    second = load_pipeline(project_yaml)

    assert first.dataset.split is not None
    assert second.dataset.split is not None
    assert first.dataset.split.output_labels != second.dataset.split.output_labels
    assert second.artifact_hashes == first.artifact_hashes


def test_artifact_cache_version_changes_artifact_hash(
    tmp_path: Path,
    monkeypatch,
) -> None:
    definition = load_pipeline(_write_pipeline(tmp_path))
    monkeypatch.setattr(
        fingerprints,
        "ARTIFACT_CACHE_VERSION",
        fingerprints.ARTIFACT_CACHE_VERSION + 1,
    )
    changed_artifact_hashes = calculate_artifact_hashes(
        definition.project,
        definition.dataset,
        definition.streams,
        definition.artifact_operations,
    )

    assert changed_artifact_hashes != definition.artifact_hashes
