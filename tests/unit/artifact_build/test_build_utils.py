import json
import math
from datetime import datetime, timezone

import pytest

from datapipeline.artifacts.models import VectorSchemaArtifact
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import MetadataTask, SchemaTask
from datapipeline.dag.context import PipelineContext
from datapipeline.operations.artifacts.metadata import (
    _window_bounds_from_stats,
    _window_size,
)
from datapipeline.operations.artifacts.metadata import materialize_metadata
from datapipeline.operations.artifacts.schema import materialize_vector_schema
from datapipeline.operations.artifacts import utils as artifact_utils
from datapipeline.operations.artifacts.utils import (
    collect_schema_entries,
    metadata_entries_from_stats,
)
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.runtime import Runtime
from datapipeline.services.constants import VECTOR_SCHEMA


def _hour(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc)


def _runtime_with_dataset(tmp_path, dataset_text: str) -> Runtime:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  ingests: ingests",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: build",
                "  tasks: tasks",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "dataset.yaml").write_text(dataset_text, encoding="utf-8")
    artifacts_root = tmp_path / "build"
    artifacts_root.mkdir()
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    runtime.registries.partition_by.register("market.prices", None)
    runtime.registries.feature_id_by.register("market.prices", None)
    return runtime


def test_collect_schema_entries_counts_nan(monkeypatch, tmp_path):
    """Ensure metadata collection treats NaN values as nulls."""
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    cfg = FeatureRecordConfig(
        id="wind_speed",
        record_stream="met.obs",
        field="value",
    )
    sample = Sample(key=(0,), features=Vector(values={"wind_speed": math.nan}))

    def fake_pipeline(
        context,
        configs,
        group_by_cadence,
        target_configs=None,
        rectangular=True,
        sample_keys=(),
    ):
        assert not rectangular
        assert sample_keys == ()
        return iter([sample])

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.utils.build_vector_pipeline",
        fake_pipeline,
    )

    stats, vector_count = collect_schema_entries(
        runtime,
        [cfg],
        group_by="1h",
        collect_metadata=True,
        progress_step="scan_features",
    )

    assert vector_count == 1
    entry = next(item for item in stats if item["id"] == "wind_speed")
    assert entry["present_count"] == 1
    assert entry["null_count"] == 1


def test_collect_schema_entries_emits_progress(monkeypatch, tmp_path):
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    cfg = FeatureRecordConfig(
        id="price",
        record_stream="market.prices",
        field="close",
    )
    samples = [
        Sample(key=(0,), features=Vector(values={"price": 1.0})),
        Sample(key=(1,), features=Vector(values={"price": 2.0})),
    ]
    trackers: list[tuple[str, float]] = []
    advances: list[int] = []

    class Progress:
        def __init__(self, step: str, interval_seconds: float) -> None:
            trackers.append((step, interval_seconds))

        def advance(self, items: int = 1) -> None:
            advances.append(items)

    runtime.heartbeat_interval_seconds = 180
    monkeypatch.setattr(
        artifact_utils,
        "build_vector_pipeline",
        lambda *_args, **_kwargs: iter(samples),
    )
    monkeypatch.setattr(artifact_utils, "OperationProgressTracker", Progress)

    collect_schema_entries(
        runtime,
        [cfg],
        group_by="1h",
        collect_metadata=False,
        progress_step="scan_features",
    )

    assert trackers == [("scan_features", 180)]
    assert advances == [1, 1]


def test_schema_materialization_rejects_configured_empty_features(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime_with_dataset(
        tmp_path,
        "\n".join(
            [
                "group_by: 1h",
                "features:",
                "  - id: price",
                "    record_stream: market.prices",
                "    field: close",
                "targets: []",
                "",
            ]
        ),
    )

    def empty_collection(*args, **kwargs):
        return [], 0

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.schema.collect_schema_entries",
        empty_collection,
    )

    with pytest.raises(RuntimeError, match="configured features produced zero vectors"):
        materialize_vector_schema(runtime, SchemaTask(output="schema.json"))

    assert not (runtime.artifacts_root / "schema.json").exists()


def test_schema_materialization_omits_legacy_metadata(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime_with_dataset(
        tmp_path,
        "\n".join(
            [
                "group_by: 1h",
                "features:",
                "  - id: price",
                "    record_stream: market.prices",
                "    field: close",
                "targets: []",
                "",
            ]
        ),
    )

    def collected_entries(*args, **kwargs):
        return [
            {
                "id": "price__@area:DK1",
                "base_id": "price",
                "kind": "list",
                "max_length": 3,
            }
        ], 1

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.schema.collect_schema_entries",
        collected_entries,
    )

    materialize_vector_schema(runtime, SchemaTask(output="schema.json"))

    payload = json.loads(
        (runtime.artifacts_root / "schema.json").read_text(encoding="utf-8")
    )
    assert "generated_at" not in payload
    assert payload["features"] == [
        {
            "id": "price__@area:DK1",
            "kind": "list",
            "cadence": {"target": 3},
        }
    ]


_EMPTY_SCHEMA = {"schema_version": 2, "features": [], "targets": []}


def _schema_with_feature(entry: dict) -> dict:
    return {**_EMPTY_SCHEMA, "features": [entry]}


@pytest.mark.parametrize(
    "document",
    [
        {"features": [], "targets": []},
        {**_EMPTY_SCHEMA, "schema_version": 2.0},
        {**_EMPTY_SCHEMA, "schema_version": 1},
        {"schema_version": 2, "features": []},
        {**_EMPTY_SCHEMA, "generated_at": "2024-01-01T00:00:00Z"},
        _schema_with_feature({"id": "x"}),
        _schema_with_feature({"id": "x", "kind": "scalar", "base_id": "x"}),
        _schema_with_feature({"id": "x", "kind": "scalar", "cadence": {"target": 2}}),
        _schema_with_feature({"id": "x", "kind": "list", "cadence": {"target": 0}}),
        _schema_with_feature({"id": "x", "kind": "list", "cadence": {"target": True}}),
        _schema_with_feature(
            {
                "id": "x",
                "kind": "list",
                "cadence": {"target": 2, "strategy": "max"},
            }
        ),
        {
            **_EMPTY_SCHEMA,
            "features": [
                {"id": "price", "kind": "scalar"},
                {"id": "price", "kind": "scalar"},
            ],
        },
    ],
)
def test_schema_artifact_rejects_invalid_documents(document: dict) -> None:
    with pytest.raises(ValueError):
        VectorSchemaArtifact.model_validate(document)


def test_schema_artifact_accepts_list_without_cadence() -> None:
    schema = VectorSchemaArtifact.model_validate(
        {
            "schema_version": 2,
            "features": [{"id": "sequence", "kind": "list"}],
            "targets": [],
        }
    )

    assert schema.features[0].id == "sequence"


def test_pipeline_context_rejects_invalid_registered_schema(tmp_path) -> None:
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
    )
    runtime.artifacts_root.mkdir()
    schema_path = runtime.artifacts_root / "schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "features": [{"id": "price"}],
                "targets": [],
            }
        ),
        encoding="utf-8",
    )
    runtime.artifacts.register(VECTOR_SCHEMA, "schema.json")

    with pytest.raises(ValueError, match="kind"):
        PipelineContext(runtime).load_schema()


def test_metadata_materialization_rejects_configured_empty_features(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime_with_dataset(
        tmp_path,
        "\n".join(
            [
                "group_by: 1h",
                "features:",
                "  - id: price",
                "    record_stream: market.prices",
                "    field: close",
                "targets: []",
                "",
            ]
        ),
    )

    def empty_collection(*args, **kwargs):
        return [], 0, {}

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.metadata.collect_schema_entries_and_sample_domain",
        empty_collection,
    )

    with pytest.raises(RuntimeError, match="configured features produced zero vectors"):
        materialize_metadata(runtime, MetadataTask(output="metadata.json"))

    assert not (runtime.artifacts_root / "metadata.json").exists()


def test_metadata_materialization_writes_keyed_sample_domain(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime_with_dataset(
        tmp_path,
        "\n".join(
            [
                "sample:",
                "  cadence: 1h",
                "  keys: [security_id]",
                "features:",
                "  - id: price",
                "    record_stream: market.prices",
                "    field: close",
                "targets: []",
                "",
            ]
        ),
    )
    start = datetime(2024, 1, 1, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 1, tzinfo=timezone.utc)

    def collected_entries(*args, **kwargs):
        return (
            [
                {
                    "id": "price",
                    "base_id": "price",
                    "kind": "scalar",
                    "present_count": 2,
                    "null_count": 0,
                    "first_ts": start,
                    "last_ts": end,
                }
            ],
            2,
            {("AAPL",): (start, end)},
        )

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.metadata.collect_schema_entries_and_sample_domain",
        collected_entries,
    )

    materialize_metadata(runtime, MetadataTask(output="metadata.json"))

    payload = json.loads(
        (runtime.artifacts_root / "metadata.json").read_text(encoding="utf-8")
    )
    assert payload["sample"] == {
        "cadence": "1h",
        "keys": ["security_id"],
        "domain": [
            {
                "key": ["AAPL"],
                "start": "2024-01-01T00:00:00Z",
                "end": "2024-01-01T01:00:00Z",
            }
        ],
    }


def test_metadata_entries_include_observation_bounds():
    stats = [
        {
            "id": "temp",
            "base_id": "temp",
            "kind": "scalar",
            "present_count": 4,
            "null_count": 0,
            "first_ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
            "last_ts": datetime(2024, 1, 2, tzinfo=timezone.utc),
        }
    ]

    entries = metadata_entries_from_stats(stats)

    assert entries[0]["first_observed"] == "2024-01-01T00:00:00Z"
    assert entries[0]["last_observed"] == "2024-01-02T00:00:00Z"


def test_window_bounds_modes():
    feature_stats = [
        {
            "id": "wind__@A",
            "base_id": "wind",
            "first_ts": _hour(0),
            "last_ts": _hour(6),
        },
        {
            "id": "wind__@B",
            "base_id": "wind",
            "first_ts": _hour(2),
            "last_ts": _hour(5),
        },
        {"id": "temp", "base_id": "temp", "first_ts": _hour(1), "last_ts": _hour(7)},
    ]
    target_stats: list[dict] = []

    start, end = _window_bounds_from_stats(feature_stats, target_stats, mode="union")
    assert start == _hour(0)
    assert end == _hour(7)

    start, end = _window_bounds_from_stats(
        feature_stats, target_stats, mode="intersection"
    )
    assert start == _hour(1)
    assert end == _hour(6)

    start, end = _window_bounds_from_stats(feature_stats, target_stats, mode="strict")
    assert start == _hour(2)
    assert end == _hour(5)

    start, end = _window_bounds_from_stats(feature_stats, target_stats, mode="relaxed")
    assert start == _hour(0)
    assert end == _hour(7)


def test_window_size_counts_cadence_buckets():
    start = _hour(4)
    end = _hour(10)

    assert _window_size(start, end, "1h") == 7  # hours 4..10 inclusive
    assert _window_size(start, end, "2h") == 4  # 4,6,8,10
    assert _window_size(start, end, "15m") == 25  # 6 hours -> 360 minutes / 15 + 1
    assert _window_size(start, end, None) is None


def test_window_size_rejects_invalid_cadence():
    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="Unsupported cadence"):
        _window_size(timestamp, timestamp, "0m")
