import json
import math
from datetime import datetime, timezone

import pytest

from datapipeline.artifacts.models import VectorSchemaArtifact
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import MetadataTask, SchemaTask
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.artifacts.metadata import (
    _window_bounds_from_stats,
    _window_size,
)
from datapipeline.operations.artifacts.metadata import materialize_metadata
from datapipeline.operations.artifacts.schema import materialize_vector_schema
from datapipeline.operations.artifacts import utils as artifact_utils
from datapipeline.operations.artifacts.utils import (
    collect_vector_metadata,
    metadata_entries_from_stats,
    VectorMetadataStats,
)
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.runtime import IngestRuntimeStream, Runtime
from datapipeline.services.constants import VECTOR_METADATA, VECTOR_SCHEMA
from datapipeline.utils.load import load_yaml


def _hour(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc)


class _EmptySource:
    def stream(self):
        return iter(())


def _identity(records):
    return records


def _runtime_with_dataset(tmp_path, dataset_text: str) -> Runtime:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "artifact_revision: 1",
                "paths:",
                "  ingests: ingests",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  artifacts: build",
                "  operations: operations",
                "",
            ]
        ),
        encoding="utf-8",
    )
    dataset_path = tmp_path / "dataset.yaml"
    dataset_path.write_text(dataset_text, encoding="utf-8")
    artifacts_root = tmp_path / "build"
    artifacts_root.mkdir()
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig.model_validate(load_yaml(dataset_path)),
    )
    runtime.streams["market.prices"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=(),
        feature_id_by=None,
        presorted=False,
    )
    return runtime


def test_collect_vector_metadata_counts_nan(monkeypatch, tmp_path):
    """Ensure metadata collection treats NaN values as nulls."""
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\nartifact_revision: 1\n", encoding="utf-8")
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    cfg = FeatureRecordConfig(
        id="wind_speed",
        stream="met.obs",
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

    stats, vector_count, domain = collect_vector_metadata(
        runtime,
        [cfg],
        "1h",
        (),
        "scan_features",
    )

    assert vector_count == 1
    assert domain == {}
    entry = next(item for item in stats if item.id == "wind_speed")
    assert entry.present_count == 1
    assert entry.null_count == 1


def test_collect_vector_metadata_emits_progress(monkeypatch, tmp_path):
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\nartifact_revision: 1\n", encoding="utf-8")
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    cfg = FeatureRecordConfig(
        id="price",
        stream="market.prices",
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

    collect_vector_metadata(
        runtime,
        [cfg],
        "1h",
        (),
        "scan_features",
    )

    assert trackers == [("scan_features", 180)]
    assert advances == [1, 1]


@pytest.mark.parametrize(
    "values",
    [
        (1.0, [2.0]),
        ([1.0], 2.0),
    ],
)
def test_collect_vector_metadata_rejects_scalar_list_mixtures(
    monkeypatch,
    tmp_path,
    values,
) -> None:
    runtime = _runtime_with_dataset(tmp_path, "sample:\n  cadence: 1h\n")
    config = FeatureRecordConfig(
        id="history",
        stream="market.prices",
        field="close",
    )
    samples = [
        Sample(key=(index,), features=Vector(values={"history": value}))
        for index, value in enumerate(values)
    ]
    monkeypatch.setattr(
        artifact_utils,
        "build_vector_pipeline",
        lambda *_args, **_kwargs: iter(samples),
    )

    with pytest.raises(ValueError, match="both (scalar and list|list and scalar)"):
        collect_vector_metadata(
            runtime,
            [config],
            "1h",
            (),
            "scan_features",
        )


def test_collect_vector_metadata_rejects_variable_list_lengths(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime_with_dataset(tmp_path, "sample:\n  cadence: 1h\n")
    config = FeatureRecordConfig(
        id="history",
        stream="market.prices",
        field="close",
    )
    samples = [
        Sample(key=(0,), features=Vector(values={"history": [1.0]})),
        Sample(key=(1,), features=Vector(values={"history": [2.0, 3.0]})),
    ]
    monkeypatch.setattr(
        artifact_utils,
        "build_vector_pipeline",
        lambda *_args, **_kwargs: iter(samples),
    )

    with pytest.raises(ValueError, match="different lengths: 1 and 2"):
        collect_vector_metadata(
            runtime,
            [config],
            "1h",
            (),
            "scan_features",
        )


def test_schema_materialization_derives_entries_from_metadata(
    tmp_path,
) -> None:
    runtime = _runtime_with_dataset(
        tmp_path,
        "\n".join(
            [
                "sample:",
                "  cadence: 1h",
                "features:",
                "  - id: price",
                "    stream: market.prices",
                "    field: close",
                "targets: []",
                "",
            ]
        ),
    )

    metadata_path = runtime.artifacts_root / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "features": [
                    {
                        "id": "price__@area:DK1",
                        "base_id": "price",
                        "kind": "list",
                        "present_count": 1,
                        "null_count": 0,
                        "first_observed": "2024-01-01T00:00:00Z",
                        "last_observed": "2024-01-01T00:00:00Z",
                        "element_types": ["float"],
                        "lengths": {"3": 1},
                        "cadence": {"target": 3},
                        "observed_elements": 3,
                    }
                ],
                "targets": [
                    {
                        "id": "return",
                        "base_id": "return",
                        "kind": "scalar",
                        "present_count": 1,
                        "null_count": 0,
                        "first_observed": "2024-01-01T00:00:00Z",
                        "last_observed": "2024-01-01T00:00:00Z",
                        "value_types": ["float"],
                    }
                ],
                "counts": {"feature_vectors": 1, "target_vectors": 1},
            }
        ),
        encoding="utf-8",
    )
    runtime.artifacts.register(VECTOR_METADATA, "metadata.json")

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
    assert payload["targets"] == [{"id": "return", "kind": "scalar"}]


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


def test_schema_artifact_rejects_list_without_cadence() -> None:
    with pytest.raises(ValueError, match="list schema entries must define cadence"):
        VectorSchemaArtifact.model_validate(
            {
                "schema_version": 2,
                "features": [{"id": "sequence", "kind": "list"}],
                "targets": [],
            }
        )


def test_pipeline_context_rejects_invalid_registered_schema(tmp_path) -> None:
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
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
                "sample:",
                "  cadence: 1h",
                "features:",
                "  - id: price",
                "    stream: market.prices",
                "    field: close",
                "targets: []",
                "",
            ]
        ),
    )

    def empty_collection(*args, **kwargs):
        return [], 0, {}

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.metadata.collect_vector_metadata",
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
                "    stream: market.prices",
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
                VectorMetadataStats(
                    id="price",
                    base_id="price",
                    kind="scalar",
                    present_count=2,
                    first_observed=start,
                    last_observed=end,
                )
            ],
            2,
            {("AAPL",): (start, end)},
        )

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.metadata.collect_vector_metadata",
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


def test_metadata_materialization_preserves_one_timestamp_window(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime_with_dataset(
        tmp_path,
        "\n".join(
            [
                "sample:",
                "  cadence: 1h",
                "features:",
                "  - id: price",
                "    stream: market.prices",
                "    field: close",
                "targets: []",
                "",
            ]
        ),
    )
    observed_at = _hour(4)

    def collected_entries(*args, **kwargs):
        return (
            [
                VectorMetadataStats(
                    id="price",
                    base_id="price",
                    kind="scalar",
                    present_count=1,
                    first_observed=observed_at,
                    last_observed=observed_at,
                )
            ],
            1,
            {},
        )

    monkeypatch.setattr(
        "datapipeline.operations.artifacts.metadata.collect_vector_metadata",
        collected_entries,
    )

    materialize_metadata(runtime, MetadataTask(output="metadata.json"))

    path = runtime.artifacts_root / "metadata.json"
    first = path.read_bytes()
    payload = json.loads(first)
    assert payload["window"] == {
        "start": "2024-01-01T04:00:00Z",
        "end": "2024-01-01T04:00:00Z",
        "mode": "intersection",
        "size": 1,
    }
    assert "generated_at" not in payload

    materialize_metadata(runtime, MetadataTask(output="metadata.json"))

    assert path.read_bytes() == first


def test_metadata_entries_include_observation_bounds():
    stats = [
        VectorMetadataStats(
            id="temp",
            base_id="temp",
            kind="scalar",
            present_count=4,
            first_observed=datetime(2024, 1, 1, tzinfo=timezone.utc),
            last_observed=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
    ]

    entries = metadata_entries_from_stats(stats)

    assert entries[0].first_observed == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert entries[0].last_observed == datetime(2024, 1, 2, tzinfo=timezone.utc)


def test_window_bounds_modes():
    feature_stats = [
        VectorMetadataStats(
            id="wind__@A",
            base_id="wind",
            first_observed=_hour(0),
            last_observed=_hour(6),
        ),
        VectorMetadataStats(
            id="wind__@B",
            base_id="wind",
            first_observed=_hour(2),
            last_observed=_hour(5),
        ),
        VectorMetadataStats(
            id="temp",
            base_id="temp",
            first_observed=_hour(1),
            last_observed=_hour(7),
        ),
    ]
    target_stats: list[VectorMetadataStats] = []

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


def test_window_bounds_preserve_single_timestamp() -> None:
    stats = [
        VectorMetadataStats(
            id="price",
            base_id="price",
            first_observed=_hour(4),
            last_observed=_hour(4),
        )
    ]

    assert _window_bounds_from_stats(stats, [], mode="union") == (
        _hour(4),
        _hour(4),
    )
    assert _window_bounds_from_stats(stats, [], mode="intersection") == (
        _hour(4),
        _hour(4),
    )


def test_window_size_counts_cadence_buckets():
    start = _hour(4)
    end = _hour(10)

    assert _window_size(start, end, "1h") == 7  # hours 4..10 inclusive
    assert _window_size(start, end, "2h") == 4  # 4,6,8,10
    assert _window_size(start, end, "15m") == 25  # 6 hours -> 360 minutes / 15 + 1


def test_window_size_rejects_invalid_cadence():
    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="Unsupported cadence"):
        _window_size(timestamp, timestamp, "0m")
