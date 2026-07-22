import json
import math
from datetime import datetime, timezone

import pytest

from datapipeline.artifacts.registry import VECTOR_METADATA_SPEC
from datapipeline.artifacts.specs import VECTOR_METADATA
from datapipeline.config.dataset.dataset import DatasetConfig, SampleConfig
from datapipeline.config.dataset.series import SeriesConfig
from datapipeline.config.tasks import MetadataTask
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.artifacts import utils as artifact_utils
from datapipeline.operations.artifacts.metadata import (
    _window_bounds_from_stats,
    materialize_metadata,
)
from datapipeline.operations.artifacts.utils import (
    VectorMetadataStats,
    collect_vector_metadata,
    metadata_entries_from_stats,
)
from datapipeline.runtime import Runtime, SourceRuntimeStream
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
                "schema_version: 3",
                "artifact_revision: 1",
                "paths:",
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
        dataset=DatasetConfig.model_validate(load_yaml(dataset_path)),
    )
    runtime.streams["market.prices"] = SourceRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        preprocess=(),
        transforms=(),
        partition_by=(),
        presorted=False,
    )
    return runtime


def _collect_from_pipeline(
    monkeypatch,
    tmp_path,
    configs,
    pipeline,
    sample_keys=(),
):
    runtime = _runtime_with_dataset(tmp_path, "sample:\n  cadence: 1h\n")
    monkeypatch.setattr(
        artifact_utils,
        "build_vector_pipeline",
        lambda *_args, **_kwargs: pipeline,
    )
    return collect_vector_metadata(
        runtime,
        configs,
        "1h",
        sample_keys,
        "scan_features",
    )


def test_collect_vector_metadata_counts_nan(monkeypatch, tmp_path):
    """Ensure metadata collection treats NaN values as nulls."""
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "schema_version: 3\nartifact_revision: 1\n", encoding="utf-8"
    )
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=DatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    cfg = SeriesConfig(
        id="wind_speed",
        stream="met.obs",
        field="value",
    )
    sample = Sample(
        key=(_hour(0),),
        features=Vector(values={"wind_speed": math.nan}),
    )

    def fake_pipeline(
        context,
        configs,
        group_by_cadence,
        target_configs=None,
        rectangular=True,
        sample_keys=(),
    ):
        assert rectangular is False
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
    project_yaml.write_text(
        "schema_version: 3\nartifact_revision: 1\n", encoding="utf-8"
    )
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=DatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    cfg = SeriesConfig(
        id="price",
        stream="market.prices",
        field="close",
    )
    samples = [
        Sample(key=(_hour(0),), features=Vector(values={"price": 1.0})),
        Sample(key=(_hour(1),), features=Vector(values={"price": 2.0})),
    ]
    trackers: list[tuple[str, str, float]] = []
    advances: list[int] = []

    class Progress:
        def __init__(self, step: str, unit: str, interval_seconds: float) -> None:
            trackers.append((step, unit, interval_seconds))

        def advance(self, count: int = 1) -> None:
            advances.append(count)

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

    assert trackers == [("scan_features", "vectors", 180)]
    assert advances == [1, 1]


def test_collect_vector_metadata_uses_config_order_not_observation_order(
    monkeypatch,
    tmp_path,
) -> None:
    configs = [
        SeriesConfig(id="history", stream="market.prices", field="value"),
        SeriesConfig(id="price", stream="market.prices", field="value"),
        SeriesConfig(
            id="fundamental",
            stream="market.prices",
            field="value",
        ),
    ]
    samples = [
        Sample(
            key=(_hour(0),),
            features=Vector(
                values={
                    "price": 1.0,
                    "fundamental__@metric:revenue": 10.0,
                    "fundamental__@metric:debt": 5.0,
                }
            ),
        ),
        Sample(
            key=(_hour(1),),
            features=Vector(values={"history": [1.0, 2.0]}),
        ),
    ]
    stats, _, _ = _collect_from_pipeline(
        monkeypatch,
        tmp_path,
        configs,
        iter(samples),
    )

    assert [entry.id for entry in stats] == [
        "history",
        "price",
        "fundamental__@metric:debt",
        "fundamental__@metric:revenue",
    ]


def test_collect_vector_metadata_rejects_configured_ids_with_no_observations(
    monkeypatch,
    tmp_path,
) -> None:
    configs = [
        SeriesConfig(id="missing", stream="market.prices", field="value"),
        SeriesConfig(id="price", stream="market.prices", field="value"),
    ]
    with pytest.raises(RuntimeError, match="missing"):
        _collect_from_pipeline(
            monkeypatch,
            tmp_path,
            configs,
            iter(
                [
                    Sample(
                        key=(_hour(0),),
                        features=Vector(values={"price": 1.0}),
                    )
                ]
            ),
        )


def test_collect_vector_metadata_rejects_when_every_configured_id_is_empty(
    monkeypatch,
    tmp_path,
) -> None:
    config = SeriesConfig(
        id="price",
        stream="market.prices",
        field="value",
    )

    with pytest.raises(RuntimeError, match="price"):
        _collect_from_pipeline(monkeypatch, tmp_path, [config], iter(()))


@pytest.mark.parametrize(
    ("sample_keys", "key"),
    [
        ((), 1),
        ((), ()),
        ((), (0,)),
        ((), (_hour(0), "unexpected")),
        (("ticker",), (_hour(0),)),
    ],
)
def test_collect_vector_metadata_rejects_malformed_sample_keys(
    monkeypatch,
    tmp_path,
    sample_keys,
    key,
) -> None:
    config = SeriesConfig(
        id="price",
        stream="market.prices",
        field="value",
    )
    with pytest.raises(RuntimeError, match="sample key"):
        _collect_from_pipeline(
            monkeypatch,
            tmp_path,
            [config],
            iter([Sample(key=key, features=Vector(values={"price": 1.0}))]),
            sample_keys=sample_keys,
        )


def test_collect_vector_metadata_rejects_ids_outside_configured_vectors(
    monkeypatch,
    tmp_path,
) -> None:
    config = SeriesConfig(
        id="price",
        stream="market.prices",
        field="value",
    )

    with pytest.raises(RuntimeError, match="rogue"):
        _collect_from_pipeline(
            monkeypatch,
            tmp_path,
            [config],
            iter(
                [
                    Sample(
                        key=(_hour(0),),
                        features=Vector(values={"price": 1.0, "rogue": 2.0}),
                    )
                ]
            ),
        )


def test_collect_vector_metadata_closes_pipeline_when_collection_fails(
    monkeypatch,
    tmp_path,
) -> None:
    config = SeriesConfig(
        id="history",
        stream="market.prices",
        field="value",
    )
    closed = False

    def failing_pipeline():
        nonlocal closed
        try:
            yield Sample(
                key=(_hour(0),),
                features=Vector(values={"history": [1.0]}),
            )
            yield Sample(
                key=(_hour(1),),
                features=Vector(values={"history": [2.0, 3.0]}),
            )
        finally:
            closed = True

    pipeline = failing_pipeline()

    with pytest.raises(ValueError):
        _collect_from_pipeline(
            monkeypatch,
            tmp_path,
            [config],
            pipeline,
        )

    assert closed


def test_collect_vector_metadata_skips_sample_domain_without_sample_keys(
    monkeypatch,
    tmp_path,
) -> None:
    config = SeriesConfig(
        id="price",
        stream="market.prices",
        field="value",
    )
    samples = [
        Sample(key=(_hour(0),), features=Vector(values={"price": 1.0})),
        Sample(key=(_hour(1),), features=Vector(values={"price": 2.0})),
    ]
    _, _, domain = _collect_from_pipeline(
        monkeypatch, tmp_path, [config], iter(samples)
    )

    assert domain == {}


def test_collect_vector_metadata_tracks_each_keyed_sample_domain(
    monkeypatch,
    tmp_path,
) -> None:
    config = SeriesConfig(
        id="price",
        stream="market.prices",
        field="value",
    )
    samples = [
        Sample(
            key=(_hour(2), "AAPL"),
            features=Vector(values={"price": 2.0}),
        ),
        Sample(
            key=(_hour(0), "AAPL"),
            features=Vector(values={"price": 1.0}),
        ),
        Sample(
            key=(_hour(1), "MSFT"),
            features=Vector(values={"price": 3.0}),
        ),
    ]
    _, _, domain = _collect_from_pipeline(
        monkeypatch,
        tmp_path,
        [config],
        iter(samples),
        sample_keys=("ticker",),
    )

    assert domain == {
        ("AAPL",): (_hour(0), _hour(2)),
        ("MSFT",): (_hour(1), _hour(1)),
    }


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
    config = SeriesConfig(
        id="history",
        stream="market.prices",
        field="close",
    )
    samples = [
        Sample(key=(_hour(index),), features=Vector(values={"history": value}))
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


def test_collect_vector_metadata_rejects_series_list_lengths(
    monkeypatch,
    tmp_path,
) -> None:
    runtime = _runtime_with_dataset(tmp_path, "sample:\n  cadence: 1h\n")
    config = SeriesConfig(
        id="history",
        stream="market.prices",
        field="close",
    )
    samples = [
        Sample(key=(_hour(0),), features=Vector(values={"history": [1.0]})),
        Sample(
            key=(_hour(1),),
            features=Vector(values={"history": [2.0, 3.0]}),
        ),
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


def test_pipeline_context_rejects_invalid_registered_metadata(tmp_path) -> None:
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
        dataset=DatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    runtime.artifacts_root.mkdir()
    metadata_path = runtime.artifacts_root / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "features": [
                    {
                        "id": "history",
                        "base_id": "history",
                        "kind": "list",
                        "present_count": 1,
                        "null_count": 0,
                        "length": 0,
                        "observed_elements": 0,
                    }
                ],
                "targets": [],
                "counts": {"feature_vectors": 1, "target_vectors": 0},
            }
        ),
        encoding="utf-8",
    )
    runtime.artifacts.register(VECTOR_METADATA, "metadata.json")

    with pytest.raises(ValueError, match="length"):
        PipelineContext(runtime).require_artifact(VECTOR_METADATA_SPEC)


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


def test_metadata_entries_preserve_scalar_and_list_statistics() -> None:
    scalar = VectorMetadataStats(id="price", base_id="price")
    scalar.observe(None, _hour(0))
    scalar.observe(3, _hour(2))
    sequence = VectorMetadataStats(id="history", base_id="history")
    sequence.observe([1.0, None, "stale"], _hour(1))
    sequence.observe([None, 2.0, 3.0], _hour(3))
    null_only = VectorMetadataStats(id="missing", base_id="missing")
    null_only.observe(None, _hour(0))
    null_only.observe(float("nan"), _hour(1))

    entries = metadata_entries_from_stats([scalar, sequence, null_only])

    assert entries[0].model_dump() == {
        "id": "price",
        "base_id": "price",
        "kind": "scalar",
        "present_count": 2,
        "null_count": 1,
        "first_observed": _hour(0),
        "last_observed": _hour(2),
        "value_types": ("int",),
    }
    assert entries[1].model_dump() == {
        "id": "history",
        "base_id": "history",
        "kind": "list",
        "present_count": 2,
        "null_count": 0,
        "first_observed": _hour(1),
        "last_observed": _hour(3),
        "element_types": ("float", "null", "str"),
        "length": 3,
        "observed_elements": 4,
    }
    assert entries[2].model_dump() == {
        "id": "missing",
        "base_id": "missing",
        "kind": "scalar",
        "present_count": 2,
        "null_count": 2,
        "first_observed": _hour(0),
        "last_observed": _hour(1),
        "value_types": (),
    }


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
