from datetime import datetime, timedelta, timezone

import pytest

from datapipeline.artifacts.scaler import (
    ScalerStatistics,
    StandardScalerArtifact,
    save_scaler_artifact,
)
from datapipeline.config.dataset.feature import SequenceConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.pipelines.feature.pipeline import build_feature_nodes
from datapipeline.pipelines.feature.nodes import (
    build_feature_stream,
    scale_features,
    sequence_features,
)
from datapipeline.runtime import IngestRuntimeStream, Runtime
from datapipeline.services.artifacts import ArtifactNotRegisteredError
from datapipeline.services.constants import SCALER_STATISTICS


class _EmptySource:
    def stream(self):
        return iter(())


def _identity(records):
    return records


def _context(tmp_path) -> PipelineContext:
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
    )
    runtime.streams["stream"] = IngestRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        transforms=(),
        partition_by=None,
        feature_id_by=None,
        presorted=False,
    )
    return PipelineContext(runtime)


def _feature(
    value: object,
    position: int,
    feature_id: str = "x",
    entity_key: tuple = (),
) -> FeatureRecord:
    return FeatureRecord(
        record=TemporalRecord(
            time=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=position)
        ),
        id=feature_id,
        value=value,
        entity_key=entity_key,
    )


def test_feature_nodes_make_scaling_and_sequence_explicit(tmp_path) -> None:
    nodes = build_feature_nodes(
        _context(tmp_path),
        record_stream_id="stream",
        feature_id="x",
        field="value",
        scale=True,
        sequence=SequenceConfig(size=3, stride=2),
    )

    assert [node.name for node in nodes] == [
        "build_feature_stream",
        "scale_features",
        "sequence_features",
        "order_feature_records",
    ]


def test_feature_nodes_omit_disabled_stages(tmp_path) -> None:
    nodes = build_feature_nodes(
        _context(tmp_path),
        record_stream_id="stream",
        feature_id="x",
        field="value",
        scale=False,
        sequence=None,
    )

    assert [node.name for node in nodes] == [
        "build_feature_stream",
        "order_feature_records",
    ]


def test_scale_features_loads_the_managed_typed_artifact(tmp_path) -> None:
    context = _context(tmp_path)
    path = context.runtime.artifacts_root / "scaler.json"
    save_scaler_artifact(
        path,
        StandardScalerArtifact(
            with_mean=True,
            with_std=True,
            epsilon=1e-12,
            observations=2,
            statistics={
                "x": ScalerStatistics(mean=2.0, std=2.0, count=2),
            },
        ),
    )
    context.artifacts.register(SCALER_STATISTICS, path.name)

    [scaled] = scale_features(context, iter([_feature(4.0, 0)]))

    assert scaled.value == 1.0


def test_scale_features_uses_the_shared_missing_artifact_error(tmp_path) -> None:
    with pytest.raises(ArtifactNotRegisteredError):
        list(scale_features(_context(tmp_path), iter([_feature(4.0, 0)])))


def test_sequence_features_builds_strided_windows() -> None:
    features = iter([_feature(value, value) for value in range(1, 6)])

    sequences = list(sequence_features(SequenceConfig(size=3, stride=2), features))

    assert [sequence.values for sequence in sequences] == [
        [1, 2, 3],
        [3, 4, 5],
    ]


def test_sequence_features_tracks_each_feature_and_entity_independently() -> None:
    features = iter(
        [
            _feature(1, 0, entity_key=("A",)),
            _feature(10, 1, entity_key=("B",)),
            _feature(2, 2, entity_key=("A",)),
            _feature(20, 3, entity_key=("B",)),
        ]
    )

    sequences = list(sequence_features(SequenceConfig(size=2), features))

    assert [(sequence.entity_key, sequence.values) for sequence in sequences] == [
        (("A",), [1, 2]),
        (("B",), [10, 20]),
    ]


def test_sequence_features_preserves_none_values() -> None:
    features = iter([_feature(1.0, 0), _feature(None, 1)])

    [sequence] = sequence_features(SequenceConfig(size=2), features)

    assert sequence.values == [1.0, None]


def test_feature_stream_rejects_sample_key_type_drift() -> None:
    first = TemporalRecord(time=datetime(2024, 1, 1, tzinfo=timezone.utc))
    first.value = 1.0
    first.security_id = True
    second = TemporalRecord(time=datetime(2024, 1, 2, tzinfo=timezone.utc))
    second.value = 2.0
    second.security_id = 1

    with pytest.raises(TypeError, match="changed type"):
        list(
            build_feature_stream(
                "value",
                "value",
                None,
                ["security_id"],
                iter([first, second]),
            )
        )
