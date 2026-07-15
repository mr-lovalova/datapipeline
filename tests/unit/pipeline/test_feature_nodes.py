import gc
import weakref
from datetime import datetime, timedelta, timezone

import pytest

from datapipeline.artifacts.registry import ArtifactNotRegisteredError
from datapipeline.artifacts.scaler import (
    ScalerStatistics,
    StandardScalerArtifact,
    save_scaler_artifact,
)
from datapipeline.artifacts.specs import SCALER_STATISTICS
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig, SequenceConfig
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.feature.nodes import (
    FeatureSequencer,
    build_feature_stream,
    scale_features,
    sequence_features,
)
from datapipeline.pipelines.feature.pipeline import build_feature_nodes
from datapipeline.pipelines.feature.projector import FeatureProjector
from datapipeline.runtime import Runtime, SourceRuntimeStream


class _EmptySource:
    def stream(self):
        return iter(())


def _identity(records):
    return records


def _context(tmp_path) -> PipelineContext:
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    runtime.streams["stream"] = SourceRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        preprocess=(),
        transforms=(),
        partition_by=(),
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
        FeatureRecordConfig(
            stream="stream",
            id="x",
            field="value",
            scale=True,
            sequence=SequenceConfig(size=3, stride=2),
        ),
    )

    assert [node.name for node in nodes] == [
        "build_feature_stream",
        "scale_features",
        "sequence_features",
        "order_feature_records",
    ]
    assert nodes[-1].progress is not None


def test_feature_nodes_omit_disabled_stages(tmp_path) -> None:
    nodes = build_feature_nodes(
        _context(tmp_path),
        FeatureRecordConfig(stream="stream", id="x", field="value"),
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
            _feature(2, 1, entity_key=("A",)),
            _feature(10, 2, entity_key=("B",)),
            _feature(20, 3, entity_key=("B",)),
        ]
    )

    sequences = list(sequence_features(SequenceConfig(size=2), features))

    assert [(sequence.entity_key, sequence.values) for sequence in sequences] == [
        (("A",), [1, 2]),
        (("B",), [10, 20]),
    ]


def test_feature_sequencer_retains_only_the_active_entity() -> None:
    sequencer = FeatureSequencer(SequenceConfig(size=2))

    for position in range(10_000):
        sequencer.append(
            _feature(position, position, entity_key=(f"entity-{position}",))
        )

    assert len(sequencer._window) == 1


def test_sequence_features_preserves_none_values() -> None:
    features = iter([_feature(1.0, 0), _feature(None, 1)])

    [sequence] = sequence_features(SequenceConfig(size=2), features)

    assert sequence.values == [1.0, None]


def test_feature_sequencer_does_not_retain_source_records() -> None:
    record = TemporalRecord(time=datetime(2024, 1, 1, tzinfo=timezone.utc))
    record_ref = weakref.ref(record)
    feature = FeatureRecord(record=record, id="x", value=1.0)
    sequencer = FeatureSequencer(SequenceConfig(size=2))

    assert sequencer.append(feature) is None
    del feature, record
    gc.collect()

    assert record_ref() is None


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
                FeatureProjector(
                    ("security_id",),
                    SampleKeyContract(["security_id"]),
                ),
                FeatureRecordConfig(stream="stream", id="value", field="value"),
                iter([first, second]),
            )
        )
