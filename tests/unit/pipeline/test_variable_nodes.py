import gc
import weakref
from datetime import datetime, timedelta, timezone

import pytest

from datapipeline.config.dataset.dataset import DatasetConfig, SampleConfig
from datapipeline.config.dataset.variable import SequenceConfig, VariableConfig
from datapipeline.domain.variable import VariableRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.variable.nodes import (
    VariableSequencer,
    project_variable_records,
    sequence_variables,
)
from datapipeline.pipelines.variable.pipeline import build_variable_nodes
from datapipeline.pipelines.variable.projector import VariableProjector
from datapipeline.runtime import Runtime, SourceRuntimeStream


class _EmptySource:
    def stream(self):
        return iter(())


def _identity(records):
    return records


def _context(
    tmp_path,
    partition_by: tuple[str, ...] = (),
) -> PipelineContext:
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
        dataset=DatasetConfig(sample=SampleConfig(cadence="1h")),
    )
    runtime.streams["stream"] = SourceRuntimeStream(
        source=_EmptySource(),
        mapper=_identity,
        preprocess=(),
        transforms=(),
        partition_by=partition_by,
        presorted=False,
    )
    return PipelineContext(runtime)


def _variable(
    value: object,
    position: int,
    variable_id: str = "x",
    entity_key: tuple = (),
) -> VariableRecord:
    return VariableRecord(
        id=variable_id,
        time=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=position),
        value=value,
        entity_key=entity_key,
    )


def test_variable_nodes_sequence_without_scaling(tmp_path) -> None:
    nodes = build_variable_nodes(
        _context(tmp_path),
        VariableConfig(
            stream="stream",
            id="x",
            field="value",
            scale=True,
            sequence=SequenceConfig(size=3, stride=2),
        ),
        group_by_cadence="1h",
    )

    assert [node.name for node in nodes] == [
        "project_variable_records",
        "sequence_variables",
    ]


def test_variable_nodes_omit_disabled_stages(tmp_path) -> None:
    nodes = build_variable_nodes(
        _context(tmp_path),
        VariableConfig(stream="stream", id="x", field="value"),
    )

    assert [node.name for node in nodes] == ["project_variable_records"]


def test_partitioned_variable_nodes_include_ordering(tmp_path) -> None:
    nodes = build_variable_nodes(
        _context(tmp_path, partition_by=("symbol",)),
        VariableConfig(stream="stream", id="x", field="value"),
        sample_keys=("symbol",),
        group_by_cadence="1h",
    )

    assert [node.name for node in nodes] == [
        "project_variable_records",
        "order_variable_records",
    ]
    assert nodes[-1].progress is not None


def test_sequence_variables_builds_strided_windows() -> None:
    variables = iter([_variable(value, value) for value in range(1, 6)])

    sequences = list(sequence_variables(SequenceConfig(size=3, stride=2), variables))

    assert [sequence.values for sequence in sequences] == [
        [1, 2, 3],
        [3, 4, 5],
    ]


def test_sequence_variables_tracks_each_variable_and_entity_independently() -> None:
    variables = iter(
        [
            _variable(1, 0, entity_key=("A",)),
            _variable(2, 1, entity_key=("A",)),
            _variable(10, 2, entity_key=("B",)),
            _variable(20, 3, entity_key=("B",)),
        ]
    )

    sequences = list(sequence_variables(SequenceConfig(size=2), variables))

    assert [(sequence.entity_key, sequence.values) for sequence in sequences] == [
        (("A",), [1, 2]),
        (("B",), [10, 20]),
    ]


def test_variable_sequencer_retains_only_the_active_entity() -> None:
    sequencer = VariableSequencer(SequenceConfig(size=2))

    for position in range(10_000):
        sequencer.append(
            _variable(position, position, entity_key=(f"entity-{position}",))
        )

    assert len(sequencer._window) == 1


def test_sequence_variables_preserves_none_values() -> None:
    variables = iter([_variable(1.0, 0), _variable(None, 1)])

    [sequence] = sequence_variables(SequenceConfig(size=2), variables)

    assert sequence.values == [1.0, None]


def test_projected_variable_does_not_retain_source_record() -> None:
    record = TemporalRecord(time=datetime(2024, 1, 1, tzinfo=timezone.utc))
    record.value = 1.0
    record_ref = weakref.ref(record)
    [variable] = VariableProjector((), SampleKeyContract(())).project(
        record,
        (VariableConfig(stream="stream", id="x", field="value"),),
    )

    del record
    gc.collect()

    assert record_ref() is None
    assert variable.time == datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_project_variable_records_rejects_sample_key_type_drift() -> None:
    first = TemporalRecord(time=datetime(2024, 1, 1, tzinfo=timezone.utc))
    first.value = 1.0
    first.security_id = True
    second = TemporalRecord(time=datetime(2024, 1, 2, tzinfo=timezone.utc))
    second.value = 2.0
    second.security_id = 1

    with pytest.raises(TypeError, match="changed type"):
        list(
            project_variable_records(
                VariableProjector(
                    ("security_id",),
                    SampleKeyContract(["security_id"]),
                ),
                VariableConfig(stream="stream", id="value", field="value"),
                iter([first, second]),
            )
        )
