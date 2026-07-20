import math
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from datapipeline.config.dataset.variable import VariableConfig
from datapipeline.domain.variable_id import base_id, variable_id_components
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.pipelines.variable import projector as projector_module
from datapipeline.pipelines.variable.projector import VariableProjector


@dataclass
class _Record:
    station_id: object = None
    sensor: object = None
    time: datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)


def _projected_id(
    record: _Record,
    partition_by: tuple[str, ...],
    sample_keys: tuple[str, ...] = (),
) -> str:
    projector = VariableProjector(partition_by, SampleKeyContract(sample_keys))
    config = VariableConfig(stream="stream", id="temp", field="sensor")
    return next(projector.project(record, (config,))).id


def test_variable_projector_without_id_components() -> None:
    assert _projected_id(_Record(), ()) == "temp"


def test_variable_projector_projects_long_identity_as_entity_key() -> None:
    projector = VariableProjector(
        ("station_id",),
        SampleKeyContract(("station_id",)),
    )
    configs = (
        VariableConfig(stream="stream", id="station", field="station_id"),
        VariableConfig(stream="stream", id="sensor", field="sensor"),
    )

    variables = tuple(projector.project(_Record("north", 7), configs))

    assert [(variable.id, variable.value) for variable in variables] == [
        ("station", "north"),
        ("sensor", 7),
    ]
    assert [variable.entity_key for variable in variables] == [("north",), ("north",)]


def test_variable_projector_normalizes_nested_nan_without_mutating_record() -> None:
    record = _Record(sensor={"values": [1.0, float("nan")]})
    projector = VariableProjector((), SampleKeyContract(()))
    config = VariableConfig(stream="stream", id="sensor", field="sensor")

    [variable] = projector.project(record, (config,))

    assert variable.value == {"values": [1.0, None]}
    assert isinstance(record.sensor, dict)
    assert math.isnan(record.sensor["values"][1])


@pytest.mark.parametrize("value", [float("inf"), float("-inf")])
def test_variable_projector_rejects_nested_infinity(value: float) -> None:
    record = _Record(sensor={"values": [1.0, value]})
    projector = VariableProjector((), SampleKeyContract(()))
    config = VariableConfig(stream="stream", id="sensor", field="sensor")

    with pytest.raises(ValueError, match="must not contain infinity"):
        next(projector.project(record, (config,)))


def test_variable_projector_derives_wide_variable_identity() -> None:
    identifier = _projected_id(
        _Record(station_id="north"),
        ("station_id",),
    )

    assert identifier == "temp__@station_id:north"


def test_variable_projector_derives_hybrid_variable_identity() -> None:
    identifier = _projected_id(
        _Record(station_id="north", sensor="temperature"),
        ("station_id", "sensor"),
        ("station_id",),
    )

    assert identifier == "temp__@sensor:temperature"


def test_variable_projector_encodes_id_components_once_per_record(monkeypatch) -> None:
    encoded_fields: list[str] = []
    encode = projector_module.encode_variable_id_component

    def count_encode(field: str, value: object) -> str:
        encoded_fields.append(field)
        return encode(field, value)

    monkeypatch.setattr(projector_module, "encode_variable_id_component", count_encode)
    projector = VariableProjector(
        ("station_id", "sensor"),
        SampleKeyContract(()),
    )
    configs = (
        VariableConfig(stream="stream", id="temperature", field="sensor"),
        VariableConfig(stream="stream", id="humidity", field="sensor"),
    )

    variables = tuple(projector.project(_Record("north", 7), configs))

    assert len(variables) == 2
    assert encoded_fields == ["station_id", "sensor"]


def test_variable_projector_tags_non_string_scalar_types() -> None:
    assert _projected_id(_Record(station_id=123), ("station_id",)) == (
        "temp__@station_id:!i:123"
    )
    assert _projected_id(_Record(station_id=True), ("station_id",)) == (
        "temp__@station_id:!b:1"
    )
    assert _projected_id(_Record(station_id=1.0), ("station_id",)) == (
        "temp__@station_id:!f:0x1.0000000000000p+0"
    )


def test_variable_projector_distinguishes_null_empty_and_scalar_types() -> None:
    identifiers = {
        _projected_id(_Record(station_id=None), ("station_id",)),
        _projected_id(_Record(station_id=""), ("station_id",)),
        _projected_id(_Record(station_id="1"), ("station_id",)),
        _projected_id(_Record(station_id=1), ("station_id",)),
        _projected_id(_Record(station_id=True), ("station_id",)),
        _projected_id(_Record(station_id=1.0), ("station_id",)),
    }

    assert len(identifiers) == 6


def test_variable_projector_escapes_component_delimiters() -> None:
    identifier = _projected_id(
        _Record(station_id="north__west|@sensor:x", sensor="A:B/100%"),
        ("station_id", "sensor"),
    )

    assert identifier == (
        "temp__@station_id:north__west%7C%40sensor%3Ax|@sensor:A%3AB%2F100%25"
    )
    assert base_id(identifier) == "temp"
    assert variable_id_components(identifier) == (
        ("station_id", "north__west|@sensor:x"),
        ("sensor", "A:B/100%"),
    )


def test_distinct_component_tuples_cannot_generate_the_same_id() -> None:
    first = _projected_id(
        _Record(station_id="north|@sensor:south", sensor="x"),
        ("station_id", "sensor"),
    )
    second = _projected_id(
        _Record(station_id="north", sensor="south|@sensor:x"),
        ("station_id", "sensor"),
    )

    assert first != second


def test_variable_projector_rejects_unsupported_component_types() -> None:
    with pytest.raises(TypeError, match="string, integer, float, boolean, or null"):
        _projected_id(_Record(station_id=object()), ("station_id",))


def test_variable_projector_rejects_non_finite_components() -> None:
    with pytest.raises(ValueError, match="finite float"):
        _projected_id(_Record(station_id=float("nan")), ("station_id",))
