from dataclasses import dataclass

import pytest

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.feature_id import base_id, feature_id_components
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.pipelines.feature import projector as projector_module
from datapipeline.pipelines.feature.projector import FeatureProjector


@dataclass
class _Record:
    station_id: object = None
    sensor: object = None


def _projected_id(record: _Record, feature_id_by: tuple[str, ...] | None) -> str:
    projector = FeatureProjector(feature_id_by, SampleKeyContract(()))
    config = FeatureRecordConfig(stream="stream", id="temp", field="sensor")
    return next(projector.project(record, (config,))).id


def test_feature_projector_without_id_components() -> None:
    assert _projected_id(_Record(), None) == "temp"


def test_feature_projector_projects_multiple_configs_with_one_entity_key() -> None:
    projector = FeatureProjector(None, SampleKeyContract(("station_id",)))
    configs = (
        FeatureRecordConfig(stream="stream", id="station", field="station_id"),
        FeatureRecordConfig(stream="stream", id="sensor", field="sensor"),
    )

    features = tuple(projector.project(_Record("north", 7), configs))

    assert [(feature.id, feature.value) for feature in features] == [
        ("station", "north"),
        ("sensor", 7),
    ]
    assert [feature.entity_key for feature in features] == [("north",), ("north",)]


def test_feature_projector_encodes_id_components_once_per_record(monkeypatch) -> None:
    encoded_fields: list[str] = []
    encode = projector_module.encode_feature_id_component

    def count_encode(field: str, value: object) -> str:
        encoded_fields.append(field)
        return encode(field, value)

    monkeypatch.setattr(projector_module, "encode_feature_id_component", count_encode)
    projector = FeatureProjector(
        ("station_id", "sensor"),
        SampleKeyContract(()),
    )
    configs = (
        FeatureRecordConfig(stream="stream", id="temperature", field="sensor"),
        FeatureRecordConfig(stream="stream", id="humidity", field="sensor"),
    )

    features = tuple(projector.project(_Record("north", 7), configs))

    assert len(features) == 2
    assert encoded_fields == ["station_id", "sensor"]


def test_feature_projector_tags_non_string_scalar_types() -> None:
    assert _projected_id(_Record(station_id=123), ("station_id",)) == (
        "temp__@station_id:!i:123"
    )
    assert _projected_id(_Record(station_id=True), ("station_id",)) == (
        "temp__@station_id:!b:1"
    )
    assert _projected_id(_Record(station_id=1.0), ("station_id",)) == (
        "temp__@station_id:!f:0x1.0000000000000p+0"
    )


def test_feature_projector_distinguishes_null_empty_and_scalar_types() -> None:
    identifiers = {
        _projected_id(_Record(station_id=None), ("station_id",)),
        _projected_id(_Record(station_id=""), ("station_id",)),
        _projected_id(_Record(station_id="1"), ("station_id",)),
        _projected_id(_Record(station_id=1), ("station_id",)),
        _projected_id(_Record(station_id=True), ("station_id",)),
        _projected_id(_Record(station_id=1.0), ("station_id",)),
    }

    assert len(identifiers) == 6


def test_feature_projector_escapes_component_delimiters() -> None:
    identifier = _projected_id(
        _Record(station_id="north__west|@sensor:x", sensor="A:B/100%"),
        ("station_id", "sensor"),
    )

    assert identifier == (
        "temp__@station_id:north__west%7C%40sensor%3Ax|@sensor:A%3AB%2F100%25"
    )
    assert base_id(identifier) == "temp"
    assert feature_id_components(identifier) == (
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


def test_feature_projector_rejects_unsupported_component_types() -> None:
    with pytest.raises(TypeError, match="string, integer, float, boolean, or null"):
        _projected_id(_Record(station_id=object()), ("station_id",))


def test_feature_projector_rejects_non_finite_components() -> None:
    with pytest.raises(ValueError, match="finite float"):
        _projected_id(_Record(station_id=float("nan")), ("station_id",))
