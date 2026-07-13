from dataclasses import dataclass

import pytest

from datapipeline.domain.feature_id import base_id, feature_id_components
from datapipeline.pipelines.feature.keygen import FeatureIdGenerator


@dataclass
class _Record:
    station_id: object = None
    sensor: object = None


def test_feature_id_generator_without_components() -> None:
    generator = FeatureIdGenerator(None)

    assert generator.generate("temp", _Record()) == "temp"


def test_feature_id_generator_tags_non_string_scalar_types() -> None:
    generator = FeatureIdGenerator("station_id")

    assert generator.generate("temp", _Record(station_id=123)) == (
        "temp__@station_id:!i:123"
    )
    assert generator.generate("temp", _Record(station_id=True)) == (
        "temp__@station_id:!b:1"
    )
    assert generator.generate("temp", _Record(station_id=1.0)) == (
        "temp__@station_id:!f:0x1.0000000000000p+0"
    )


def test_feature_id_generator_distinguishes_null_empty_and_scalar_types() -> None:
    generator = FeatureIdGenerator("station_id")

    identifiers = {
        generator.generate("temp", _Record(station_id=None)),
        generator.generate("temp", _Record(station_id="")),
        generator.generate("temp", _Record(station_id="1")),
        generator.generate("temp", _Record(station_id=1)),
        generator.generate("temp", _Record(station_id=True)),
        generator.generate("temp", _Record(station_id=1.0)),
    }

    assert len(identifiers) == 6


def test_feature_id_generator_escapes_component_delimiters() -> None:
    generator = FeatureIdGenerator(["station_id", "sensor"])
    identifier = generator.generate(
        "temp",
        _Record(station_id="north__west|@sensor:x", sensor="A:B/100%"),
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
    generator = FeatureIdGenerator(["station_id", "sensor"])

    first = generator.generate(
        "temp",
        _Record(station_id="north|@sensor:south", sensor="x"),
    )
    second = generator.generate(
        "temp",
        _Record(station_id="north", sensor="south|@sensor:x"),
    )

    assert first != second


def test_feature_id_generator_rejects_unsupported_component_types() -> None:
    generator = FeatureIdGenerator("station_id")

    with pytest.raises(TypeError, match="string, integer, float, boolean, or null"):
        generator.generate("temp", _Record(station_id=object()))


def test_feature_id_generator_rejects_non_finite_components() -> None:
    generator = FeatureIdGenerator("station_id")

    with pytest.raises(ValueError, match="finite float"):
        generator.generate("temp", _Record(station_id=float("nan")))
