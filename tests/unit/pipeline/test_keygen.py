from dataclasses import dataclass

from datapipeline.pipeline.utils.keygen import FeatureIdGenerator


@dataclass
class _Record:
    station_id: int | None = None
    sensor: str | None = None


def test_feature_id_generator_without_partition():
    gen = FeatureIdGenerator(None)
    assert gen.generate("temp", _Record()) == "temp"


def test_feature_id_generator_with_single_partition():
    gen = FeatureIdGenerator("station_id")
    rec = _Record(station_id=123)
    assert gen.generate("temp", rec) == "temp__@station_id:123"


def test_feature_id_generator_with_multiple_partitions():
    gen = FeatureIdGenerator(["station_id", "sensor"])
    rec = _Record(station_id=123, sensor="ABC")
    assert (
        gen.generate("temp", rec)
        == "temp__@station_id:123_@sensor:ABC"
    )
