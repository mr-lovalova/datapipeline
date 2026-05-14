import pytest

from datapipeline.config.catalog import IngestConfig, StreamConfig


def test_stream_rejects_old_kind_shape() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        StreamConfig.model_validate(
            {
                "kind": "ingest",
                "id": "sample",
                "source": "demo.source",
                "mapper": {"entrypoint": "map", "args": {}},
            }
        )


def test_manual_stream_requires_map_entrypoint() -> None:
    with pytest.raises(ValueError, match="map.entrypoint is required"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"streams": {"a": "stream.a"}},
            }
        )


def test_joined_stream_requires_primary() -> None:
    with pytest.raises(ValueError, match="from.join requires 'primary'"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"join": {"a": "stream.a"}},
                "map": {"entrypoint": "join", "args": {}},
            }
        )


def test_stream_rejects_from_source() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        StreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source", "mode": "left"},
            }
        )


def test_stream_rejects_record_transforms() -> None:
    with pytest.raises(ValueError, match="streams cannot define record transforms"):
        StreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"stream": "demo.ingest"},
                "record": [],
            }
        )


def test_ingest_rejects_stream_transforms() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        IngestConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
                "map": {"entrypoint": "identity", "args": {}},
                "stream": [],
            }
        )


def test_stream_accepts_upstream_stream_shape() -> None:
    spec = StreamConfig.model_validate(
        {
            "id": "sample",
            "from": {"stream": "sample.ingest"},
            "partition_by": "ticker",
            "stream": [{"dedupe": {}}],
        }
    )

    assert spec.input_refs() == {"stream": "sample.ingest"}


def test_manual_stream_rejects_join_options() -> None:
    with pytest.raises(ValueError, match="from.streams cannot define join options"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"streams": {"a": "stream.a"}, "on": "time"},
                "map": {"entrypoint": "manual", "args": {}},
            }
        )


def test_stream_rejects_duplicate_input_aliases_after_trimming() -> None:
    with pytest.raises(ValueError, match="duplicate alias"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"streams": {" x ": "stream.a", "x": "stream.b"}},
                "map": {"entrypoint": "manual", "args": {}},
            }
        )


def test_manual_stream_rejects_unknown_driver_alias() -> None:
    with pytest.raises(
        ValueError,
        match="map.args.driver must reference one of the declared input aliases",
    ):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"streams": {"x": "stream.a", "y": "stream.b"}},
                "map": {"entrypoint": "manual", "args": {"driver": "missing"}},
            }
        )


def test_joined_stream_accepts_join_shape() -> None:
    spec = StreamConfig.model_validate(
        {
            "id": "derived.sample",
            "from": {
                "join": {" a ": " stream.a ", " b ": " stream.b "},
                "primary": "a",
                "broadcast": ["b"],
            },
            "map": {"entrypoint": "join", "args": {}},
        }
    )

    assert spec.input_refs() == {"a": "stream.a", "b": "stream.b"}
    assert spec.from_.primary == "a"
    assert spec.from_.broadcast == ["b"]
