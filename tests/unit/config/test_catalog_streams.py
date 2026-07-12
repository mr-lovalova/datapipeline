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


def test_stream_rejects_from_source() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        StreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
            }
        )


def test_stream_rejects_record_transforms() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
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


@pytest.mark.parametrize("config_type", [IngestConfig, StreamConfig])
def test_stream_configs_reject_execution_sort_policy(config_type) -> None:
    config = {
        "id": "sample",
        "from": {"source": "demo.source"},
        "map": {"entrypoint": "identity", "args": {}},
        "sort_batch_size": 100,
    }
    if config_type is StreamConfig:
        config["from"] = {"stream": "demo.ingest"}

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        config_type.model_validate(config)


def test_ingest_accepts_feature_id_by() -> None:
    spec = IngestConfig.model_validate(
        {
            "id": "sample",
            "from": {"source": "demo.source"},
            "map": {"entrypoint": "identity", "args": {}},
            "partition_by": "ticker",
            "feature_id_by": "ticker",
        }
    )

    assert spec.feature_id_by == "ticker"


def test_stream_accepts_upstream_stream_shape() -> None:
    spec = StreamConfig.model_validate(
        {
            "id": "sample",
            "from": {"stream": " sample.ingest "},
            "partition_by": "ticker",
            "feature_id_by": [],
            "stream": [{"dedupe": {}}],
        }
    )

    assert spec.input_streams() == ("sample.ingest",)
    assert spec.feature_id_by == []


def test_aligned_stream_accepts_ordered_stream_list() -> None:
    spec = StreamConfig.model_validate(
        {
            "id": "derived.sample",
            "from": {"align": [" stream.a ", " stream.b ", "stream.c"]},
            "map": {"entrypoint": "calculate", "args": {}},
        }
    )

    assert spec.aligns_streams
    assert spec.input_streams() == ("stream.a", "stream.b", "stream.c")


def test_aligned_stream_requires_two_inputs() -> None:
    with pytest.raises(ValueError, match="at least 2 items"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a"]},
                "map": {"entrypoint": "calculate"},
            }
        )


def test_aligned_stream_requires_mapper() -> None:
    with pytest.raises(ValueError, match="require map.entrypoint"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a", "stream.b"]},
            }
        )


def test_aligned_stream_rejects_blank_mapper_entrypoint() -> None:
    with pytest.raises(ValueError, match="at least 1 character"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a", "stream.b"]},
                "map": {"entrypoint": "   "},
            }
        )


def test_aligned_stream_inherits_partition() -> None:
    with pytest.raises(ValueError, match="inherit partition_by"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a", "stream.b"]},
                "map": {"entrypoint": "calculate"},
                "partition_by": "ticker",
            }
        )


@pytest.mark.parametrize(
    "streams",
    [
        ["stream.a", "stream.a"],
        ["stream.a@raw", "stream.b"],
        ["stream:a", "stream.b"],
        ["", "stream.b"],
    ],
)
def test_aligned_stream_rejects_invalid_stream_ids(streams) -> None:
    with pytest.raises(ValueError):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": streams},
                "map": {"entrypoint": "calculate"},
            }
        )


@pytest.mark.parametrize(
    "old_from",
    [
        {"join": {"a": "stream.a", "b": "stream.b"}, "primary": "a"},
        {"streams": {"a": "stream.a", "b": "stream.b"}},
    ],
)
def test_stream_rejects_removed_multi_stream_shapes(old_from) -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        StreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": old_from,
                "map": {"entrypoint": "calculate"},
            }
        )
