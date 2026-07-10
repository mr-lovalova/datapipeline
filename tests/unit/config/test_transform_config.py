import pytest
from pydantic import ValidationError

from datapipeline.config.catalog import IngestConfig, StreamConfig
from datapipeline.config.postprocess import PostprocessConfig
from datapipeline.transforms.spec import TransformSpec


def _ingest_config(**values: object) -> IngestConfig:
    return IngestConfig.model_validate(
        {
            "id": "prices.raw",
            "from": {"source": "prices"},
            "map": {"entrypoint": "identity", "args": {}},
            **values,
        }
    )


def _stream_config(**values: object) -> StreamConfig:
    return StreamConfig.model_validate(
        {
            "id": "prices.daily",
            "from": {"stream": "prices.raw"},
            **values,
        }
    )


def test_catalog_parses_each_transform_list_into_specs() -> None:
    ingest = _ingest_config(record=[{"shift_time": {"by": "1d"}}])
    stream = _stream_config(
        stream=[{"dedupe": None}],
        debug=[{"lint": {"strict": True}}],
    )

    assert ingest.record == [TransformSpec(name="shift_time", params={"by": "1d"})]
    assert stream.stream == [TransformSpec(name="dedupe", params={})]
    assert stream.debug == [TransformSpec(name="lint", params={"strict": True})]


def test_catalog_serializes_transform_specs_as_config_clauses() -> None:
    stream = _stream_config(
        stream=[{"dedupe": None}, {"rolling": {"field": "price", "window": 2}}]
    )

    assert stream.model_dump()["stream"] == [
        {"dedupe": {}},
        {"rolling": {"field": "price", "window": 2}},
    ]


def test_catalog_preserves_omitted_and_explicit_empty_transform_lists() -> None:
    assert _ingest_config().record is None
    assert _ingest_config(record=[]).record == []
    assert _stream_config().stream is None
    assert _stream_config(stream=[]).stream == []
    assert _stream_config().debug is None
    assert _stream_config(debug=[]).debug == []


@pytest.mark.parametrize(
    "field, clause",
    [
        ("record", {"lag": [1]}),
        ("stream", {"lag": "price"}),
        ("debug", {"lint": {1: True}}),
    ],
)
def test_catalog_rejects_invalid_transform_clauses(
    field: str,
    clause: object,
) -> None:
    validate = _ingest_config if field == "record" else _stream_config

    with pytest.raises(ValidationError):
        validate(**{field: [clause]})


def test_postprocess_parses_transform_specs() -> None:
    config = PostprocessConfig.model_validate(
        [{"forward_fill": None}, {"replace": {"value": 0}}]
    )

    assert config.root == [
        TransformSpec(name="forward_fill", params={}),
        TransformSpec(name="replace", params={"value": 0}),
    ]
    assert config.model_dump() == [
        {"forward_fill": {}},
        {"replace": {"value": 0}},
    ]


def test_postprocess_rejects_invalid_transform_clause() -> None:
    with pytest.raises(ValidationError):
        PostprocessConfig.model_validate([{"replace": [0]}])
