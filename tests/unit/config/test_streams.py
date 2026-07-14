import pytest

from datapipeline.config.streams import (
    AlignedStreamConfig,
    DerivedStreamConfig,
    SourceStreamConfig,
    StreamsConfig,
)


def test_source_stream_has_explicit_pipeline_phases() -> None:
    stream = SourceStreamConfig.model_validate(
        {
            "id": "prices",
            "from": {"source": "vendor.prices"},
            "map": {"entrypoint": "map_price"},
            "preprocess": [
                {
                    "operation": "where",
                    "field": "time",
                    "operator": "ge",
                    "comparand": "2024-01-01T00:00:00Z",
                }
            ],
            "partition_by": ["ticker"],
            "ordered_by": ["ticker", "time"],
            "transforms": [
                {
                    "operation": "rolling",
                    "field": "close",
                    "window": 20,
                }
            ],
        }
    )

    assert stream.from_.source == "vendor.prices"
    assert stream.partition_by == ("ticker",)
    assert stream.ordered_by == ("ticker", "time")
    assert len(stream.preprocess) == 1
    assert len(stream.transforms) == 1


def test_source_stream_requires_mapper() -> None:
    with pytest.raises(ValueError, match="map"):
        SourceStreamConfig.model_validate(
            {"id": "prices", "from": {"source": "vendor.prices"}}
        )


@pytest.mark.parametrize("field", ["record", "stream"])
def test_source_stream_rejects_removed_transform_names(field: str) -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SourceStreamConfig.model_validate(
            {
                "id": "prices",
                "from": {"source": "vendor.prices"},
                "map": {"entrypoint": "map_price"},
                field: [],
            }
        )


def test_derived_stream_inherits_its_input_contract() -> None:
    stream = DerivedStreamConfig.model_validate(
        {
            "id": "returns",
            "from": {"stream": " prices "},
            "transforms": [{"operation": "lag", "field": "close", "periods": 1}],
        }
    )

    assert stream.input_streams() == ("prices",)
    assert len(stream.transforms) == 1


def test_derived_stream_requires_a_transform() -> None:
    with pytest.raises(ValueError, match="at least 1 item"):
        DerivedStreamConfig.model_validate(
            {"id": "returns", "from": {"stream": "prices"}, "transforms": []}
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("map", {"entrypoint": "identity"}),
        ("preprocess", []),
        ("partition_by", ["ticker"]),
        ("ordered_by", ["ticker", "time"]),
        ("record", []),
        ("stream", []),
    ],
)
def test_derived_stream_rejects_source_and_legacy_fields(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        DerivedStreamConfig.model_validate(
            {"id": "returns", "from": {"stream": "prices"}, field: value}
        )


def test_aligned_stream_has_combiner_and_transforms() -> None:
    stream = AlignedStreamConfig.model_validate(
        {
            "id": "market_cap",
            "from": {"align": [" prices ", " shares "]},
            "combine": {"entrypoint": "market_cap"},
            "transforms": [{"operation": "dedupe"}],
        }
    )

    assert stream.input_streams() == ("prices", "shares")
    assert len(stream.transforms) == 1


def test_aligned_stream_requires_two_inputs() -> None:
    with pytest.raises(ValueError, match="at least 2 items"):
        AlignedStreamConfig.model_validate(
            {
                "id": "market_cap",
                "from": {"align": ["prices"]},
                "combine": {"entrypoint": "market_cap"},
            }
        )


def test_aligned_stream_rejects_duplicate_inputs() -> None:
    with pytest.raises(ValueError, match="must not contain duplicate"):
        AlignedStreamConfig.model_validate(
            {
                "id": "market_cap",
                "from": {"align": ["prices", "prices"]},
                "combine": {"entrypoint": "market_cap"},
            }
        )


@pytest.mark.parametrize("field", ["map", "partition_by", "ordered_by"])
def test_aligned_stream_rejects_other_stream_contracts(field: str) -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        AlignedStreamConfig.model_validate(
            {
                "id": "market_cap",
                "from": {"align": ["prices", "shares"]},
                "combine": {"entrypoint": "market_cap"},
                field: {"entrypoint": "identity"} if field == "map" else [],
            }
        )


@pytest.mark.parametrize(
    "partition_by", ["ticker", [""], ["ticker", "ticker"], ["time"]]
)
def test_source_stream_rejects_invalid_partition_fields(partition_by: object) -> None:
    with pytest.raises(ValueError, match="partition_by|tuple|at least 1 character"):
        SourceStreamConfig.model_validate(
            {
                "id": "prices",
                "from": {"source": "vendor.prices"},
                "map": {"entrypoint": "map_price"},
                "partition_by": partition_by,
            }
        )


def test_stream_catalog_selects_all_concrete_stream_types() -> None:
    catalog = StreamsConfig.model_validate(
        {
            "streams": {
                "prices": {
                    "id": "prices",
                    "from": {"source": "vendor.prices"},
                    "map": {"entrypoint": "map_price"},
                },
                "returns": {
                    "id": "returns",
                    "from": {"stream": "prices"},
                    "transforms": [{"operation": "dedupe"}],
                },
                "market_cap": {
                    "id": "market_cap",
                    "from": {"align": ["prices", "returns"]},
                    "combine": {"entrypoint": "market_cap"},
                },
            }
        }
    )

    assert isinstance(catalog.streams["prices"], SourceStreamConfig)
    assert isinstance(catalog.streams["returns"], DerivedStreamConfig)
    assert isinstance(catalog.streams["market_cap"], AlignedStreamConfig)


def test_stream_catalog_rejects_removed_ingest_registry() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        StreamsConfig.model_validate({"ingests": {}})


def test_stream_catalog_rejects_source_registry_key_mismatch() -> None:
    with pytest.raises(ValueError, match="Source registry key 'alias'.*'demo.source'"):
        StreamsConfig.model_validate(
            {
                "sources": {
                    "alias": {
                        "id": "demo.source",
                        "parser": {"entrypoint": "parse"},
                        "loader": {"entrypoint": "load"},
                    }
                }
            }
        )


def test_stream_catalog_rejects_stream_registry_key_mismatch() -> None:
    with pytest.raises(ValueError, match="Stream registry key 'alias'.*'prices'"):
        StreamsConfig.model_validate(
            {
                "streams": {
                    "alias": {
                        "id": "prices",
                        "from": {"source": "vendor.prices"},
                        "map": {"entrypoint": "map_price"},
                    }
                }
            }
        )
