import pytest

from datapipeline.config.catalog import (
    AlignedStreamConfig,
    DerivedStreamConfig,
    IngestConfig,
    SourceConfig,
    StreamsConfig,
)


def _source_with_inputs(files: list[str]) -> SourceConfig:
    return SourceConfig.model_validate(
        {
            "id": "sample.source",
            "parser": {"entrypoint": "identity"},
            "loader": {"entrypoint": "custom.loader"},
            "inputs": {"files": files},
        }
    )


def test_source_input_files_have_canonical_order() -> None:
    source = _source_with_inputs(["data/b.jsonl", "data/a.jsonl"])

    assert source.inputs is not None
    assert source.inputs.files == ("data/a.jsonl", "data/b.jsonl")


def test_source_input_files_reject_duplicates() -> None:
    with pytest.raises(ValueError, match="must not contain duplicates"):
        _source_with_inputs(["data/a.jsonl", "data/a.jsonl"])


def test_stream_rejects_old_kind_shape() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        DerivedStreamConfig.model_validate(
            {
                "kind": "ingest",
                "id": "sample",
                "source": "demo.source",
                "mapper": {"entrypoint": "map", "args": {}},
            }
        )


def test_stream_rejects_from_source() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        DerivedStreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
            }
        )


def test_stream_rejects_record_transforms() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        DerivedStreamConfig.model_validate(
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


@pytest.mark.parametrize("config_type", [IngestConfig, DerivedStreamConfig])
def test_stream_configs_reject_execution_sort_policy(config_type) -> None:
    config = {
        "id": "sample",
        "from": {"source": "demo.source"},
        "map": {"entrypoint": "identity", "args": {}},
        "sort_batch_size": 100,
    }
    if config_type is DerivedStreamConfig:
        config["from"] = {"stream": "demo.ingest"}

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        config_type.model_validate(config)


def test_ingest_accepts_feature_id_by() -> None:
    spec = IngestConfig.model_validate(
        {
            "id": "sample",
            "from": {"source": "demo.source"},
            "map": {"entrypoint": "identity", "args": {}},
            "partition_by": ["ticker"],
            "feature_id_by": ["ticker"],
            "ordered_by": ["ticker", "time"],
        }
    )

    assert spec.partition_by == ("ticker",)
    assert spec.feature_id_by == ("ticker",)
    assert spec.ordered_by == ("ticker", "time")


@pytest.mark.parametrize("feature_id_by", [["ticker", "ticker"], [""]])
def test_ingest_rejects_invalid_feature_id_fields(feature_id_by) -> None:
    with pytest.raises(ValueError, match="duplicate|at least 1 character"):
        IngestConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
                "map": {"entrypoint": "identity", "args": {}},
                "feature_id_by": feature_id_by,
            }
        )


def test_ingest_rejects_scalar_identity_fields() -> None:
    with pytest.raises(ValueError, match="partition_by|tuple"):
        IngestConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
                "map": {"entrypoint": "identity"},
                "partition_by": "ticker",
            }
        )

    with pytest.raises(ValueError, match="feature_id_by|tuple"):
        IngestConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
                "map": {"entrypoint": "identity"},
                "feature_id_by": "ticker",
            }
        )


@pytest.mark.parametrize(
    "partition_by",
    [[""], ["ticker", "ticker"], ["time"]],
)
def test_ingest_rejects_invalid_partition_fields(partition_by) -> None:
    with pytest.raises(ValueError, match="partition_by|at least 1 character"):
        IngestConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
                "map": {"entrypoint": "identity"},
                "partition_by": partition_by,
            }
        )


@pytest.mark.parametrize(
    "partition_by",
    [[""], ["ticker", "ticker"], ["time"]],
)
def test_derived_stream_rejects_invalid_partition_fields(partition_by) -> None:
    with pytest.raises(ValueError, match="partition_by|at least 1 character"):
        DerivedStreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"stream": "demo.ingest"},
                "partition_by": partition_by,
            }
        )


def test_stream_rejects_duplicate_feature_id_fields() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        DerivedStreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"stream": "demo.ingest"},
                "feature_id_by": ["ticker", "ticker"],
            }
        )


def test_stream_accepts_upstream_stream_shape() -> None:
    spec = DerivedStreamConfig.model_validate(
        {
            "id": "sample",
            "from": {"stream": " sample.ingest "},
            "partition_by": ["ticker"],
            "feature_id_by": [],
            "stream": [{"operation": "dedupe"}],
        }
    )

    assert spec.input_streams() == ("sample.ingest",)
    assert spec.partition_by == ("ticker",)
    assert spec.feature_id_by == ()


def test_derived_identity_distinguishes_inheritance_from_explicit_empty() -> None:
    missing = DerivedStreamConfig.model_validate(
        {"id": "missing", "from": {"stream": "source"}}
    )
    empty = DerivedStreamConfig.model_validate(
        {
            "id": "empty",
            "from": {"stream": "source"},
            "partition_by": [],
            "feature_id_by": [],
        }
    )

    assert missing.partition_by is None
    assert missing.feature_id_by is None
    assert empty.partition_by == ()
    assert empty.feature_id_by == ()


@pytest.mark.parametrize("field", ["partition_by", "feature_id_by"])
def test_derived_identity_rejects_null_override(field: str) -> None:
    with pytest.raises(ValueError, match=rf"{field} must be a list.*omit it"):
        DerivedStreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"stream": "source"},
                field: None,
            }
        )


def test_aligned_stream_accepts_ordered_stream_list() -> None:
    spec = AlignedStreamConfig.model_validate(
        {
            "id": "derived.sample",
            "from": {"align": [" stream.a ", " stream.b ", "stream.c"]},
            "combine": {"entrypoint": "calculate", "args": {}},
        }
    )

    assert spec.input_streams() == ("stream.a", "stream.b", "stream.c")


def test_stream_catalog_selects_concrete_stream_types() -> None:
    catalog = StreamsConfig.model_validate(
        {
            "streams": {
                "derived": {
                    "id": "derived",
                    "from": {"stream": "source"},
                },
                "aligned": {
                    "id": "aligned",
                    "from": {"align": ["source", "derived"]},
                    "combine": {"entrypoint": "calculate"},
                },
            }
        }
    )

    assert isinstance(catalog.streams["derived"], DerivedStreamConfig)
    assert isinstance(catalog.streams["aligned"], AlignedStreamConfig)


def test_stream_catalog_rejects_legacy_raw_source_key() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        StreamsConfig.model_validate({"raw": {}})


def test_aligned_stream_requires_two_inputs() -> None:
    with pytest.raises(ValueError, match="at least 2 items"):
        AlignedStreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a"]},
                "combine": {"entrypoint": "calculate"},
            }
        )


def test_aligned_stream_requires_combiner() -> None:
    with pytest.raises(ValueError, match="combine"):
        AlignedStreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a", "stream.b"]},
            }
        )


def test_aligned_stream_rejects_blank_combiner_entrypoint() -> None:
    with pytest.raises(ValueError, match="at least 1 character"):
        AlignedStreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a", "stream.b"]},
                "combine": {"entrypoint": "   "},
            }
        )


def test_aligned_stream_rejects_partition_override() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        AlignedStreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a", "stream.b"]},
                "combine": {"entrypoint": "calculate"},
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
        AlignedStreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": streams},
                "combine": {"entrypoint": "calculate"},
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
        AlignedStreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": old_from,
                "combine": {"entrypoint": "calculate"},
            }
        )


def test_aligned_stream_rejects_iterator_map() -> None:
    with pytest.raises(ValueError, match="combine|Extra inputs are not permitted"):
        AlignedStreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"align": ["stream.a", "stream.b"]},
                "map": {"entrypoint": "calculate"},
            }
        )


def test_derived_stream_rejects_combiner() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        DerivedStreamConfig.model_validate(
            {
                "id": "derived.sample",
                "from": {"stream": "stream.a"},
                "combine": {"entrypoint": "calculate"},
            }
        )


def test_ingest_rejects_top_level_cadence() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        IngestConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
                "map": {"entrypoint": "identity"},
                "cadence": "1d",
            }
        )


def test_derived_stream_rejects_top_level_cadence() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        DerivedStreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"stream": "demo.ingest"},
                "cadence": "1d",
            }
        )


def test_source_rejects_unknown_top_level_field() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SourceConfig.model_validate(
            {
                "id": "demo.source",
                "parser": {"entrypoint": "parse"},
                "loader": {"entrypoint": "load"},
                "cadence": "1d",
            }
        )


def test_source_rejects_unknown_parser_field() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SourceConfig.model_validate(
            {
                "id": "demo.source",
                "parser": {
                    "entrypoint": "parse",
                    "fallback": "hidden.magic",
                },
                "loader": {"entrypoint": "load"},
            }
        )


def test_source_rejects_unknown_loader_field() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SourceConfig.model_validate(
            {
                "id": "demo.source",
                "parser": {"entrypoint": "parse"},
                "loader": {
                    "entrypoint": "load",
                    "fallback": "hidden.magic",
                },
            }
        )


def test_core_io_source_rejects_unknown_loader_argument() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        SourceConfig.model_validate(
            {
                "id": "demo.source",
                "parser": {"entrypoint": "parse"},
                "loader": {
                    "entrypoint": "core.io",
                    "args": {
                        "transport": "fs",
                        "format": "csv",
                        "path": "prices.csv",
                        "delimeter": ",",
                    },
                },
            }
        )


def test_core_io_source_rejects_options_for_another_format() -> None:
    with pytest.raises(ValueError, match="delimiter is only valid for the csv format"):
        SourceConfig.model_validate(
            {
                "id": "demo.source",
                "parser": {"entrypoint": "parse"},
                "loader": {
                    "entrypoint": "core.io",
                    "args": {
                        "transport": "fs",
                        "format": "jsonl",
                        "path": "prices.jsonl",
                        "delimiter": ",",
                    },
                },
            }
        )


def test_custom_source_loader_keeps_plugin_arguments_explicitly_open() -> None:
    source = SourceConfig.model_validate(
        {
            "id": "demo.source",
            "parser": {"entrypoint": "parse"},
            "loader": {
                "entrypoint": "custom.loader",
                "args": {"vendor_option": "value"},
            },
        }
    )

    assert source.loader.args == {"vendor_option": "value"}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("count_by_fetch", "false"),
        ("timeout_seconds", "10"),
        ("timeout_seconds", True),
        ("timeout_seconds", float("inf")),
    ],
)
def test_core_http_source_rejects_coerced_runtime_options(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        SourceConfig.model_validate(
            {
                "id": "demo.source",
                "parser": {"entrypoint": "parse"},
                "loader": {
                    "entrypoint": "core.io",
                    "args": {
                        "transport": "http",
                        "format": "jsonl",
                        "url": "https://example.test/prices.jsonl",
                        field: value,
                    },
                },
            }
        )


def test_ingest_rejects_unknown_map_field() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        IngestConfig.model_validate(
            {
                "id": "sample",
                "from": {"source": "demo.source"},
                "map": {"entrypoint": "identity", "mode": "implicit"},
            }
        )


def test_aligned_stream_rejects_unknown_combine_field() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        AlignedStreamConfig.model_validate(
            {
                "id": "sample",
                "from": {"align": ["stream.a", "stream.b"]},
                "combine": {"entrypoint": "calculate", "mode": "implicit"},
            }
        )


def test_ingest_rejects_internal_from_field_name() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        IngestConfig.model_validate(
            {
                "id": "sample",
                "from_": {"source": "demo.source"},
                "map": {"entrypoint": "identity"},
            }
        )


def test_derived_stream_rejects_internal_from_field_name() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        DerivedStreamConfig.model_validate(
            {
                "id": "sample",
                "from_": {"stream": "demo.ingest"},
            }
        )


def test_aligned_stream_rejects_internal_from_field_name() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        AlignedStreamConfig.model_validate(
            {
                "id": "sample",
                "from_": {"align": ["stream.a", "stream.b"]},
                "combine": {"entrypoint": "calculate"},
            }
        )


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


def test_stream_catalog_rejects_ingest_registry_key_mismatch() -> None:
    with pytest.raises(ValueError, match="Ingest registry key 'alias'.*'demo.ingest'"):
        StreamsConfig.model_validate(
            {
                "ingests": {
                    "alias": {
                        "id": "demo.ingest",
                        "from": {"source": "demo.source"},
                        "map": {"entrypoint": "identity"},
                    }
                }
            }
        )


def test_stream_catalog_rejects_stream_registry_key_mismatch() -> None:
    with pytest.raises(ValueError, match="Stream registry key 'alias'.*'demo.stream'"):
        StreamsConfig.model_validate(
            {
                "streams": {
                    "alias": {
                        "id": "demo.stream",
                        "from": {"stream": "demo.ingest"},
                    }
                }
            }
        )
