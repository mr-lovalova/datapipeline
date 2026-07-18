import pytest

from datapipeline.config.sources import SourceConfig


def source_config(**overrides: object) -> SourceConfig:
    values: dict[str, object] = {
        "id": "sample.source",
        "parser": {"entrypoint": "parse"},
        "loader": {"entrypoint": "load"},
    }
    values.update(overrides)
    return SourceConfig.model_validate(values)


def test_source_input_files_have_canonical_order() -> None:
    source = source_config(inputs={"files": ["data/b.jsonl", "data/a.jsonl"]})

    assert source.inputs is not None
    assert source.inputs.files == ("data/a.jsonl", "data/b.jsonl")


def test_source_input_files_reject_duplicates() -> None:
    with pytest.raises(ValueError, match="must not contain duplicates"):
        source_config(inputs={"files": ["data/a.jsonl", "data/a.jsonl"]})


@pytest.mark.parametrize(
    "source_id",
    ["bad/id", "bad id", "bad@id", "bad:id", ".bad", "bad.", "bad..id"],
)
def test_source_rejects_noncanonical_id(source_id: str) -> None:
    with pytest.raises(ValueError, match="String should match pattern"):
        source_config(id=source_id)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cadence", "1d"),
        ("fallback", "hidden.magic"),
    ],
)
def test_source_rejects_unknown_top_level_field(field: str, value: str) -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        source_config(**{field: value})


def test_source_rejects_unknown_parser_field() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        source_config(parser={"entrypoint": "parse", "fallback": "hidden.magic"})


def test_source_rejects_unknown_loader_field() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        source_config(loader={"entrypoint": "load", "fallback": "hidden.magic"})


def test_core_io_source_rejects_unknown_loader_argument() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        source_config(
            loader={
                "entrypoint": "core.io",
                "args": {
                    "transport": "fs",
                    "format": "csv",
                    "path": "prices.csv",
                    "delimeter": ",",
                },
            }
        )


def test_core_io_source_rejects_options_for_another_format() -> None:
    with pytest.raises(ValueError, match="delimiter is only valid for the csv format"):
        source_config(
            loader={
                "entrypoint": "core.io",
                "args": {
                    "transport": "fs",
                    "format": "jsonl",
                    "path": "prices.jsonl",
                    "delimiter": ",",
                },
            }
        )


@pytest.mark.parametrize("format_", ["csv", "jsonl"])
def test_core_fs_source_accepts_gzip_compression(format_: str) -> None:
    source = source_config(
        loader={
            "entrypoint": "core.io",
            "args": {
                "transport": "fs",
                "format": format_,
                "path": "records.gz",
                "compression": "gzip",
            },
        }
    )

    assert source.loader.args.compression == "gzip"


@pytest.mark.parametrize("format_", ["json", "pickle"])
def test_core_fs_source_rejects_gzip_for_unsupported_format(format_: str) -> None:
    with pytest.raises(
        ValueError,
        match="gzip compression is supported only for csv and jsonl formats",
    ):
        source_config(
            loader={
                "entrypoint": "core.io",
                "args": {
                    "transport": "fs",
                    "format": format_,
                    "path": "records.gz",
                    "compression": "gzip",
                },
            }
        )


def test_core_http_source_rejects_compression() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        source_config(
            loader={
                "entrypoint": "core.io",
                "args": {
                    "transport": "http",
                    "format": "jsonl",
                    "url": "https://example.test/records.jsonl.gz",
                    "compression": "gzip",
                },
            }
        )


def test_custom_source_loader_keeps_plugin_arguments_explicitly_open() -> None:
    source = source_config(
        loader={
            "entrypoint": "custom.loader",
            "args": {"vendor_option": "value"},
        }
    )

    assert source.loader.args == {"vendor_option": "value"}


@pytest.mark.parametrize("value", ["10", True, float("inf")])
def test_core_http_source_rejects_invalid_timeout(value: object) -> None:
    with pytest.raises(ValueError):
        source_config(
            loader={
                "entrypoint": "core.io",
                "args": {
                    "transport": "http",
                    "format": "jsonl",
                    "url": "https://example.test/prices.jsonl",
                    "timeout_seconds": value,
                },
            }
        )
