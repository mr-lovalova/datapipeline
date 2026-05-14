import pytest

from datapipeline.config.catalog import IngestConfig, StreamConfig
from datapipeline.services.bootstrap.core import (
    _load_canonical_ingests,
    _load_canonical_streams,
    load_streams,
)
from datapipeline.services.streams.validation import (
    validate_stream_configs,
    validate_unique_stream_ids,
)


def _ingest(stream_id: str, partition_by=None) -> IngestConfig:
    return IngestConfig.model_validate(
        {
            "id": stream_id,
            "from": {"source": "source.alias"},
            "map": {"entrypoint": "identity", "args": {}},
            "partition_by": partition_by,
        }
    )


def _manual(
    stream_id: str,
    inputs: list[str],
    partition_by=None,
    driver: str | None = None,
) -> StreamConfig:
    args = {}
    if driver is not None:
        args["driver"] = driver
    return StreamConfig.model_validate(
        {
            "id": stream_id,
            "from": {"streams": _input_map(inputs)},
            "partition_by": partition_by,
            "map": {"entrypoint": "manual", "args": args},
        }
    )


def _joined(
    stream_id: str,
    inputs: list[str],
    primary: str,
    on: str | list[str] = "time",
) -> StreamConfig:
    return StreamConfig.model_validate(
        {
            "id": stream_id,
            "from": {
                "join": _input_map(inputs),
                "primary": primary,
                "on": on,
                "mode": "inner",
            },
            "map": {"entrypoint": "join", "args": {}},
        }
    )


def _input_map(inputs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in inputs:
        alias, ref = item.split("=", 1)
        out[alias.strip()] = ref.strip()
    return out


def test_validate_stream_configs_rejects_missing_refs() -> None:
    ingests = {}
    stream_configs = {
        "derived": _manual(
            "derived",
            ["x=stream.a", "y=stream.b"],
            driver="x",
        ),
    }
    with pytest.raises(ValueError, match="references unknown stream"):
        validate_stream_configs(ingests, stream_configs)


def test_validate_stream_configs_allows_manual_partition_mismatch() -> None:
    ingests = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
    }
    stream_configs = {
        "derived": _manual(
            "derived",
            ["x=stream.a", "y=stream.b"],
            driver="x",
        ),
    }

    validate_stream_configs(ingests, stream_configs)


def test_validate_stream_configs_allows_joined_unrelated_partitions() -> None:
    ingests = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
    }
    stream_configs = {
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
        ),
    }
    validate_stream_configs(ingests, stream_configs)


def test_validate_stream_configs_accepts_joined_matching_partitions() -> None:
    ingests = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="station"),
    }
    stream_configs = {
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
        ),
    }

    validate_stream_configs(ingests, stream_configs)


def test_validate_stream_configs_accepts_joined_subset_key() -> None:
    ingests = {
        "stream.a": _ingest("stream.a", partition_by=["ticker", "horizon"]),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
    }
    stream_configs = {
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
            on=["ticker", "time"],
        ),
    }

    validate_stream_configs(ingests, stream_configs)


def test_validate_unique_stream_ids_rejects_ingest_stream_duplicate() -> None:
    with pytest.raises(ValueError, match="Duplicate stream id"):
        validate_unique_stream_ids(
            {"same": _ingest("same")},
            {
                "same": StreamConfig.model_validate(
                    {"id": "same", "from": {"stream": "upstream"}}
                )
            },
        )


def test_load_canonical_ingests_loads_source_shape(tmp_path) -> None:
    ingests_dir = tmp_path / "ingest_configs"
    ingests_dir.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  ingests: ./ingest_configs",
                "  streams: ./stream_configs",
                "  sources: ./sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: ./artifacts",
            ]
        ),
        encoding="utf-8",
    )
    (ingests_dir / "old.yaml").write_text(
        "\n".join(
            [
                "id: old",
                "from:",
                "  source: demo.source",
                "map:",
                "  entrypoint: old_mapper",
            ]
        ),
        encoding="utf-8",
    )

    assert _load_canonical_ingests(project_yaml, {}) == {
        "old": {
            "id": "old",
            "from": {"source": "demo.source"},
            "map": {"entrypoint": "old_mapper"},
        }
    }


def test_load_canonical_streams_loads_from_shape(tmp_path) -> None:
    streams_dir = tmp_path / "stream_configs"
    streams_dir.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  ingests: ./ingest_configs",
                "  streams: ./stream_configs",
                "  sources: ./sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: ./artifacts",
            ]
        ),
        encoding="utf-8",
    )
    (streams_dir / "old.yaml").write_text(
        "\n".join(
            [
                "id: old",
                "from:",
                "  stream: stream.a",
                "map:",
                "  entrypoint: old_mapper",
            ]
        ),
        encoding="utf-8",
    )

    assert _load_canonical_streams(project_yaml, {}) == {
            "old": {
                "id": "old",
                "from": {"stream": "stream.a"},
                "map": {"entrypoint": "old_mapper"},
            }
    }


def test_load_canonical_streams_rejects_old_kind_shape(tmp_path) -> None:
    streams_dir = tmp_path / "stream_configs"
    streams_dir.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  ingests: ./ingest_configs",
                "  streams: ./stream_configs",
                "  sources: ./sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: ./artifacts",
            ]
        ),
        encoding="utf-8",
    )
    (streams_dir / "old.yaml").write_text(
        "\n".join(
            [
                "kind: ingest",
                "id: old",
                "source: demo.source",
                "mapper:",
                "  entrypoint: map_old",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="'kind' is no longer supported"):
        _load_canonical_streams(project_yaml, {})


def test_load_streams_rejects_malformed_map(tmp_path) -> None:
    ingests_dir = tmp_path / "ingests"
    streams_dir = tmp_path / "streams"
    sources_dir = tmp_path / "sources"
    ingests_dir.mkdir()
    streams_dir.mkdir()
    sources_dir.mkdir()
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  ingests: ./ingests",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: ./artifacts",
            ]
        ),
        encoding="utf-8",
    )
    (ingests_dir / "bad.yaml").write_text(
        "\n".join(
            [
                "id: bad",
                "from:",
                "  source: demo.source",
                "map: identity",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        load_streams(project_yaml)
