import pytest

from datapipeline.config.catalog import StreamConfig
from datapipeline.services.bootstrap.core import _load_canonical_streams
from datapipeline.services.streams.validation import validate_stream_configs


def _ingest(stream_id: str, partition_by=None) -> StreamConfig:
    return StreamConfig.model_validate(
        {
            "id": stream_id,
            "from": {"source": "source.alias"},
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
    broadcast: list[str] | None = None,
) -> StreamConfig:
    return StreamConfig.model_validate(
        {
            "id": stream_id,
            "from": {
                "join": _input_map(inputs),
                "primary": primary,
                "on": "time",
                "mode": "inner",
                "broadcast": list(broadcast or []),
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
    stream_configs = {
        "derived": _manual(
            "derived",
            ["x=stream.a", "y=stream.b"],
            driver="x",
        ),
    }
    with pytest.raises(ValueError, match="references unknown stream"):
        validate_stream_configs(stream_configs)


def test_validate_stream_configs_allows_manual_partition_mismatch() -> None:
    stream_configs = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
        "derived": _manual(
            "derived",
            ["x=stream.a", "y=stream.b"],
            driver="x",
        ),
    }

    validate_stream_configs(stream_configs)


def test_validate_stream_configs_rejects_joined_unrelated_partitions() -> None:
    stream_configs = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
        ),
    }
    with pytest.raises(ValueError, match="incompatible partition_by"):
        validate_stream_configs(stream_configs)


def test_validate_stream_configs_accepts_joined_matching_partitions() -> None:
    stream_configs = {
        "stream.a": _ingest("stream.a", partition_by="station"),
        "stream.b": _ingest("stream.b", partition_by="station"),
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
        ),
    }

    validate_stream_configs(stream_configs)


def test_validate_stream_configs_accepts_joined_broadcast_subset() -> None:
    stream_configs = {
        "stream.a": _ingest("stream.a", partition_by=["ticker", "horizon"]),
        "stream.b": _ingest("stream.b", partition_by="ticker"),
        "derived": _joined(
            "derived",
            ["x=stream.a", "y=stream.b"],
            primary="x",
            broadcast=["y"],
        ),
    }

    validate_stream_configs(stream_configs)


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
                "  streams:",
                "    a: stream.a",
                "map:",
                "  entrypoint: old_mapper",
            ]
        ),
        encoding="utf-8",
    )

    assert _load_canonical_streams(project_yaml, {}) == {
        "old": {
            "id": "old",
            "from": {"streams": {"a": "stream.a"}},
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
