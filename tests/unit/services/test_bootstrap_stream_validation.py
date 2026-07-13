import pytest

from datapipeline.config.catalog import (
    AlignedStreamConfig,
    DerivedStreamConfig,
    IngestConfig,
)
from datapipeline.services.bootstrap.core import (
    _build_runtime_stream,
    _load_canonical_ingests,
    _load_canonical_streams,
    load_streams,
)
from datapipeline.runtime import DerivedRuntimeStream
from datapipeline.services.streams.validation import (
    stream_feature_id_by,
    stream_partition_by,
    validate_ingest_sources,
    validate_stream_configs,
    validate_unique_stream_ids,
)


def test_validate_ingest_sources_rejects_unknown_source() -> None:
    with pytest.raises(ValueError, match="references unknown source 'source.alias'"):
        validate_ingest_sources({}, {"prices": _ingest("prices")})


def _ingest(
    stream_id: str,
    partition_by: list[str] | None = None,
    feature_id_by: list[str] | None = None,
    ordered_by: list[str] | None = None,
) -> IngestConfig:
    config = {
        "id": stream_id,
        "from": {"source": "source.alias"},
        "map": {"entrypoint": "identity", "args": {}},
        "partition_by": [] if partition_by is None else partition_by,
        "ordered_by": ordered_by,
    }
    if feature_id_by is not None:
        config["feature_id_by"] = feature_id_by
    return IngestConfig.model_validate(config)


def _stream(
    stream_id: str,
    upstream: str,
    partition_by: list[str] | None = None,
    feature_id_by: list[str] | None = None,
    ordered_by: list[str] | None = None,
) -> DerivedStreamConfig:
    config = {
        "id": stream_id,
        "from": {"stream": upstream},
        "ordered_by": ordered_by,
    }
    if partition_by is not None:
        config["partition_by"] = partition_by
    if feature_id_by is not None:
        config["feature_id_by"] = feature_id_by
    return DerivedStreamConfig.model_validate(config)


def _aligned(
    stream_id: str,
    inputs: list[str],
    ordered_by: list[str] | None = None,
) -> AlignedStreamConfig:
    return AlignedStreamConfig.model_validate(
        {
            "id": stream_id,
            "from": {"align": inputs},
            "combine": {"entrypoint": "calculate", "args": {}},
            "ordered_by": ordered_by,
        }
    )


def test_validate_stream_configs_rejects_missing_refs() -> None:
    ingests = {}
    stream_configs = {
        "derived": _aligned("derived", ["stream.a", "stream.b"]),
    }
    with pytest.raises(ValueError, match="references unknown stream"):
        validate_stream_configs(ingests, stream_configs)


def test_validate_stream_configs_rejects_aligned_partition_mismatch() -> None:
    ingests = {
        "stream.a": _ingest("stream.a", partition_by=["station"]),
        "stream.b": _ingest("stream.b", partition_by=["ticker"]),
    }
    stream_configs = {
        "derived": _aligned("derived", ["stream.a", "stream.b"]),
    }

    with pytest.raises(
        ValueError,
        match=r"partition_by \['ticker'\]; expected \['station'\]",
    ):
        validate_stream_configs(ingests, stream_configs)


def test_validate_stream_configs_accepts_aligned_matching_partitions() -> None:
    ingests = {
        "stream.a": _ingest("stream.a", partition_by=["ticker"]),
        "stream.b": _ingest("stream.b", partition_by=["ticker"]),
    }
    stream_configs = {
        "derived": _aligned("derived", ["stream.a", "stream.b"]),
    }

    validate_stream_configs(ingests, stream_configs)
    assert stream_partition_by(ingests, stream_configs, "derived") == ("ticker",)


def test_aligned_partition_inheritance_is_transitive() -> None:
    ingests = {
        "stream.a": _ingest("stream.a", partition_by=["ticker"]),
        "stream.b": _ingest("stream.b", partition_by=["ticker"]),
        "stream.c": _ingest("stream.c", partition_by=["ticker"]),
    }
    stream_configs = {
        "second": _aligned("second", ["first", "stream.c"]),
        "first": _aligned("first", ["stream.a", "stream.b"]),
    }

    validate_stream_configs(ingests, stream_configs)
    assert stream_partition_by(ingests, stream_configs, "second") == ("ticker",)


def test_single_input_identity_inheritance_is_transitive() -> None:
    ingests = {
        "prices": _ingest(
            "prices",
            partition_by=["ticker"],
            feature_id_by=["ticker"],
        )
    }
    stream_configs = {
        "daily": _stream("daily", "prices"),
        "returns": _stream("returns", "daily"),
    }

    validate_stream_configs(ingests, stream_configs)

    assert stream_partition_by(ingests, stream_configs, "returns") == ("ticker",)
    assert stream_feature_id_by(ingests, stream_configs, "returns") == ("ticker",)
    runtime_stream = _build_runtime_stream(
        stream_configs["returns"],
        ingests,
        stream_configs,
    )
    assert isinstance(runtime_stream, DerivedRuntimeStream)
    assert runtime_stream.partition_by == ("ticker",)
    assert runtime_stream.feature_id_by == ("ticker",)


def test_single_input_identity_can_be_replaced_with_empty_lists() -> None:
    ingests = {
        "prices": _ingest(
            "prices",
            partition_by=["ticker"],
            feature_id_by=["ticker"],
        )
    }
    stream_configs = {
        "cleared": _stream(
            "cleared",
            "prices",
            partition_by=[],
            feature_id_by=[],
        ),
        "downstream": _stream("downstream", "cleared"),
    }

    validate_stream_configs(ingests, stream_configs)

    assert stream_partition_by(ingests, stream_configs, "downstream") == ()
    assert stream_feature_id_by(ingests, stream_configs, "downstream") == ()
    runtime_stream = _build_runtime_stream(
        stream_configs["downstream"],
        ingests,
        stream_configs,
    )
    assert isinstance(runtime_stream, DerivedRuntimeStream)
    assert runtime_stream.partition_by == ()
    assert runtime_stream.feature_id_by == ()


def test_validate_stream_configs_rejects_noncanonical_ingest_order() -> None:
    ingests = {
        "prices": _ingest(
            "prices",
            partition_by=["security_id"],
            ordered_by=["time"],
        )
    }

    with pytest.raises(
        ValueError,
        match=r"Ingest 'prices' ordered_by must be \['security_id', 'time'\]",
    ):
        validate_stream_configs(ingests, {})


def test_validate_stream_configs_rejects_noncanonical_stream_order() -> None:
    ingests = {"prices": _ingest("prices")}
    stream_configs = {
        "returns": _stream(
            "returns",
            "prices",
            partition_by=["security_id"],
            ordered_by=["time"],
        )
    }

    with pytest.raises(
        ValueError,
        match=r"Stream 'returns' ordered_by must be \['security_id', 'time'\]",
    ):
        validate_stream_configs(ingests, stream_configs)


def test_validate_stream_configs_uses_aligned_partition_for_canonical_order() -> None:
    ingests = {
        "prices": _ingest("prices", partition_by=["security_id"]),
        "shares": _ingest("shares", partition_by=["security_id"]),
    }
    stream_configs = {
        "market_cap": _aligned(
            "market_cap",
            ["prices", "shares"],
            ordered_by=["security_id", "time"],
        )
    }

    validate_stream_configs(ingests, stream_configs)


def test_validate_stream_configs_accepts_time_order_without_partition() -> None:
    validate_stream_configs(
        {"prices": _ingest("prices", ordered_by=["time"])},
        {},
    )


def test_validate_stream_configs_rejects_dependency_cycle() -> None:
    stream_configs = {
        "first": _stream("first", "second"),
        "second": _stream("second", "first"),
    }

    with pytest.raises(ValueError, match="first -> second -> first"):
        validate_stream_configs({}, stream_configs)


def test_validate_unique_stream_ids_rejects_ingest_stream_duplicate() -> None:
    with pytest.raises(ValueError, match="Duplicate stream id"):
        validate_unique_stream_ids(
            {"same": _ingest("same")},
            {
                "same": DerivedStreamConfig.model_validate(
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
                "id: ${configured_id}",
                "from:",
                "  source: demo.source",
                "map:",
                "  entrypoint: old_mapper",
            ]
        ),
        encoding="utf-8",
    )

    loaded = _load_canonical_ingests(
        project_yaml,
        {"configured_id": " old "},
    )

    assert list(loaded) == ["old"]
    assert loaded["old"].id == "old"
    assert loaded["old"].from_.source == "demo.source"
    assert loaded["old"].map.entrypoint == "old_mapper"


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
                "id: ${configured_id}",
                "from:",
                "  stream: stream.a",
                "map:",
                "  entrypoint: old_mapper",
            ]
        ),
        encoding="utf-8",
    )

    loaded = _load_canonical_streams(
        project_yaml,
        {"configured_id": " old "},
    )

    assert list(loaded) == ["old"]
    stream = loaded["old"]
    assert isinstance(stream, DerivedStreamConfig)
    assert stream.id == "old"
    assert stream.from_.stream == "stream.a"
    assert stream.map is not None
    assert stream.map.entrypoint == "old_mapper"


def test_load_canonical_streams_reads_multiple_stream_roots(tmp_path) -> None:
    project_root = tmp_path / "project"
    common_root = tmp_path / "common"
    local_streams = project_root / "streams"
    common_streams = common_root / "streams"
    local_streams.mkdir(parents=True)
    common_streams.mkdir(parents=True)
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  ingests: ./ingests",
                "  streams:",
                "    - ./streams",
                "    - ../common/streams",
                "  sources: ./sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: ./artifacts",
            ]
        ),
        encoding="utf-8",
    )
    (local_streams / "local.yaml").write_text(
        "\n".join(
            [
                "id: local",
                "from:",
                "  stream: base.local",
            ]
        ),
        encoding="utf-8",
    )
    (common_streams / "common.yaml").write_text(
        "\n".join(
            [
                "id: common",
                "from:",
                "  stream: base.common",
            ]
        ),
        encoding="utf-8",
    )

    loaded = _load_canonical_streams(project_yaml, {})

    assert sorted(loaded) == ["common", "local"]


def test_load_canonical_streams_rejects_duplicate_ids_across_roots(tmp_path) -> None:
    project_root = tmp_path / "project"
    common_root = tmp_path / "common"
    local_streams = project_root / "streams"
    common_streams = common_root / "streams"
    local_streams.mkdir(parents=True)
    common_streams.mkdir(parents=True)
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  ingests: ./ingests",
                "  streams:",
                "    - ./streams",
                "    - ../common/streams",
                "  sources: ./sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: ./artifacts",
            ]
        ),
        encoding="utf-8",
    )
    for streams_dir, configured_id in (
        (local_streams, "same"),
        (common_streams, '" same "'),
    ):
        (streams_dir / "same.yaml").write_text(
            "\n".join(
                [
                    f"id: {configured_id}",
                    "from:",
                    "  stream: base",
                ]
            ),
            encoding="utf-8",
        )

    with pytest.raises(ValueError, match="Duplicate stream id 'same'"):
        _load_canonical_streams(project_yaml, {})


def test_load_canonical_ingests_reads_multiple_ingest_roots(tmp_path) -> None:
    project_root = tmp_path / "project"
    common_root = tmp_path / "common"
    local_ingests = project_root / "ingests"
    common_ingests = common_root / "ingests"
    local_ingests.mkdir(parents=True)
    common_ingests.mkdir(parents=True)
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  ingests:",
                "    - ./ingests",
                "    - ../common/ingests",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: ./artifacts",
            ]
        ),
        encoding="utf-8",
    )
    for ingests_dir, stream_id in (
        (local_ingests, "local"),
        (common_ingests, "common"),
    ):
        (ingests_dir / f"{stream_id}.yaml").write_text(
            "\n".join(
                [
                    f"id: {stream_id}",
                    "from:",
                    "  source: demo.source",
                    "map:",
                    "  entrypoint: old_mapper",
                ]
            ),
            encoding="utf-8",
        )

    loaded = _load_canonical_ingests(project_yaml, {})

    assert sorted(loaded) == ["common", "local"]


def test_load_streams_rejects_old_kind_shape(tmp_path) -> None:
    streams_dir = tmp_path / "stream_configs"
    streams_dir.mkdir()
    (tmp_path / "ingest_configs").mkdir()
    (tmp_path / "sources").mkdir()
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

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        load_streams(project_yaml)


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


@pytest.mark.parametrize(
    ("directory_name", "loader"),
    [
        ("ingests", _load_canonical_ingests),
        ("streams", _load_canonical_streams),
    ],
)
def test_canonical_loaders_reject_non_mapping_yaml(
    tmp_path,
    directory_name,
    loader,
) -> None:
    config_dir = tmp_path / directory_name
    config_dir.mkdir()
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
    (config_dir / "bad.yaml").write_text("- not\n- a\n- mapping\n", encoding="utf-8")

    with pytest.raises(TypeError, match="Top-level YAML .* must be a mapping"):
        loader(project_yaml, {})


@pytest.mark.parametrize(
    ("directory_name", "loader", "expected_message"),
    [
        ("ingests", _load_canonical_ingests, "Missing 'id' in ingest file"),
        ("streams", _load_canonical_streams, "Missing 'id' in stream file"),
    ],
)
def test_canonical_loaders_reject_missing_id(
    tmp_path,
    directory_name,
    loader,
    expected_message,
) -> None:
    config_dir = tmp_path / directory_name
    config_dir.mkdir()
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
    (config_dir / "bad.yaml").write_text(
        "from:\n  stream: upstream\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=expected_message):
        loader(project_yaml, {})
