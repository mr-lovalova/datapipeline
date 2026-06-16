from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from datapipeline.cache.record_streams import RecordStreamCache, cached_record_stream
from datapipeline.cache.signatures import resolve_record_stream_cache_ref
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.record import TemporalRecord
from datapipeline.runtime import Runtime, StreamRuntimeSpec
from datapipeline.services.bootstrap.config import artifacts_root


@dataclass
class _Rec(TemporalRecord):
    symbol: str
    value: float


def _ts(day: int) -> datetime:
    return datetime(2021, 1, day, tzinfo=timezone.utc)


def _write_project(tmp_path: Path, *, partition_by: str | None = None) -> Path:
    project_root = tmp_path / "project"
    (project_root / "sources").mkdir(parents=True)
    (project_root / "ingests").mkdir(parents=True)
    (project_root / "streams").mkdir(parents=True)
    (project_root / "tasks").mkdir(parents=True)
    (project_root / "profiles").mkdir(parents=True)

    (project_root / "dataset.yaml").write_text("group_by: 1d\nfeatures: []\n", encoding="utf-8")
    (project_root / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    (project_root / "project.yaml").write_text(
        "\n".join(
            [
                "version: 1",
                "name: demo",
                "paths:",
                "  ingests: ./ingests",
                "  streams: ./streams",
                "  sources: ./sources",
                "  dataset: ./dataset.yaml",
                "  postprocess: ./postprocess.yaml",
                "  artifacts: ./artifacts",
                "  tasks: ./tasks",
                "  profiles: ./profiles",
                "globals: {}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (project_root / "sources" / "raw.aapl.yaml").write_text(
        "\n".join(
            [
                "id: raw.aapl",
                "parser:",
                "  entrypoint: identity",
                "loader:",
                "  entrypoint: fs",
                "  args:",
                "    transport: fs",
                "    format: pickle",
                "    path: ./data/raw.pkl",
                "",
            ]
        ),
        encoding="utf-8",
    )
    ingest_lines = [
        "id: prices.aapl",
        "from:",
        "  source: raw.aapl",
        "map:",
        "  entrypoint: identity",
        "record: []",
    ]
    if partition_by is not None:
        ingest_lines.append(f"partition_by: {partition_by}")
    ingest_lines.append("")
    (project_root / "ingests" / "prices.aapl.yaml").write_text(
        "\n".join(ingest_lines),
        encoding="utf-8",
    )
    return project_root / "project.yaml"


def _write_derived_stream(project_yaml: Path) -> Path:
    stream_path = project_yaml.parent / "streams" / "prices.liquid.yaml"
    stream_path.write_text(
        "\n".join(
            [
                "id: prices.liquid",
                "from:",
                "  stream: prices.aapl",
                "stream: []",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return stream_path


def test_record_stream_cache_materializes_and_replays(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    records = [
        _Rec(time=_ts(1), symbol="AAPL", value=1.0),
        _Rec(time=_ts(2), symbol="AAPL", value=2.0),
    ]

    materialized = list(cache.materialize("prices.aapl", iter(records)))
    replayed = list(cache.load("prices.aapl") or ())

    assert materialized == records
    assert replayed == records


def test_record_stream_cache_signature_includes_sample_keys(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    before = resolve_record_stream_cache_ref(project_yaml, "prices.aapl")

    (project_yaml.parent / "dataset.yaml").write_text(
        "\n".join(
            [
                "sample:",
                "  cadence: 1d",
                "  keys: [symbol]",
                "features: []",
                "",
            ]
        ),
        encoding="utf-8",
    )
    after = resolve_record_stream_cache_ref(project_yaml, "prices.aapl")

    assert before is not None
    assert after is not None
    assert before[2] != after[2]


def test_record_stream_cache_reraises_materialization_failure(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    records = [
        _Rec(time=_ts(1), symbol="AAPL", value=1.0),
        _Rec(time=_ts(2), symbol="AAPL", value=2.0),
    ]

    def _broken_records():
        yield records[0]
        raise FileNotFoundError("missing source file")

    with pytest.raises(FileNotFoundError, match="missing source file"):
        list(cache.materialize("prices.aapl", _broken_records()))

    ref = cache._materialization_ref("prices.aapl")
    assert ref is not None
    assert not cache._store._data_path(ref).exists()
    assert not cache._store._manifest_path(ref).exists()
    assert cache.load("prices.aapl") is None


def test_record_stream_cache_invalidates_when_stream_changes(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    list(cache.materialize("prices.aapl", iter([_Rec(time=_ts(1), symbol="AAPL", value=1.0)])))

    ingest_path = project_yaml.parent / "ingests" / "prices.aapl.yaml"
    ingest_path.write_text(
        ingest_path.read_text(encoding="utf-8") + "partition_by: symbol\n",
        encoding="utf-8",
    )

    invalidated = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    assert invalidated.load("prices.aapl") is None


def test_stream_cache_invalidates_when_stream_config_changes(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    stream_path = _write_derived_stream(project_yaml)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    list(cache.materialize("prices.liquid", iter([_Rec(time=_ts(1), symbol="AAPL", value=1.0)])))

    stream_path.write_text(
        stream_path.read_text(encoding="utf-8") + "partition_by: symbol\n",
        encoding="utf-8",
    )

    invalidated = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    assert invalidated.load("prices.liquid") is None


def test_stream_cache_invalidates_when_upstream_ingest_changes(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    _write_derived_stream(project_yaml)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    list(cache.materialize("prices.liquid", iter([_Rec(time=_ts(1), symbol="AAPL", value=1.0)])))

    ingest_path = project_yaml.parent / "ingests" / "prices.aapl.yaml"
    ingest_path.write_text(
        ingest_path.read_text(encoding="utf-8") + "partition_by: symbol\n",
        encoding="utf-8",
    )

    invalidated = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    assert invalidated.load("prices.liquid") is None


def test_record_stream_cache_treats_malformed_manifest_as_cache_miss(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    list(cache.materialize("prices.aapl", iter([_Rec(time=_ts(1), symbol="AAPL", value=1.0)])))

    ref = cache._materialization_ref("prices.aapl")
    assert ref is not None
    manifest_path = cache._store._manifest_path(ref)
    manifest_path.write_text('{"signature_hash": ', encoding="utf-8")

    assert cache.load("prices.aapl") is None


def test_cached_record_stream_reuses_cache_without_rebuilding(tmp_path: Path, monkeypatch) -> None:
    project_yaml = _write_project(tmp_path)
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
    )
    runtime.registries.partition_by.register("prices.aapl", None)
    runtime.registries.debug_operations.register("prices.aapl", [])
    runtime.registries.stream_specs.register(
        "prices.aapl",
        StreamRuntimeSpec(pipeline="ingest"),
    )
    context = PipelineContext(runtime)
    records = [
        _Rec(time=_ts(1), symbol="AAPL", value=1.0),
        _Rec(time=_ts(2), symbol="AAPL", value=2.0),
    ]
    calls = {"count": 0}

    def _build(*args, **kwargs):
        calls["count"] += 1
        return iter(records)

    monkeypatch.setattr("datapipeline.pipelines.ingest.build_ingest_pipeline", _build)

    first = list(cached_record_stream(context, "prices.aapl"))
    second = list(cached_record_stream(context, "prices.aapl"))

    assert first == records
    assert second == records
    assert calls["count"] == 1


def test_record_stream_cache_load_observes_source_visuals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_yaml = _write_project(tmp_path)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    records = [_Rec(time=_ts(1), symbol="AAPL", value=1.0)]
    list(cache.materialize("prices.aapl", iter(records)))

    captured: dict[str, str] = {}

    def _observe(source, stream_id):
        captured["stream_id"] = stream_id
        return source

    monkeypatch.setattr("datapipeline.cli.visuals.streams.observe_source", _observe)

    replayed = list(cache.load("prices.aapl") or ())

    assert replayed == records
    assert captured == {"stream_id": "prices.aapl"}


def test_record_stream_cache_loader_exposes_cache_progress_label(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    records = [_Rec(time=_ts(1), symbol="AAPL", value=1.0)]
    list(cache.materialize("prices.aapl", iter(records)))

    ref = cache._materialization_ref("prices.aapl")
    assert ref is not None
    source = cache._store.load(ref)

    assert source is not None
    assert source.loader.progress_label() == "Loading cache"
    assert source.loader.info_lines() == ["Loaded stream output from cache"]
    assert source.loader.debug_lines() == [
        "cache.file: ingest_stream/prices.aapl/ordered_records/data.pkl"
    ]
    assert source.loader.include_transport_info() is False
    assert source.loader.include_transport_debug() is False


def test_runtime_uses_owned_temp_cache_root_and_cleans_it(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
    )

    cache_root = runtime.cache_root

    assert cache_root is not None
    assert cache_root.exists()
    assert runtime._owns_cache_root is True

    runtime.cleanup_cache()

    assert not cache_root.exists()


def test_runtime_disable_cache_removes_owned_temp_cache_root(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
    )
    cache_root = runtime.cache_root

    runtime.disable_cache()

    assert runtime.cache_enabled is False
    assert cache_root is not None
    assert not cache_root.exists()
    assert runtime.cache_root is None


def test_runtime_sort_spill_dir_recreates_owned_temp_root_after_cache_disabled(
    tmp_path: Path,
) -> None:
    project_yaml = _write_project(tmp_path)
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
    )
    first_root = runtime.cache_root
    runtime.disable_cache()

    sort_dir = runtime.sort_spill_dir()

    assert first_root is not None
    assert not first_root.exists()
    assert runtime.cache_root is not None
    assert runtime.cache_root.exists()
    assert sort_dir == runtime.cache_root / "_sort"


def test_runtime_preserves_explicit_cache_root(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    cache_root = tmp_path / "cache"
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
        cache_root=cache_root,
    )

    assert runtime.cache_root == cache_root.resolve()
    assert runtime._owns_cache_root is False

    runtime.cleanup_cache()

    assert cache_root.exists()


def test_cached_record_stream_bypasses_cache_when_disabled(tmp_path: Path, monkeypatch) -> None:
    project_yaml = _write_project(tmp_path)
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
    )
    runtime.cache_enabled = False
    runtime.registries.partition_by.register("prices.aapl", None)
    runtime.registries.debug_operations.register("prices.aapl", [])
    runtime.registries.stream_specs.register(
        "prices.aapl",
        StreamRuntimeSpec(pipeline="ingest"),
    )
    context = PipelineContext(runtime)
    records = [
        _Rec(time=_ts(1), symbol="AAPL", value=1.0),
        _Rec(time=_ts(2), symbol="AAPL", value=2.0),
    ]
    calls = {"count": 0, "node": None}

    def _build(*args, **kwargs):
        calls["count"] += 1
        calls["node"] = kwargs.get("node")
        return iter(records)

    monkeypatch.setattr("datapipeline.pipelines.ingest.build_ingest_pipeline", _build)

    first = list(cached_record_stream(context, "prices.aapl"))
    second = list(cached_record_stream(context, "prices.aapl"))

    assert first == records
    assert second == records
    assert calls["count"] == 2
