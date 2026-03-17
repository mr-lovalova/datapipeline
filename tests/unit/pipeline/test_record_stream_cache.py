from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from datapipeline.cache.record_streams import RecordStreamCache, cached_record_stream
from datapipeline.dag.context import PipelineContext
from datapipeline.domain.record import TemporalRecord
from datapipeline.runtime import Runtime
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
    (project_root / "contracts").mkdir(parents=True)
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
                "  streams: ./contracts",
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
    contract_lines = [
        "kind: ingest",
        "id: prices.aapl",
        "source: raw.aapl",
        "mapper:",
        "  entrypoint: identity",
        "record: []",
        "stream: []",
        "debug: []",
    ]
    if partition_by is not None:
        contract_lines.append(f"partition_by: {partition_by}")
    contract_lines.append("")
    (project_root / "contracts" / "prices.aapl.yaml").write_text(
        "\n".join(contract_lines),
        encoding="utf-8",
    )
    return project_root / "project.yaml"


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


def test_record_stream_cache_invalidates_when_contract_changes(tmp_path: Path) -> None:
    project_yaml = _write_project(tmp_path)
    cache = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    list(cache.materialize("prices.aapl", iter([_Rec(time=_ts(1), symbol="AAPL", value=1.0)])))

    contract_path = project_yaml.parent / "contracts" / "prices.aapl.yaml"
    contract_path.write_text(
        contract_path.read_text(encoding="utf-8") + "partition_by: symbol\n",
        encoding="utf-8",
    )

    invalidated = RecordStreamCache(
        project_yaml=project_yaml,
        root=(artifacts_root(project_yaml) / "_system" / "cache").resolve(),
    )
    assert invalidated.load("prices.aapl") is None


def test_cached_record_stream_reuses_cache_without_rebuilding(tmp_path: Path, monkeypatch) -> None:
    project_yaml = _write_project(tmp_path)
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
    )
    runtime.registries.partition_by.register("prices.aapl", None)
    runtime.registries.debug_operations.register("prices.aapl", [])
    context = PipelineContext(runtime)
    records = [
        _Rec(time=_ts(1), symbol="AAPL", value=1.0),
        _Rec(time=_ts(2), symbol="AAPL", value=2.0),
    ]
    calls = {"count": 0}

    def _build(*args, **kwargs):
        calls["count"] += 1
        return iter(records)

    monkeypatch.setattr("datapipeline.pipelines.record.dag.build_record_pipeline", _build)

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
    assert source.loader.info_lines() == ["Loaded contract output from cache"]
    assert source.loader.debug_lines() == [
        "cache.file: record_stream/prices.aapl/stream_transforms/data.pkl"
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
    context = PipelineContext(runtime)
    records = [
        _Rec(time=_ts(1), symbol="AAPL", value=1.0),
        _Rec(time=_ts(2), symbol="AAPL", value=2.0),
    ]
    calls = {"count": 0, "stage": None}

    def _build(*args, **kwargs):
        calls["count"] += 1
        calls["stage"] = kwargs.get("stage")
        return iter(records)

    monkeypatch.setattr("datapipeline.pipelines.record.dag.build_record_pipeline", _build)

    first = list(cached_record_stream(context, "prices.aapl"))
    second = list(cached_record_stream(context, "prices.aapl"))

    assert first == records
    assert second == records
    assert calls["count"] == 2
