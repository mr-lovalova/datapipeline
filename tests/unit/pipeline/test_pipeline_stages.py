import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import VectorInputsTask
from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.dag.context import PipelineContext
from datapipeline.operations.artifacts.vector_inputs import materialize_vector_inputs
from datapipeline.pipelines import (
    build_full_dag,
    build_full_pipeline,
    build_feature_dag,
    build_feature_pipeline,
    build_stream_id_dag,
    build_vector_pipeline,
)
from datapipeline.pipelines.ingest import build_ingest_dag, build_ingest_pipeline
from datapipeline.pipelines.stream import build_stream_dag, build_stream_pipeline
from datapipeline.pipelines.vector import dag as vector_dag
from datapipeline.pipelines.vector.nodes import sample_domain_window_keys, window_keys
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.runtime import Runtime, StreamRuntimeSpec
from datapipeline.services.constants import (
    POSTPROCESS_TRANSFORMS,
    VECTOR_INPUTS,
    VECTOR_SCHEMA,
)
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder
from datapipeline.transforms.spec import TransformSpec
from datapipeline.vector_inputs import CachedVectorInputShard
from tests.vector_input_helpers import register_vector_inputs


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour=hour, minute=minute, tzinfo=timezone.utc)


def test_sample_domain_window_keys_emit_sorted_composite_keys() -> None:
    domain = [
        {"key": ["MSFT"], "start": _ts(0), "end": _ts(1)},
        {"key": ["AAPL"], "start": _ts(1), "end": _ts(1)},
    ]

    keys = list(
        sample_domain_window_keys(
            _ts(0),
            _ts(2),
            "1h",
            ["security_id"],
            domain,
        )
    )

    assert keys == [
        (_ts(0), "MSFT"),
        (_ts(1), "AAPL"),
        (_ts(1), "MSFT"),
    ]


def test_window_keys_rejects_invalid_cadence() -> None:
    with pytest.raises(ValueError, match="Unsupported cadence"):
        window_keys(_ts(0), _ts(1), "0m")


def test_sample_domain_window_keys_rejects_invalid_cadence() -> None:
    with pytest.raises(ValueError, match="Unsupported cadence"):
        sample_domain_window_keys(
            _ts(0),
            _ts(1),
            "0m",
            ["security_id"],
            [{"key": ["AAPL"], "start": _ts(0), "end": _ts(1)}],
        )


class _StubSource:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def stream(self):
        return iter(self._rows)


class _UpstreamSource:
    def __init__(self, runtime: Runtime, upstream_id: str) -> None:
        self._runtime = runtime
        self._upstream_id = upstream_id

    def stream(self):
        from datapipeline.pipelines.record.streams import open_record_stream

        return open_record_stream(PipelineContext(self._runtime), self._upstream_id)


class _LoaderSource:
    def __init__(self, loader) -> None:
        self.loader = loader

    def stream(self):
        return iter(())


class _DagParentObserver:
    def __init__(self) -> None:
        self.dag_parents: list[tuple[str, str | None, str | None]] = []

    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        summary=None,
        dag_parent=None,
    ) -> None:
        _ = node_count, depth, summary
        self.dag_parents.append(
            (
                dag_name,
                None if dag_parent is None else dag_parent.dag_name,
                None if dag_parent is None else dag_parent.node_name,
            )
        )

    def on_node_start(self, **kwargs) -> None:
        pass

    def on_node_end(self, event) -> None:
        pass

    def on_dag_end(self, event) -> None:
        pass


def _mapper(rows):
    for row in rows:
        rec = TemporalRecord(time=row["time"])
        for key, value in row.items():
            if key == "time":
                continue
            setattr(rec, key, value)
        yield rec


def _identity(rows):
    yield from rows


def _sample_payload(samples):
    return [
        (
            item.key,
            dict(item.features.values),
            None if item.targets is None else dict(item.targets.values),
        )
        for item in samples
    ]


def _runtime_with_rows(
    tmp_path: Path,
    rows: list[dict],
    *,
    stream_id: str = "stream",
    record_ops: list[TransformSpec] | None = None,
    stream_ops: list[TransformSpec] | None = None,
    partition_by: str | None = None,
    feature_id_by: str | list[str] | None = None,
) -> Runtime:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        execution=ExecutionConfig(sort_batch_records=128),
    )

    regs = runtime.registries
    if stream_ops is None:
        regs.stream_specs.register(stream_id, StreamRuntimeSpec(pipeline="ingest"))
        regs.stream_sources.register(stream_id, _StubSource(rows))
        regs.mappers.register(stream_id, _mapper)
        regs.record_operations.register(stream_id, record_ops or [])
        regs.stream_operations.register(stream_id, [])
        regs.debug_operations.register(stream_id, [])
        regs.partition_by.register(stream_id, partition_by)
        regs.feature_id_by.register(stream_id, feature_id_by)
        regs.ordered_by.register(stream_id, None)
        return runtime

    ingest_id = f"{stream_id}.ingest"
    regs.stream_specs.register(ingest_id, StreamRuntimeSpec(pipeline="ingest"))
    regs.stream_sources.register(ingest_id, _StubSource(rows))
    regs.mappers.register(ingest_id, _mapper)
    regs.record_operations.register(ingest_id, record_ops or [])
    regs.stream_operations.register(ingest_id, [])
    regs.debug_operations.register(ingest_id, [])
    regs.partition_by.register(ingest_id, partition_by)
    regs.feature_id_by.register(ingest_id, feature_id_by)
    regs.ordered_by.register(ingest_id, None)

    regs.stream_sources.register(stream_id, _UpstreamSource(runtime, ingest_id))
    regs.stream_specs.register(stream_id, StreamRuntimeSpec(pipeline="stream"))
    regs.mappers.register(stream_id, _identity)
    regs.record_operations.register(stream_id, [])
    regs.stream_operations.register(stream_id, stream_ops)
    regs.debug_operations.register(stream_id, [])
    regs.partition_by.register(stream_id, partition_by)
    regs.feature_id_by.register(stream_id, feature_id_by)
    regs.ordered_by.register(stream_id, None)
    return runtime


def _register_price_schema(runtime: Runtime) -> None:
    schema_path = runtime.artifacts_root / "schema.json"
    schema_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "features": [{"id": "price", "kind": "scalar"}],
                "targets": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    runtime.artifacts.register(VECTOR_SCHEMA, "schema.json")


def test_ingest_dag_carries_source_summary(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [], stream_id="prices")
    runtime.registries.stream_sources.register(
        "prices",
        _LoaderSource(
            DataLoader(
                FsFileTransport("/tmp/prices.jsonl"),
                JsonLinesDecoder(),
            )
        ),
    )

    dag = build_ingest_dag(PipelineContext(runtime), "prices")

    assert dag.summary == "transport=fs.file file=prices.jsonl"


def test_stream_dag_carries_source_summary(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [], stream_id="derived", stream_ops=[])
    source = _LoaderSource(
        DataLoader(
            FsGlobTransport("/definitely/not/real/*.jsonl"),
            JsonLinesDecoder(),
        )
    )
    source.loader.transport._files = [  # type: ignore[attr-defined]
        "/tmp/AAPL.jsonl",
        "/tmp/MSFT.jsonl",
    ]
    runtime.registries.stream_sources.register("derived", source)

    dag = build_stream_dag(PipelineContext(runtime), "derived")

    assert dag.summary == "transport=fs.glob count=2 first=AAPL.jsonl last=MSFT.jsonl"


def test_node_0_to_2_record_pipeline(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0, 30), "value": 1.0},
        {"time": _ts(0, 10), "value": 2.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        record_ops=[TransformSpec(name="floor_time", params={"cadence": "1h"})],
    )
    ctx = PipelineContext(runtime)

    stage0 = list(build_ingest_pipeline(ctx, "stream", node=0))
    assert stage0 == rows

    stage1 = list(build_ingest_pipeline(ctx, "stream", node=1))
    assert all(isinstance(rec, TemporalRecord) for rec in stage1)
    assert [rec.time for rec in stage1] == [rows[0]["time"], rows[1]["time"]]

    stage2 = list(build_ingest_pipeline(ctx, "stream", node=2))
    assert all(rec.time.minute == 0 for rec in stage2)


def test_dag_builders_expose_structure(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [{"time": _ts(0), "value": 1.0}])
    context = PipelineContext(runtime)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")

    stream_id_dag = build_stream_id_dag(context, "stream")
    feature_dag = build_feature_dag(context, cfg)
    preview_feature_dag = build_feature_dag(context, cfg, include_record_nodes=True)
    vector_assemble = build_full_dag(context, [cfg], "1h", rectangular=False).nodes[0]

    assert [node.name for node in stream_id_dag.nodes[:2]] == [
        "open_source",
        "map_records",
    ]
    assert [node.name for node in feature_dag.nodes] == [
        "build_feature_stream",
        "feature_transforms",
        "order_feature_records",
    ]
    assert feature_dag.summary is None
    assert preview_feature_dag.nodes[0].name == "open_source"
    assert vector_assemble.kind == "function"
    assert vector_assemble.calls_dag is None


def test_node_3_orders_by_partition_and_time(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(1), "value": 10.0, "symbol": "B"},
        {"time": _ts(0), "value": 5.0, "symbol": "A"},
        {"time": _ts(2), "value": 6.0, "symbol": "A"},
    ]
    runtime = _runtime_with_rows(tmp_path, rows, partition_by="symbol")
    ctx = PipelineContext(runtime)

    ordered = list(build_ingest_pipeline(ctx, "stream", node=3))
    assert [(rec.symbol, rec.time.hour) for rec in ordered] == [
        ("A", 0),
        ("A", 2),
        ("B", 1),
    ]


def test_node_4_applies_stream_transforms(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": 1.0},
        {"time": _ts(2), "value": 2.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        stream_ops=[
            TransformSpec(
                name="ensure_cadence",
                params={"cadence": "1h", "field": "value"},
            )
        ],
    )
    ctx = PipelineContext(runtime)

    transformed = list(build_stream_pipeline(ctx, "stream", node=3))
    assert [(rec.time.hour, rec.value) for rec in transformed] == [
        (0, 1.0),
        (1, None),
        (2, 2.0),
    ]


def test_ensure_cadence_placeholders_do_not_copy_payload_fields(
    tmp_path: Path,
) -> None:
    rows = [
        {"time": _ts(0), "symbol": "A", "value": 1.0, "volume": 100},
        {"time": _ts(2), "symbol": "A", "value": 2.0, "volume": 200},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        partition_by="symbol",
        stream_ops=[
            TransformSpec(
                name="ensure_cadence",
                params={"cadence": "1h", "field": "value"},
            )
        ],
    )
    ctx = PipelineContext(runtime)

    placeholder = list(build_stream_pipeline(ctx, "stream", node=3))[1]

    assert placeholder.time == _ts(1)
    assert placeholder.symbol == "A"
    assert placeholder.value is None
    assert placeholder.volume is None


def test_ensure_cadence_uses_stream_partition_for_tick_artifact(
    tmp_path: Path,
) -> None:
    rows = [
        {"time": _ts(1), "symbol": "A", "value": 1.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        partition_by="symbol",
        stream_ops=[
            TransformSpec(
                name="ensure_cadence",
                params={"cadence": "model_grid", "field": "value"},
            )
        ],
    )
    artifact_path = runtime.artifacts_root / "model_grid.jsonl"
    artifact_path.write_text(
        "\n".join(
            [
                json.dumps({"time": _ts(0).isoformat(), "symbol": "A"}),
                json.dumps({"time": _ts(1).isoformat(), "symbol": "A"}),
                json.dumps({"time": _ts(2).isoformat(), "symbol": "A"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    runtime.artifacts.register(
        "model_grid",
        artifact_path.name,
        meta={"grid_by": ["symbol"]},
    )
    ctx = PipelineContext(runtime)

    transformed = list(build_stream_pipeline(ctx, "stream", node=3))

    assert [(rec.symbol, rec.time.hour, rec.value) for rec in transformed] == [
        ("A", 0, None),
        ("A", 1, 1.0),
        ("A", 2, None),
    ]


def test_node_6_wraps_feature_values(tmp_path: Path) -> None:
    rows = [{"time": _ts(0), "value": 3.0, "symbol": "X"}]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        partition_by="symbol",
        feature_id_by="symbol",
    )
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(
        record_stream="stream",
        id="price",
        field="value",
    )

    features = list(build_feature_pipeline(ctx, cfg, node=6))
    assert len(features) == 1
    feature = features[0]
    assert feature.value == 3.0
    assert feature.id == "price__@symbol:X"


def test_node_7_applies_feature_transforms(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": 1.0},
        {"time": _ts(1), "value": 2.0},
        {"time": _ts(2), "value": 3.0},
        {"time": _ts(3), "value": 4.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(
        record_stream="stream",
        id="price",
        field="value",
        sequence={"size": 2, "stride": 2},
    )

    sequences = list(build_feature_pipeline(ctx, cfg, node=7))
    assert len(sequences) == 2
    assert isinstance(sequences[0], FeatureRecordSequence)
    assert sequences[0].values == [1.0, 2.0]
    assert sequences[1].values == [3.0, 4.0]


def test_node_7_vs_8_postprocess(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": None},
        {"time": _ts(1), "value": 2.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    _register_price_schema(runtime)
    runtime.registries.postprocesses.register(
        POSTPROCESS_TRANSFORMS,
        [TransformSpec(name="replace", params={"value": 0})],
    )
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")
    register_vector_inputs(runtime, [cfg], "1h")

    raw = list(build_vector_pipeline(ctx, [cfg], "1h", rectangular=False))
    assert raw[0].features.values["price"] is None

    processed = list(post_process(ctx, iter(raw)))
    assert processed[0].features.values["price"] == 0


def test_post_process_rejects_targets_absent_from_schema(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [])
    _register_price_schema(runtime)
    runtime.registries.postprocesses.register(POSTPROCESS_TRANSFORMS, [])
    samples = [
        Sample(key=(_ts(0),), features=Vector(values={"price": 1.0})),
        Sample(
            key=(_ts(1),),
            features=Vector(values={"price": 2.0}),
            targets=Vector(values={"return": 0.1}),
        ),
    ]

    with pytest.raises(RuntimeError, match="no target entries"):
        list(post_process(PipelineContext(runtime), iter(samples)))


def test_full_pipeline_matches_vector_and_postprocess_chain(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": None},
        {"time": _ts(1), "value": 2.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    _register_price_schema(runtime)
    runtime.registries.postprocesses.register(
        POSTPROCESS_TRANSFORMS,
        [TransformSpec(name="replace", params={"value": 0})],
    )
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")
    register_vector_inputs(runtime, [cfg], "1h")

    full_out = list(build_full_pipeline(ctx, [cfg], "1h", rectangular=False))

    manual = build_vector_pipeline(ctx, [cfg], "1h", rectangular=False)
    manual_out = list(post_process(ctx, manual))

    assert full_out == manual_out


def test_vector_inputs_artifact_feeds_serve_pipeline(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(1), "id_": "B", "value": 2.0, "other": 20.0},
        {"time": _ts(0), "id_": "A", "value": 1.0, "other": 10.0},
        {"time": _ts(0), "id_": "B", "value": 3.0, "other": 30.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        stream_id="prices",
        partition_by="id_",
        feature_id_by=[],
    )
    for name in ("ingests", "streams", "sources", "tasks", "profiles"):
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
    (tmp_path / "dataset.yaml").write_text(
        "\n".join(
            [
                "sample:",
                "  cadence: 1h",
                "  keys: [id_]",
                "features:",
                "  - id: value_feature",
                "    record_stream: prices",
                "    field: value",
                "  - id: other_feature",
                "    record_stream: prices",
                "    field: other",
                "targets: []",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "postprocess.yaml").write_text("transforms: []\n", encoding="utf-8")
    runtime.project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  ingests: ingests",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: artifacts",
                "  tasks: tasks",
                "  profiles: profiles",
            ]
        ),
        encoding="utf-8",
    )
    configs = [
        FeatureRecordConfig(
            record_stream="prices",
            id="value_feature",
            field="value",
        ),
        FeatureRecordConfig(
            record_stream="prices",
            id="other_feature",
            field="other",
        ),
    ]

    result = materialize_vector_inputs(runtime, VectorInputsTask())
    runtime.artifacts.register(
        VECTOR_INPUTS,
        relative_path=result.relative_path,
        meta=result.meta,
    )
    cached = _sample_payload(
        build_vector_pipeline(
            PipelineContext(runtime),
            configs,
            "1h",
            rectangular=False,
            sample_keys=["id_"],
        )
    )

    assert result.meta == {
        "features": 2,
        "targets": 0,
        "feature_rows": 6,
        "target_rows": 0,
        "format": "jsonl.gz",
    }
    assert cached == [
        (
            (_ts(0), "A"),
            {"value_feature": 1.0, "other_feature": 10.0},
            None,
        ),
        (
            (_ts(0), "B"),
            {"value_feature": 3.0, "other_feature": 30.0},
            None,
        ),
        (
            (_ts(1), "B"),
            {"value_feature": 2.0, "other_feature": 20.0},
            None,
        ),
    ]


def test_vector_pipeline_requires_vector_inputs_artifact(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        rows=[{"time": _ts(0), "value": 1.0}],
    )
    context = PipelineContext(runtime)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")

    with pytest.raises(RuntimeError, match="Vector inputs artifact is required"):
        list(build_vector_pipeline(context, [cfg], "1h", rectangular=False))


def test_cached_vector_pipeline_rejects_manifest_cadence_mismatch(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        rows=[{"time": _ts(0), "value": 1.0}],
    )
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")
    register_vector_inputs(runtime, [cfg], "1h")
    manifest = runtime.artifacts_root / "build/vector_inputs/manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["group_by"] = "1d"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="group_by does not match"):
        list(
            build_vector_pipeline(
                PipelineContext(runtime),
                [cfg],
                "1h",
                rectangular=False,
            )
        )


def test_cached_vector_pipeline_reads_requested_feature_subset(
    tmp_path: Path,
) -> None:
    rows = [
        {"time": _ts(0), "value": 1.0, "other": 10.0},
        {"time": _ts(1), "value": 2.0, "other": 20.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    value_cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")
    other_cfg = FeatureRecordConfig(record_stream="stream", id="other", field="other")
    register_vector_inputs(runtime, [value_cfg, other_cfg], "1h")

    samples = list(
        build_vector_pipeline(
            PipelineContext(runtime),
            [value_cfg],
            "1h",
            rectangular=False,
        )
    )

    assert _sample_payload(samples) == [
        ((_ts(0),), {"price": 1.0}, None),
        ((_ts(1),), {"price": 2.0}, None),
    ]


def test_cached_vector_records_close_streams_when_stopped_early(
    tmp_path: Path,
    monkeypatch,
) -> None:
    configs = [
        FeatureRecordConfig(record_stream="stream", id="a", field="value"),
        FeatureRecordConfig(record_stream="stream", id="b", field="value"),
    ]
    closed_streams: list[str] = []

    class _ClosingStream:
        def __init__(self, feature_id: str) -> None:
            self.feature_id = feature_id
            self.items = iter(
                [
                    FeatureRecord(
                        record=TemporalRecord(time=_ts(0)),
                        id=feature_id,
                        value=1.0,
                    ),
                    FeatureRecord(
                        record=TemporalRecord(time=_ts(1)),
                        id=feature_id,
                        value=2.0,
                    ),
                ]
            )

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.items)

        def close(self) -> None:
            closed_streams.append(self.feature_id)

    def _open_records(path):
        if path.name == "a.jsonl.gz":
            return _ClosingStream("a")
        if path.name == "b.jsonl.gz":
            return _ClosingStream("b")
        raise AssertionError(path)

    monkeypatch.setattr(
        "datapipeline.pipelines.vector.dag.open_vector_input_records",
        _open_records,
    )

    records = vector_dag._merged_cached_records(
        manifest_path=tmp_path / "manifest.json",
        shards=(
            CachedVectorInputShard(id="a", path="a.jsonl.gz", rows=2),
            CachedVectorInputShard(id="b", path="b.jsonl.gz", rows=2),
        ),
        configs=configs,
        group_by_cadence="1h",
    )
    first = next(records)
    assert first.id == "a"
    assert closed_streams == []

    records.close()
    assert set(closed_streams) == {"a", "b"}
