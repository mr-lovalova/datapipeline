import json
from datetime import datetime, timezone
from pathlib import Path

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines import (
    build_full_dag,
    build_full_pipeline,
    build_feature_dag,
    build_feature_pipeline,
    build_stream_id_dag,
    build_vector_dag,
    build_vector_pipeline,
)
from datapipeline.pipelines.ingest import build_ingest_pipeline
from datapipeline.pipelines.stream import build_stream_pipeline
from datapipeline.pipelines.vector import dag as vector_dag
from datapipeline.pipelines.vector.nodes import sample_domain_window_keys
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.pipelines.full.split import apply_split_stage
from datapipeline.runtime import Runtime, StreamRuntimeSpec
from datapipeline.services.constants import POSTPROCESS_TRANSFORMS, VECTOR_SCHEMA


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


class _DagParentObserver:
    def __init__(self) -> None:
        self.dag_parents: list[tuple[str, str | None, str | None]] = []

    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        dag_metadata=None,
        dag_parent=None,
    ) -> None:
        _ = node_count, depth, dag_metadata
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


def _runtime_with_rows(
    tmp_path: Path,
    rows: list[dict],
    *,
    stream_id: str = "stream",
    record_ops: list[dict] | None = None,
    stream_ops: list[dict] | None = None,
    partition_by: str | None = None,
    feature_id_by: str | list[str] | None = None,
) -> Runtime:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)

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
        regs.sort_batch_size.register(stream_id, 128)
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
    regs.sort_batch_size.register(ingest_id, 128)

    regs.stream_sources.register(stream_id, _UpstreamSource(runtime, ingest_id))
    regs.stream_specs.register(stream_id, StreamRuntimeSpec(pipeline="stream"))
    regs.mappers.register(stream_id, _identity)
    regs.record_operations.register(stream_id, [])
    regs.stream_operations.register(stream_id, stream_ops)
    regs.debug_operations.register(stream_id, [])
    regs.partition_by.register(stream_id, partition_by)
    regs.feature_id_by.register(stream_id, feature_id_by)
    regs.ordered_by.register(stream_id, None)
    regs.sort_batch_size.register(stream_id, 128)
    return runtime


def test_node_0_to_2_record_pipeline(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0, 30), "value": 1.0},
        {"time": _ts(0, 10), "value": 2.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        record_ops=[{"floor_time": {"cadence": "1h"}}],
    )
    ctx = PipelineContext(runtime)

    stage0 = list(build_ingest_pipeline(ctx, "stream", node=0))
    assert stage0 == rows

    stage1 = list(build_ingest_pipeline(ctx, "stream", node=1))
    assert all(isinstance(rec, TemporalRecord) for rec in stage1)
    assert [rec.time for rec in stage1] == [rows[0]["time"], rows[1]["time"]]

    stage2 = list(build_ingest_pipeline(ctx, "stream", node=2))
    assert all(rec.time.minute == 0 for rec in stage2)


def test_dag_builders_expose_structural_child_dags(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [{"time": _ts(0), "value": 1.0}])
    context = PipelineContext(runtime)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")

    stream_id_dag = build_stream_id_dag(context, "stream")
    feature_dag = build_feature_dag(context, cfg)
    preview_feature_dag = build_feature_dag(context, cfg, include_record_nodes=True)
    vector_dag = build_vector_dag(context, [cfg], "1h", rectangular=False)
    full_dag = build_full_dag(context, [cfg], "1h", rectangular=False)

    assert [node.name for node in stream_id_dag.nodes[:2]] == [
        "open_source",
        "map_records",
    ]
    assert [node.name for node in feature_dag.nodes] == [
        "build_feature_stream",
        "feature_transforms",
        "order_feature_records",
    ]
    assert preview_feature_dag.nodes[0].name == "open_source"

    feature_fanout = vector_dag.nodes[0]
    assert feature_fanout.kind == "dag_fanout"
    assert [dag.name for dag in feature_fanout.child_dags] == ["feature:price"]

    vector_assemble = full_dag.nodes[0]
    assert vector_assemble.kind == "dag_call"
    assert [dag.name for dag in vector_assemble.child_dags] == ["vector:assemble"]


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
        stream_ops=[{"ensure_cadence": {"cadence": "1h", "field": "value"}}],
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
        stream_ops=[{"ensure_cadence": {"cadence": "1h", "field": "value"}}],
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
        stream_ops=[{"ensure_cadence": {"cadence": "model_grid", "field": "value"}}],
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
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(
        record_stream="stream",
        id="price",
        field="value",
        sequence={"size": 2, "stride": 1},
    )

    sequences = list(build_feature_pipeline(ctx, cfg, node=7))
    assert len(sequences) == 2
    assert isinstance(sequences[0], FeatureRecordSequence)
    assert sequences[0].values == [1.0, 2.0]
    assert sequences[1].values == [2.0, 3.0]


def test_node_7_vs_8_postprocess(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": None},
        {"time": _ts(1), "value": 2.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    schema_path = runtime.artifacts_root / "schema.json"
    schema_doc = {"features": [{"id": "price"}], "targets": []}
    schema_path.write_text(json.dumps(schema_doc, indent=2), encoding="utf-8")
    runtime.artifacts.register(
        VECTOR_SCHEMA,
        relative_path=schema_path.relative_to(runtime.artifacts_root).as_posix(),
    )
    runtime.registries.postprocesses.register(
        POSTPROCESS_TRANSFORMS,
        [{"replace": {"value": 0}}],
    )
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")

    raw = list(build_vector_pipeline(ctx, [cfg], "1h", rectangular=False))
    assert raw[0].features.values["price"] is None

    processed = list(post_process(ctx, iter(raw)))
    assert processed[0].features.values["price"] == 0


def test_full_pipeline_matches_manual_chain(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": None},
        {"time": _ts(1), "value": 2.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    schema_path = runtime.artifacts_root / "schema.json"
    schema_doc = {"features": [{"id": "price"}], "targets": []}
    schema_path.write_text(json.dumps(schema_doc, indent=2), encoding="utf-8")
    runtime.artifacts.register(
        VECTOR_SCHEMA,
        relative_path=schema_path.relative_to(runtime.artifacts_root).as_posix(),
    )
    runtime.registries.postprocesses.register(
        POSTPROCESS_TRANSFORMS,
        [{"replace": {"value": 0}}],
    )
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")

    full_out = list(build_full_pipeline(ctx, [cfg], "1h", rectangular=False))

    manual = build_vector_pipeline(ctx, [cfg], "1h", rectangular=False)
    manual = post_process(ctx, manual)
    manual = apply_split_stage(runtime, manual)
    manual_out = list(manual)

    assert full_out == manual_out


def test_vector_pipeline_parents_feature_dags_to_fanout_node(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        rows=[{"time": _ts(0), "value": 1.0}],
    )
    observer = _DagParentObserver()
    context = PipelineContext(runtime, execution_observer=observer)
    cfg = FeatureRecordConfig(record_stream="stream", id="price", field="value")

    assert list(build_vector_pipeline(context, [cfg], "1h", rectangular=False))

    assert (
        "feature:price",
        "vector:assemble",
        "feature_fanout",
    ) in observer.dag_parents


def test_vector_assembly_closes_feature_streams_when_stopped_early(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        rows=[{"time": _ts(0), "value": 1.0}],
    )
    context = PipelineContext(runtime)
    configs = [
        FeatureRecordConfig(record_stream="stream", id="a", field="value"),
        FeatureRecordConfig(record_stream="stream", id="b", field="value"),
    ]
    closed_streams: list[str] = []

    def _stream_for(cfg: FeatureRecordConfig):
        def _iter():
            try:
                yield FeatureRecord(
                    record=TemporalRecord(time=_ts(0)),
                    id=cfg.id,
                    value=1.0,
                )
                yield FeatureRecord(
                    record=TemporalRecord(time=_ts(1)),
                    id=cfg.id,
                    value=2.0,
                )
            finally:
                closed_streams.append(cfg.id)

        return _iter()

    def _build_stream(_context, cfg, sample_keys=(), group_by_cadence=None):
        return _stream_for(cfg)

    monkeypatch.setattr(
        "datapipeline.pipelines.vector.dag.build_feature_pipeline",
        _build_stream,
    )

    vectors = vector_dag._assemble_vectors(context, configs, "1h")
    first = next(vectors)
    assert first[0] == (_ts(0),)
    assert closed_streams == []

    vectors.close()
    assert set(closed_streams) == {"a", "b"}
