import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import datapipeline.operations.artifacts.vector_inputs as vector_inputs_operation
from datapipeline.artifacts.models import SampleDomainEntry
from datapipeline.artifacts.scaler import (
    ScalerStatistics,
    StandardScalerArtifact,
    save_scaler_artifact,
)
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig, SequenceConfig
from datapipeline.config.tasks import VectorInputsTask
from datapipeline.config.transforms import (
    EnsureCadenceConfig,
    EnsureTicksConfig,
    FloorTimeConfig,
    RecordTransformConfig,
    StreamTransformConfig,
)
from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample import Sample
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.domain.vector import Vector
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import SourceNode
from datapipeline.execution.observer import NoopPipelineObserver
from datapipeline.execution.runner import run_pipeline
from datapipeline.operations.artifacts.vector_inputs import materialize_vector_inputs
from datapipeline.parsers.identity import IdentityParser
from datapipeline.pipelines.feature.pipeline import (
    build_feature_pipeline,
    run_feature_pipeline,
)
from datapipeline.pipelines.dataset.nodes import apply_postprocess
from datapipeline.pipelines.dataset.pipeline import run_dataset_pipeline
from datapipeline.pipelines.stream.pipeline import (
    build_stream_pipeline,
    run_stream_pipeline,
)
from datapipeline.pipelines.vector.nodes import sample_domain_window_keys, window_keys
from datapipeline.pipelines.vector import pipeline as vector_pipeline
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.runtime import (
    AlignedRuntimeStream,
    DerivedRuntimeStream,
    IngestRuntimeStream,
    Runtime,
)
from datapipeline.services.constants import (
    SCALER_STATISTICS,
    VECTOR_INPUTS,
    VECTOR_SCHEMA,
)
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.decoders import JsonLinesDecoder
from datapipeline.sources.models.source import Source
from datapipeline.vector_inputs.store import (
    CachedVectorInputShard,
    feature_record_to_vector_input_row,
    load_vector_inputs_manifest,
    open_vector_input_records,
    prune_vector_input_cache,
)
from datapipeline.utils.time import parse_cadence
from tests.vector_input_helpers import register_vector_inputs


def _ts(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 1, hour=hour, minute=minute, tzinfo=timezone.utc)


def test_sample_domain_window_keys_emit_sorted_composite_keys() -> None:
    domain = [
        SampleDomainEntry(key=["MSFT"], start=_ts(0), end=_ts(1)),
        SampleDomainEntry(key=["AAPL"], start=_ts(1), end=_ts(1)),
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
            [SampleDomainEntry(key=["AAPL"], start=_ts(0), end=_ts(1))],
        )


def test_sample_domain_window_keys_rejects_mismatched_key_width() -> None:
    with pytest.raises(ValueError, match="key length"):
        sample_domain_window_keys(
            _ts(0),
            _ts(1),
            "1h",
            ["security_id"],
            [SampleDomainEntry(key=[], start=_ts(0), end=_ts(1))],
        )


class _StubSource:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.opens = 0
        self.closes = 0

    def stream(self):
        self.opens += 1
        try:
            yield from self._rows
        finally:
            self.closes += 1


class _PipelineStarts(NoopPipelineObserver):
    def __init__(self) -> None:
        self.starts: list[tuple[str, int]] = []
        self.nodes: list[str] = []

    def on_pipeline_start(
        self,
        pipeline_name: str,
        node_count: int,
        summary: str | None = None,
    ) -> None:
        self.starts.append((pipeline_name, node_count))

    def on_node_start(
        self,
        pipeline_name: str,
        node_name: str,
        node_index: int,
    ) -> None:
        self.nodes.append(node_name)


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
    record_ops: list[RecordTransformConfig] | None = None,
    stream_ops: list[StreamTransformConfig] | None = None,
    partition_by: tuple[str, ...] = (),
) -> Runtime:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\nartifact_revision: 1\n", encoding="utf-8")
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
        execution=ExecutionConfig(),
    )

    if stream_ops is None:
        runtime.streams[stream_id] = IngestRuntimeStream(
            source=_StubSource(rows),
            mapper=_mapper,
            transforms=tuple(record_ops or ()),
            partition_by=partition_by,
            presorted=False,
        )
        return runtime

    ingest_id = f"{stream_id}.ingest"
    runtime.streams[ingest_id] = IngestRuntimeStream(
        source=_StubSource(rows),
        mapper=_mapper,
        transforms=tuple(record_ops or ()),
        partition_by=partition_by,
        presorted=False,
    )
    runtime.streams[stream_id] = DerivedRuntimeStream(
        input_stream=ingest_id,
        mapper=_identity,
        transforms=tuple(stream_ops),
        partition_by=partition_by,
        presorted=False,
    )
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


def test_ingest_pipeline_carries_source_summary(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [], stream_id="prices")
    runtime.streams["prices"] = replace(
        runtime.streams["prices"],
        source=Source(
            DataLoader(
                FsFileTransport("/tmp/prices.jsonl"),
                JsonLinesDecoder(),
            ),
            IdentityParser(),
        ),
    )

    pipeline = build_stream_pipeline(PipelineContext(runtime), "prices")

    assert pipeline.name == "ingest:prices"
    assert pipeline.summary == "transport=fs.file file=prices.jsonl"
    assert [node.name for node in pipeline.nodes] == [
        "open_source",
        "map_records",
        "order_records",
    ]
    assert [node.progress is not None for node in pipeline.nodes] == [
        True,
        False,
        True,
    ]


def test_stream_pipeline_carries_source_summary(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [], stream_id="derived", stream_ops=[])
    source = Source(
        DataLoader(
            FsGlobTransport("/definitely/not/real/*.jsonl"),
            JsonLinesDecoder(),
        ),
        IdentityParser(),
    )
    source.loader.transport._files = [  # type: ignore[attr-defined]
        "/tmp/AAPL.jsonl",
        "/tmp/MSFT.jsonl",
    ]
    runtime.streams["derived.ingest"] = replace(
        runtime.streams["derived.ingest"],
        source=source,
    )

    pipeline = build_stream_pipeline(PipelineContext(runtime), "derived")

    assert pipeline.name == "stream:derived"
    assert pipeline.summary == (
        "transport=fs.glob count=2 first=AAPL.jsonl last=MSFT.jsonl"
    )
    assert [node.name for node in pipeline.nodes] == [
        "ingest:derived.ingest/open_source",
        "ingest:derived.ingest/map_records",
        "ingest:derived.ingest/order_records",
        "map_records",
        "order_records",
    ]
    assert [node.progress is not None for node in pipeline.nodes] == [
        True,
        False,
        True,
        False,
        True,
    ]


def test_aligned_pipeline_closes_inputs_after_partial_read(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [])
    left = _StubSource([{"time": _ts(0)}, {"time": _ts(1)}])
    right = _StubSource([{"time": _ts(0)}, {"time": _ts(1)}])

    def take_left(rows):
        for left_record, _ in rows:
            yield left_record

    runtime.streams = {
        "left": IngestRuntimeStream(
            source=left,
            mapper=_mapper,
            transforms=(),
            partition_by=(),
            presorted=True,
        ),
        "right": IngestRuntimeStream(
            source=right,
            mapper=_mapper,
            transforms=(),
            partition_by=(),
            presorted=True,
        ),
        "combined": AlignedRuntimeStream(
            input_streams=("left", "right"),
            combine=take_left,
            transforms=(),
            partition_by=(),
            presorted=True,
        ),
    }

    records = run_stream_pipeline(PipelineContext(runtime), "combined")
    assert next(records).time == _ts(0)
    records.close()

    assert left.closes == 1
    assert right.closes == 1


def test_ingest_pipeline_exposes_source_mapping_and_record_transforms(
    tmp_path: Path,
) -> None:
    rows = [
        {"time": _ts(0, 30), "value": 1.0},
        {"time": _ts(0, 10), "value": 2.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        record_ops=[FloorTimeConfig(cadence="1h")],
    )
    ctx = PipelineContext(runtime)

    pipeline = build_stream_pipeline(ctx, "stream")
    stage0 = list(run_pipeline(ctx, pipeline.through_node(0)))
    assert stage0 == rows

    stage1 = list(run_pipeline(ctx, pipeline.through_node(1)))
    assert all(isinstance(rec, TemporalRecord) for rec in stage1)
    assert [rec.time for rec in stage1] == [rows[0]["time"], rows[1]["time"]]

    stage2 = list(run_pipeline(ctx, pipeline.through_node_named("floor_time")))
    assert all(rec.time.minute == 0 for rec in stage2)


def test_pipeline_builders_expose_structure(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [{"time": _ts(0), "value": 1.0}])
    _register_price_schema(runtime)
    context = PipelineContext(runtime)
    cfg = FeatureRecordConfig(stream="stream", id="price", field="value")

    stream_pipeline = build_stream_pipeline(context, "stream")
    feature_pipeline = build_feature_pipeline(context, cfg)

    assert [node.name for node in stream_pipeline.nodes[:2]] == [
        "open_source",
        "map_records",
    ]
    assert [node.name for node in feature_pipeline.nodes] == [
        "ingest:stream/open_source",
        "ingest:stream/map_records",
        "ingest:stream/order_records",
        "build_feature_stream",
        "order_feature_records",
    ]
    assert feature_pipeline.summary is None
    assert isinstance(feature_pipeline.nodes[0], SourceNode)


def test_ingest_pipeline_orders_by_partition_and_time(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(1), "value": 10.0, "symbol": "B"},
        {"time": _ts(0), "value": 5.0, "symbol": "A"},
        {"time": _ts(2), "value": 6.0, "symbol": "A"},
    ]
    runtime = _runtime_with_rows(tmp_path, rows, partition_by=("symbol",))
    ctx = PipelineContext(runtime)

    ordered = list(run_pipeline(ctx, build_stream_pipeline(ctx, "stream")))
    assert [(rec.symbol, rec.time.hour) for rec in ordered] == [
        ("A", 0),
        ("A", 2),
        ("B", 1),
    ]


def test_stream_pipeline_applies_stream_transforms(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": 1.0},
        {"time": _ts(2), "value": 2.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        stream_ops=[EnsureCadenceConfig(cadence="1h")],
    )
    ctx = PipelineContext(runtime)

    transformed = list(run_stream_pipeline(ctx, "stream"))
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
        partition_by=("symbol",),
        stream_ops=[EnsureCadenceConfig(cadence="1h")],
    )
    ctx = PipelineContext(runtime)

    placeholder = list(run_stream_pipeline(ctx, "stream"))[1]

    assert placeholder.time == _ts(1)
    assert placeholder.symbol == "A"
    assert placeholder.value is None
    assert placeholder.volume is None


def test_ensure_ticks_uses_stream_partition_for_tick_artifact(
    tmp_path: Path,
) -> None:
    rows = [
        {"time": _ts(1), "symbol": "A", "value": 1.0},
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        partition_by=("symbol",),
        stream_ops=[EnsureTicksConfig(artifact="model_grid")],
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

    transformed = list(run_stream_pipeline(ctx, "stream"))

    assert [(rec.symbol, rec.time.hour, rec.value) for rec in transformed] == [
        ("A", 0, None),
        ("A", 1, 1.0),
        ("A", 2, None),
    ]


def test_feature_pipeline_wraps_record_values(tmp_path: Path) -> None:
    rows = [{"time": _ts(0), "value": 3.0, "symbol": "X"}]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        partition_by=("symbol",),
    )
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(
        stream="stream",
        id="price",
        field="value",
    )

    preview_pipeline = build_feature_pipeline(ctx, cfg)
    features = list(
        run_pipeline(
            ctx,
            preview_pipeline.through_node_named("build_feature_stream"),
        )
    )
    assert len(features) == 1
    feature = features[0]
    assert feature.value == 3.0
    assert feature.id == "price__@symbol:X"


def test_feature_pipeline_builds_sequences(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": 1.0},
        {"time": _ts(1), "value": 2.0},
        {"time": _ts(2), "value": 3.0},
        {"time": _ts(3), "value": 4.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(
        stream="stream",
        id="price",
        field="value",
        sequence={"size": 2, "stride": 2},
    )

    preview_pipeline = build_feature_pipeline(ctx, cfg)
    sequences = list(
        run_pipeline(
            ctx,
            preview_pipeline.through_node_named("sequence_features"),
        )
    )
    assert len(sequences) == 2
    assert isinstance(sequences[0], FeatureSequence)
    assert sequences[0].time == _ts(1)
    assert sequences[0].values == [1.0, 2.0]
    assert sequences[1].time == _ts(3)
    assert sequences[1].values == [3.0, 4.0]


def test_postprocess_rejects_targets_absent_from_schema(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(tmp_path, [])
    _register_price_schema(runtime)
    samples = [
        Sample(key=(_ts(0),), features=Vector(values={"price": 1.0})),
        Sample(
            key=(_ts(1),),
            features=Vector(values={"price": 2.0}),
            targets=Vector(values={"return": 0.1}),
        ),
    ]

    with pytest.raises(RuntimeError, match="no target entries"):
        list(apply_postprocess(PipelineContext(runtime), iter(samples)))


def test_dataset_pipeline_matches_vector_and_postprocess_chain(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(0), "value": None},
        {"time": _ts(1), "value": 2.0},
    ]
    runtime = _runtime_with_rows(tmp_path, rows)
    _register_price_schema(runtime)
    ctx = PipelineContext(runtime)
    cfg = FeatureRecordConfig(stream="stream", id="price", field="value")
    register_vector_inputs(runtime, [cfg], "1h")

    dataset_out = list(run_dataset_pipeline(ctx, [cfg], "1h", rectangular=False))

    manual = build_vector_pipeline(ctx, [cfg], "1h", rectangular=False)
    manual_out = list(apply_postprocess(ctx, manual))

    assert dataset_out == manual_out


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
        partition_by=("id_",),
    )
    configs = [
        FeatureRecordConfig(
            stream="prices",
            id="value_feature",
            field="value",
        ),
        FeatureRecordConfig(
            stream="prices",
            id="other_feature",
            field="other",
        ),
    ]
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h", keys=["id_"]),
        features=configs,
    )
    source = runtime.streams["prices"].source
    assert isinstance(source, _StubSource)

    unrelated = runtime.artifacts_root / "build/vector_inputs/features/keep.txt"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("keep", encoding="utf-8")

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
    manifest_path = runtime.artifacts_root / result.relative_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    shard_paths = [Path(shard["path"]) for shard in manifest["features"]]
    assert shard_paths[0].parts[:1] == ("manifest.shards",)
    assert shard_paths[0].parts[1] == shard_paths[1].parts[1]
    assert [path.parts[2:] for path in shard_paths] == [
        ("features", "000000.jsonl.gz"),
        ("features", "000001.jsonl.gz"),
    ]
    assert result.companion_paths == tuple(
        str(Path("build/vector_inputs") / path) for path in shard_paths
    )
    assert unrelated.read_text(encoding="utf-8") == "keep"
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
    assert source.opens == 1
    assert source.closes == 1


def test_vector_inputs_shared_stream_matches_independent_feature_pipelines(
    monkeypatch,
    tmp_path: Path,
) -> None:
    rows = [
        {
            "time": _ts(0),
            "exchange": "X",
            "symbol": "A",
            "value": 1.0,
            "volume": 10.0,
        },
        {
            "time": _ts(1),
            "exchange": "X",
            "symbol": "A",
            "value": 3.0,
            "volume": 30.0,
        },
        {
            "time": _ts(0),
            "exchange": "X",
            "symbol": "B",
            "value": 10.0,
            "volume": 100.0,
        },
        {
            "time": _ts(1),
            "exchange": "X",
            "symbol": "B",
            "value": 14.0,
            "volume": 140.0,
        },
    ]
    runtime = _runtime_with_rows(
        tmp_path,
        rows,
        partition_by=("exchange", "symbol"),
    )
    runtime.streams["stream"] = replace(runtime.streams["stream"], presorted=True)
    price = FeatureRecordConfig(
        stream="stream",
        id="price",
        field="value",
        scale=True,
        sequence=SequenceConfig(size=2),
    )
    volume = FeatureRecordConfig(
        stream="stream",
        id="volume",
        field="volume",
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h", keys=["exchange"]),
        features=[price],
        targets=[volume],
    )
    scaler_path = runtime.artifacts_root / "scaler.json"
    save_scaler_artifact(
        scaler_path,
        StandardScalerArtifact(
            with_mean=True,
            with_std=True,
            epsilon=1e-12,
            observations=4,
            statistics={
                "price__@symbol:A": ScalerStatistics(
                    mean=2.0,
                    std=1.0,
                    count=2,
                ),
                "price__@symbol:B": ScalerStatistics(
                    mean=12.0,
                    std=2.0,
                    count=2,
                ),
            },
        ),
    )
    runtime.artifacts.register(SCALER_STATISTICS, scaler_path.name)
    context = PipelineContext(runtime)
    expected_price = list(
        run_feature_pipeline(
            context,
            price,
            sample_keys=["exchange"],
            group_by_cadence="1h",
        )
    )
    expected_volume = list(
        run_feature_pipeline(
            context,
            volume,
            sample_keys=["exchange"],
            group_by_cadence="1h",
        )
    )

    for row in rows:
        row["unpickleable"] = lambda: None

    source = runtime.streams["stream"].source
    assert isinstance(source, _StubSource)
    source.opens = 0
    source.closes = 0
    normal_batch_sort = vector_inputs_operation.batch_sort

    def spilling_batch_sort(items, buffer_bytes, key, progress=None):
        return normal_batch_sort(
            items,
            buffer_bytes=1,
            key=key,
            progress=progress,
        )

    monkeypatch.setattr(
        vector_inputs_operation,
        "batch_sort",
        spilling_batch_sort,
    )

    result = materialize_vector_inputs(runtime, VectorInputsTask())
    manifest_path = runtime.artifacts_root / result.relative_path
    manifest = load_vector_inputs_manifest(manifest_path)
    actual_price = list(
        open_vector_input_records(manifest_path.parent / manifest.features[0].path)
    )
    actual_volume = list(
        open_vector_input_records(manifest_path.parent / manifest.targets[0].path)
    )

    assert [feature_record_to_vector_input_row(item) for item in actual_price] == [
        feature_record_to_vector_input_row(item) for item in expected_price
    ]
    assert [feature_record_to_vector_input_row(item) for item in actual_volume] == [
        feature_record_to_vector_input_row(item) for item in expected_volume
    ]
    assert source.opens == 1
    assert source.closes == 1


def test_vector_inputs_writes_empty_shards_from_a_shared_stream(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        [
            {"time": _ts(0), "value": 1.0},
            {"time": _ts(1), "value": 2.0},
        ],
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[
            FeatureRecordConfig(
                stream="stream",
                id="window",
                field="value",
                sequence=SequenceConfig(size=3),
            ),
            FeatureRecordConfig(
                stream="stream",
                id="value",
                field="value",
            ),
        ],
    )

    result = materialize_vector_inputs(runtime, VectorInputsTask())
    manifest_path = runtime.artifacts_root / result.relative_path
    manifest = load_vector_inputs_manifest(manifest_path)

    assert [shard.rows for shard in manifest.features] == [0, 2]
    assert (
        list(
            open_vector_input_records(manifest_path.parent / manifest.features[0].path)
        )
        == []
    )


def test_vector_input_sort_is_part_of_the_observed_stream_pipeline(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        [{"time": _ts(0), "value": 1.0}],
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[FeatureRecordConfig(stream="stream", id="value", field="value")],
    )
    observer = _PipelineStarts()
    runtime.pipeline_observer = observer

    materialize_vector_inputs(runtime, VectorInputsTask())

    assert observer.starts == [("vector_inputs:stream", 5)]
    assert "build_vector_inputs" in observer.nodes
    assert "order_vector_inputs" in observer.nodes


def test_vector_inputs_closes_shared_stream_after_feature_error(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        [{"time": _ts(0), "value": 1.0}],
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[
            FeatureRecordConfig(stream="stream", id="value", field="value"),
            FeatureRecordConfig(stream="stream", id="missing", field="missing"),
        ],
    )
    source = runtime.streams["stream"].source
    assert isinstance(source, _StubSource)

    with pytest.raises(KeyError, match="Record field 'missing'"):
        materialize_vector_inputs(runtime, VectorInputsTask())

    assert source.opens == 1
    assert source.closes == 1
    assert not (runtime.artifacts_root / "build/vector_inputs/manifest.json").exists()


def test_failed_vector_inputs_rebuild_preserves_previous_generation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        [
            {"time": _ts(0), "value": 1.0, "other": 10.0},
            {"time": _ts(1), "value": 2.0, "other": 20.0},
        ],
        stream_id="prices",
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[
            FeatureRecordConfig(stream="prices", id="value", field="value"),
            FeatureRecordConfig(stream="prices", id="other", field="other"),
        ],
    )
    task = VectorInputsTask()
    first = materialize_vector_inputs(runtime, task)
    manifest_path = runtime.artifacts_root / first.relative_path
    previous_manifest = manifest_path.read_bytes()
    previous = json.loads(previous_manifest)
    previous_shards = [
        manifest_path.parent / shard["path"] for shard in previous["features"]
    ]
    previous_generation = Path(previous["features"][0]["path"]).parts[1]

    write_rows = vector_inputs_operation.write_vector_input_rows
    writes = 0

    def fail_second_shard(path, rows):
        nonlocal writes
        writes += 1
        if writes == 2:
            raise RuntimeError("second shard failed")
        return write_rows(path, rows)

    monkeypatch.setattr(
        vector_inputs_operation,
        "write_vector_input_rows",
        fail_second_shard,
    )

    with pytest.raises(RuntimeError, match="second shard failed"):
        materialize_vector_inputs(runtime, task)

    assert manifest_path.read_bytes() == previous_manifest
    assert all(path.is_file() for path in previous_shards)
    cache_root = manifest_path.parent / "manifest.shards"
    assert [path.name for path in cache_root.iterdir()] == [previous_generation]


def test_failed_vector_inputs_manifest_commit_removes_new_generation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        [{"time": _ts(0), "value": 1.0}],
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[FeatureRecordConfig(stream="stream", id="value", field="value")],
    )
    task = VectorInputsTask()
    first = materialize_vector_inputs(runtime, task)
    manifest_path = runtime.artifacts_root / first.relative_path
    previous_manifest = manifest_path.read_bytes()
    previous = load_vector_inputs_manifest(manifest_path)
    previous_path = manifest_path.parent / previous.features[0].path

    def fail_manifest(*_args, **_kwargs):
        raise OSError("manifest commit failed")

    monkeypatch.setattr(
        vector_inputs_operation,
        "write_json_artifact",
        fail_manifest,
    )

    with pytest.raises(OSError, match="manifest commit failed"):
        materialize_vector_inputs(runtime, task)

    assert manifest_path.read_bytes() == previous_manifest
    previous_generation = previous_path.parent.parent
    assert set(previous_generation.parent.iterdir()) == {previous_generation}


def test_identical_vector_inputs_rebuild_publishes_a_new_generation(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        [{"time": _ts(0), "value": 1.0}],
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[FeatureRecordConfig(stream="stream", id="value", field="value")],
    )
    task = VectorInputsTask()

    first = materialize_vector_inputs(runtime, task)
    manifest_path = runtime.artifacts_root / first.relative_path
    first_manifest = load_vector_inputs_manifest(manifest_path)
    first_path = manifest_path.parent / first_manifest.features[0].path

    materialize_vector_inputs(runtime, task)
    second_manifest = load_vector_inputs_manifest(manifest_path)
    second_path = manifest_path.parent / second_manifest.features[0].path

    assert second_path != first_path
    assert first_path.is_file()
    assert len(list(open_vector_input_records(second_path))) == 1


def test_changed_vector_inputs_rebuild_retains_previous_generation(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        [{"time": _ts(0), "value": 1.0}],
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[FeatureRecordConfig(stream="stream", id="value", field="value")],
    )
    task = VectorInputsTask()

    first = materialize_vector_inputs(runtime, task)
    manifest_path = runtime.artifacts_root / first.relative_path
    first_manifest = load_vector_inputs_manifest(manifest_path)
    first_path = manifest_path.parent / first_manifest.features[0].path
    first_rows = list(open_vector_input_records(first_path))

    source = runtime.streams["stream"].source
    assert isinstance(source, _StubSource)
    source._rows.append({"time": _ts(1), "value": 2.0})
    materialize_vector_inputs(runtime, task)

    second_manifest = load_vector_inputs_manifest(manifest_path)
    second_path = manifest_path.parent / second_manifest.features[0].path
    assert second_path != first_path
    assert first_path.is_file()
    assert list(open_vector_input_records(first_path)) == first_rows
    assert len(list(open_vector_input_records(second_path))) == 2

    assert prune_vector_input_cache(manifest_path) == (first_path.parent.parent,)
    assert not first_path.exists()
    assert second_path.is_file()


def test_vector_inputs_rebuild_replaces_a_corrupt_generation(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        [{"time": _ts(0), "value": 1.0}],
    )
    runtime.dataset = FeatureDatasetConfig(
        sample=SampleConfig(cadence="1h"),
        features=[FeatureRecordConfig(stream="stream", id="value", field="value")],
    )
    task = VectorInputsTask()

    first = materialize_vector_inputs(runtime, task)
    manifest_path = runtime.artifacts_root / first.relative_path
    manifest = load_vector_inputs_manifest(manifest_path)
    shard_path = manifest_path.parent / manifest.features[0].path
    shard_path.write_bytes(b"corrupt")

    materialize_vector_inputs(runtime, task)

    rebuilt = load_vector_inputs_manifest(manifest_path)
    rebuilt_path = manifest_path.parent / rebuilt.features[0].path
    assert rebuilt_path != shard_path
    assert len(list(open_vector_input_records(rebuilt_path))) == 1
    removed = prune_vector_input_cache(manifest_path)
    assert removed == (shard_path.parent.parent,)


def test_vector_inputs_rejects_symlinked_output_before_mutation(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "artifacts"
    redirected = artifacts_root / "redirected"
    redirected.mkdir(parents=True)
    victim = redirected / "manifest.json"
    victim.write_text("keep", encoding="utf-8")
    (artifacts_root / "build").symlink_to(redirected, target_is_directory=True)
    runtime = SimpleNamespace(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=artifacts_root,
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
        streams={},
    )

    with pytest.raises(ValueError, match="must not resolve through a symlink"):
        materialize_vector_inputs(
            runtime,
            VectorInputsTask(output="build/manifest.json"),
        )

    assert victim.read_text(encoding="utf-8") == "keep"


def test_vector_pipeline_requires_vector_inputs_artifact(tmp_path: Path) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        rows=[{"time": _ts(0), "value": 1.0}],
    )
    context = PipelineContext(runtime)
    cfg = FeatureRecordConfig(stream="stream", id="price", field="value")

    with pytest.raises(RuntimeError, match="Vector inputs artifact is required"):
        list(build_vector_pipeline(context, [cfg], "1h", rectangular=False))


def test_cached_vector_pipeline_rejects_manifest_cadence_mismatch(
    tmp_path: Path,
) -> None:
    runtime = _runtime_with_rows(
        tmp_path,
        rows=[{"time": _ts(0), "value": 1.0}],
    )
    cfg = FeatureRecordConfig(stream="stream", id="price", field="value")
    register_vector_inputs(runtime, [cfg], "1h")
    manifest = runtime.artifacts_root / "build/vector_inputs/manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["cadence"] = "1d"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="cadence does not match"):
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
    value_cfg = FeatureRecordConfig(stream="stream", id="price", field="value")
    other_cfg = FeatureRecordConfig(stream="stream", id="other", field="other")
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
        FeatureRecordConfig(stream="stream", id="a", field="value"),
        FeatureRecordConfig(stream="stream", id="b", field="value"),
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
        "datapipeline.pipelines.vector.pipeline.open_vector_input_records",
        _open_records,
    )

    records = vector_pipeline._merged_cached_records(
        manifest_path=tmp_path / "manifest.json",
        shards=(
            CachedVectorInputShard(id="a", path="a.jsonl.gz", rows=2),
            CachedVectorInputShard(id="b", path="b.jsonl.gz", rows=2),
        ),
        configs=configs,
        group_by_cadence=parse_cadence("1h"),
        sample_key_contract=SampleKeyContract(()),
    )
    first = next(records)
    assert first.id == "a"
    assert closed_streams == []

    records.close()
    assert set(closed_streams) == {"a", "b"}
