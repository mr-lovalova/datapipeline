from datapipeline.execution.context import PipelineContext
from datapipeline.execution.observer import NoopPipelineObserver
from datapipeline.domain.record import TemporalRecord
from datapipeline.operations.runtime.pipeline import _record_preview_stream
from datapipeline.pipelines.stream.pipeline import (
    build_stream_pipeline,
    run_stream_pipeline,
)
from datapipeline.plugins import MAPPERS_EP
from datapipeline.runtime import IngestRuntimeStream
from datapipeline.services.pipeline import load_pipeline
from datapipeline.services.runtime_compiler import compile_runtime
from datapipeline.sources.models.source import Source


class _PipelineObserver(NoopPipelineObserver):
    def __init__(self) -> None:
        self.started: list[str] = []

    def on_pipeline_start(
        self,
        pipeline_name: str,
        node_count: int,
        summary: str | None = None,
    ) -> None:
        self.started.append(pipeline_name)


def _write_test_project(tmp_path):
    sources_dir = tmp_path / "sources"
    ingests_dir = tmp_path / "ingests"
    streams_dir = tmp_path / "streams"
    data_dir = tmp_path / "data"
    for directory in (sources_dir, ingests_dir, streams_dir, data_dir):
        directory.mkdir()

    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        """\
version: 1
artifact_revision: 1
name: runtime-compiler-test
paths:
  sources: sources
  ingests: ingests
  streams: streams
  dataset: dataset.yaml
  artifacts: artifacts
""",
        encoding="utf-8",
    )
    (tmp_path / "dataset.yaml").write_text(
        "sample:\n  cadence: 1h\nfeatures: []\ntargets: []\n",
        encoding="utf-8",
    )
    return project_yaml, sources_dir, ingests_dir, streams_dir, data_dir


def test_ingests_sharing_a_source_compile_distinct_runtime_objects(tmp_path) -> None:
    project_yaml, sources_dir, ingests_dir, _, _ = _write_test_project(tmp_path)
    (sources_dir / "shared.yaml").write_text(
        """\
id: shared
parser:
  entrypoint: core.temporal_record
loader:
  entrypoint: core.io
  args:
    transport: fs
    format: jsonl
    path: data.jsonl
""",
        encoding="utf-8",
    )
    for ingest_id in ("left", "right"):
        (ingests_dir / f"{ingest_id}.yaml").write_text(
            f"""\
id: {ingest_id}
from:
  source: shared
map:
  entrypoint: identity
""",
            encoding="utf-8",
        )

    runtime = compile_runtime(load_pipeline(project_yaml))
    left = runtime.streams["left"]
    right = runtime.streams["right"]

    assert isinstance(left, IngestRuntimeStream)
    assert isinstance(right, IngestRuntimeStream)
    assert isinstance(left.source, Source)
    assert isinstance(right.source, Source)
    assert left.source is not right.source
    assert left.source.loader is not right.source.loader
    assert left.source.parser is not right.source.parser


def test_yaml_aligned_stream_runs_with_inherited_partition_and_combiner(
    tmp_path,
    monkeypatch,
) -> None:
    project_yaml, sources_dir, ingests_dir, streams_dir, data_dir = _write_test_project(
        tmp_path
    )

    records_by_input = {
        "left": [
            '{"time":"2025-01-02T00:00:00Z","ticker":"A","value":2}',
            '{"time":"2025-01-01T00:00:00Z","ticker":"A","value":1}',
        ],
        "right": [
            '{"time":"2025-01-01T00:00:00Z","ticker":"A","value":10}',
            '{"time":"2025-01-02T00:00:00Z","ticker":"A","value":20}',
        ],
    }
    for input_name, rows in records_by_input.items():
        (data_dir / f"{input_name}.jsonl").write_text(
            "\n".join(rows) + "\n",
            encoding="utf-8",
        )
        (sources_dir / f"{input_name}.yaml").write_text(
            f"""\
id: {input_name}.source
parser:
  entrypoint: core.temporal_record
loader:
  entrypoint: core.io
  args:
    transport: fs
    format: jsonl
    path: data/{input_name}.jsonl
""",
            encoding="utf-8",
        )
        (ingests_dir / f"{input_name}.yaml").write_text(
            f"""\
id: {input_name}
from:
  source: {input_name}.source
partition_by: [ticker]
map:
  entrypoint: identity
""",
            encoding="utf-8",
        )

    (streams_dir / "combined.yaml").write_text(
        """\
id: combined
from:
  align:
    - left
    - right
combine:
  entrypoint: combine
  args:
    offset: 5
""",
        encoding="utf-8",
    )

    def combine(left, right, offset):
        record = TemporalRecord(time=left.time)
        record.ticker = left.ticker
        record.value = left.value * 100 + right.value + offset
        return record

    def load_mapper(group, entrypoint):
        assert group == MAPPERS_EP
        assert entrypoint == "combine"
        return combine

    monkeypatch.setattr(
        "datapipeline.services.streams.aligned.load_ep",
        load_mapper,
    )

    runtime = compile_runtime(load_pipeline(project_yaml))
    context = PipelineContext(runtime)
    pipeline = build_stream_pipeline(context, "combined")
    assert pipeline.name == "stream:combined"
    assert [node.name for node in pipeline.nodes] == [
        "align_inputs",
        "combine_records",
        "order_records",
    ]
    assert [node.progress is not None for node in pipeline.nodes] == [
        False,
        False,
        True,
    ]
    preview = list(_record_preview_stream(context, "combined", "mapped"))
    assert [record.value for record in preview] == [115, 225]

    observer = _PipelineObserver()
    runtime.pipeline_observer = observer

    assert runtime.streams["combined"].partition_by == ("ticker",)
    records = list(run_stream_pipeline(PipelineContext(runtime), "combined"))
    assert [(record.time.day, record.ticker, record.value) for record in records] == [
        (1, "A", 115),
        (2, "A", 225),
    ]
    assert observer.started == ["stream:combined"]
