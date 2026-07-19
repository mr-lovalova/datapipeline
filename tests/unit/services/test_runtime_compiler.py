from datapipeline.execution.context import PipelineContext
from datapipeline.execution.events import PipelineEvent, PipelineStarted
from datapipeline.domain.record import TemporalRecord
from datapipeline.operations.runtime.pipeline import _record_preview_stream
from datapipeline.pipelines.stream.pipeline import (
    build_stream_pipeline,
    run_stream_pipeline,
)
from datapipeline.plugins import COMBINERS_EP
from datapipeline.runtime import (
    BroadcastRuntimeStream,
    DerivedRuntimeStream,
    SourceRuntimeStream,
)
from datapipeline.services.pipeline import load_pipeline
from datapipeline.services.runtime_compiler import compile_runtime
from datapipeline.sources.models.source import Source


class _PipelineObserver:
    def __init__(self) -> None:
        self.started: list[str] = []

    def __call__(self, event: PipelineEvent) -> None:
        if isinstance(event, PipelineStarted):
            self.started.append(event.pipeline_name)


def _write_test_project(tmp_path):
    sources_dir = tmp_path / "sources"
    streams_dir = tmp_path / "streams"
    data_dir = tmp_path / "data"
    for directory in (sources_dir, streams_dir, data_dir):
        directory.mkdir()

    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        """\
schema_version: 2
artifact_revision: 1
name: runtime-compiler-test
paths:
  sources: sources
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
    return project_yaml, sources_dir, streams_dir, data_dir


def test_streams_sharing_a_source_compile_distinct_runtime_objects(tmp_path) -> None:
    project_yaml, sources_dir, streams_dir, _ = _write_test_project(tmp_path)
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
    for stream_id in ("left", "right"):
        (streams_dir / f"{stream_id}.yaml").write_text(
            f"""\
id: {stream_id}
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

    assert isinstance(left, SourceRuntimeStream)
    assert isinstance(right, SourceRuntimeStream)
    assert isinstance(left.source, Source)
    assert isinstance(right.source, Source)
    assert left.source is not right.source
    assert left.source.loader is not right.source.loader
    assert left.source.parser is not right.source.parser


def test_yaml_derived_stream_runs_with_inherited_partition(tmp_path) -> None:
    project_yaml, sources_dir, streams_dir, data_dir = _write_test_project(tmp_path)
    (data_dir / "prices.jsonl").write_text(
        "\n".join(
            [
                '{"time":"2025-01-02T00:00:00Z","ticker":"A","value":2}',
                '{"time":"2025-01-01T00:00:00Z","ticker":"A","value":1}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (sources_dir / "prices.yaml").write_text(
        """\
id: prices.source
parser:
  entrypoint: core.temporal_record
loader:
  entrypoint: core.io
  args:
    transport: fs
    format: jsonl
    path: data/prices.jsonl
""",
        encoding="utf-8",
    )
    (streams_dir / "prices.yaml").write_text(
        """\
id: prices
from:
  source: prices.source
partition_by: [ticker]
map:
  entrypoint: identity
""",
        encoding="utf-8",
    )
    (streams_dir / "filtered.yaml").write_text(
        """\
id: filtered
from:
  stream: prices
transforms:
  - {operation: where, field: value, operator: gt, comparand: 1}
""",
        encoding="utf-8",
    )

    runtime = compile_runtime(load_pipeline(project_yaml))
    derived = runtime.streams["filtered"]

    assert isinstance(derived, DerivedRuntimeStream)
    assert derived.partition_by == ("ticker",)
    records = list(run_stream_pipeline(PipelineContext(runtime), "filtered"))
    assert [(record.time.day, record.ticker, record.value) for record in records] == [
        (2, "A", 2)
    ]


def test_yaml_broadcast_stream_reuses_exact_input_across_partitions(
    tmp_path,
    monkeypatch,
) -> None:
    project_yaml, sources_dir, streams_dir, data_dir = _write_test_project(tmp_path)
    rows_by_input = {
        "measurements": [
            '{"time":"2025-01-02T00:00:00Z","station":"A","value":2}',
            '{"time":"2025-01-01T00:00:00Z","station":"B","value":3}',
            '{"time":"2025-01-01T00:00:00Z","station":"A","value":1}',
            '{"time":"2025-01-02T00:00:00Z","station":"B","value":4}',
        ],
        "reference": [
            '{"time":"2025-01-02T00:00:00Z","value":20}',
            '{"time":"2025-01-01T00:00:00Z","value":10}',
        ],
    }
    for input_name, rows in rows_by_input.items():
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
    (streams_dir / "measurements.yaml").write_text(
        """\
id: measurements
from: {source: measurements.source}
partition_by: [station]
map: {entrypoint: identity}
""",
        encoding="utf-8",
    )
    (streams_dir / "reference.yaml").write_text(
        """\
id: reference
from: {source: reference.source}
map: {entrypoint: identity}
""",
        encoding="utf-8",
    )
    (streams_dir / "enriched.yaml").write_text(
        """\
id: enriched
from:
  stream: measurements
  broadcast: reference
combine:
  entrypoint: attach_reference
  args: {offset: 2}
transforms:
  - {operation: derive, left: value, operator: mul, right_value: 2, to: doubled}
""",
        encoding="utf-8",
    )

    def attach_reference(measurement, reference, offset):
        record = TemporalRecord(time=measurement.time)
        record.station = measurement.station
        record.value = measurement.value + reference.value + offset
        return record

    monkeypatch.setattr(
        "datapipeline.services.streams.combine.load_ep",
        lambda group, entrypoint: attach_reference,
    )

    runtime = compile_runtime(load_pipeline(project_yaml))
    enriched = runtime.streams["enriched"]

    assert isinstance(enriched, BroadcastRuntimeStream)
    assert enriched.input_stream == "measurements"
    assert enriched.broadcast_stream == "reference"
    assert enriched.partition_by == ("station",)
    assert len(enriched.transforms) == 1

    records = list(run_stream_pipeline(PipelineContext(runtime), "enriched"))
    assert [
        (record.station, record.time.day, record.value, record.doubled)
        for record in records
    ] == [
        ("A", 1, 13, 26),
        ("A", 2, 24, 48),
        ("B", 1, 15, 30),
        ("B", 2, 26, 52),
    ]


def test_yaml_aligned_stream_runs_with_inherited_partition_and_combiner(
    tmp_path,
    monkeypatch,
) -> None:
    project_yaml, sources_dir, streams_dir, data_dir = _write_test_project(tmp_path)

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
        (streams_dir / f"{input_name}.yaml").write_text(
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
        assert group == COMBINERS_EP
        assert entrypoint == "combine"
        return combine

    monkeypatch.setattr(
        "datapipeline.services.streams.combine.load_ep",
        load_mapper,
    )

    runtime = compile_runtime(load_pipeline(project_yaml))
    context = PipelineContext(runtime)
    pipeline = build_stream_pipeline(context, "combined")
    assert pipeline.name == "stream:combined"
    assert [node.name for node in pipeline.nodes] == [
        "align_inputs",
        "combine_records",
    ]
    assert [node.progress is not None for node in pipeline.nodes] == [
        False,
        False,
    ]
    input_rows = list(_record_preview_stream(context, "combined", "input"))
    assert [[record.value for record in row] for row in input_rows] == [
        [1, 10],
        [2, 20],
    ]
    canonical = list(_record_preview_stream(context, "combined", "canonical"))
    assert [record.value for record in canonical] == [115, 225]

    observer = _PipelineObserver()
    runtime.pipeline_observer = observer

    assert runtime.streams["combined"].partition_by == ("ticker",)
    records = list(run_stream_pipeline(PipelineContext(runtime), "combined"))
    assert [(record.time.day, record.ticker, record.value) for record in records] == [
        (1, "A", 115),
        (2, "A", 225),
    ]
    assert observer.started == ["stream:combined"]
