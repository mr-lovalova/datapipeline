from datapipeline.execution.context import PipelineContext
from datapipeline.execution.observer import NoopPipelineObserver
from datapipeline.domain.record import TemporalRecord
from datapipeline.pipelines.stream.pipeline import run_stream_pipeline
from datapipeline.plugins import MAPPERS_EP
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap.core import init_streams, load_streams


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


def test_yaml_aligned_stream_runs_with_inherited_partition_and_positional_mapper(
    tmp_path,
    monkeypatch,
) -> None:
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
name: aligned-test
paths:
  sources: sources
  ingests: ingests
  streams: streams
  dataset: dataset.yaml
  postprocess: postprocess.yaml
  artifacts: artifacts
""",
        encoding="utf-8",
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
partition_by: ticker
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
map:
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

    runtime = Runtime(project_yaml, tmp_path / "artifacts")
    init_streams(load_streams(project_yaml), runtime)
    observer = _PipelineObserver()
    runtime.pipeline_observer = observer

    assert runtime.streams["combined"].partition_by == "ticker"
    records = list(run_stream_pipeline(PipelineContext(runtime), "combined"))
    assert [(record.time.day, record.ticker, record.value) for record in records] == [
        (1, "A", 115),
        (2, "A", 225),
    ]
    assert observer.started == ["stream:combined"]
