from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import os

import yaml

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.dag.runner import run_dag
from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget
from datapipeline.io.sinks import AtomicTextFileSink
from datapipeline.pipelines.ingest import build_ingest_pipeline
from datapipeline.pipelines.shared.record_nodes import required_record_order
from datapipeline.pipelines.stream import build_stream_dag
from datapipeline.runtime import Runtime
from datapipeline.services.project_paths import ingests_dir, sources_dir


@dataclass(frozen=True)
class MaterializeResult:
    count: int
    output: Path
    metadata: Path
    source_config: Path | None = None
    ingest_config: Path | None = None


@dataclass(frozen=True)
class MaterializeDestinationPaths:
    output: Path
    metadata: Path
    source_config: Path | None = None
    ingest_config: Path | None = None

    @property
    def paths(self) -> tuple[Path, ...]:
        optional = (self.source_config, self.ingest_config)
        return (self.output, self.metadata) + tuple(
            path for path in optional if path is not None
        )


def materialize_stream_to_path(
    *,
    runtime: Runtime,
    stream_id: str,
    output: Path,
    as_stream_id: str | None = None,
    overwrite: bool = False,
    dataset: FeatureDatasetConfig | None = None,
) -> MaterializeResult:
    if dataset is not None:
        runtime.sample_keys = dataset.sample_keys
    destinations = materialize_destination_paths(runtime, output, as_stream_id)
    check_materialize_destinations(destinations, overwrite)
    raw_partition_by = runtime.registries.partition_by.get(stream_id)
    ordered_by = required_record_order(raw_partition_by)
    partition_by = _field_list(raw_partition_by)
    feature_id_by = _field_list(runtime.registries.feature_id_by.get(stream_id))

    rows = materialized_stream_rows(runtime, stream_id)
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=destinations.output,
        overwrite=overwrite,
    )
    count = write_rows(rows, target)
    metadata_path = write_materialized_stream_metadata(
        output=destinations.output,
        rows=count,
        partition_by=partition_by,
        feature_id_by=feature_id_by,
        ordered_by=ordered_by,
        overwrite=overwrite,
    )

    source_config = None
    ingest_config = None
    if as_stream_id is not None:
        source_config, ingest_config = write_materialized_stream_config(
            runtime=runtime,
            stream_id=as_stream_id,
            source_id=f"{as_stream_id}.source",
            output=destinations.output,
            partition_by=partition_by,
            feature_id_by=feature_id_by,
            ordered_by=ordered_by,
            overwrite=overwrite,
        )

    return MaterializeResult(
        count=count,
        output=destinations.output,
        metadata=metadata_path,
        source_config=source_config,
        ingest_config=ingest_config,
    )


def materialized_stream_rows(runtime: Runtime, stream_id: str) -> Iterator[Any]:
    context = PipelineContext(runtime)
    pipeline = runtime.registries.stream_specs.get(stream_id).pipeline
    if pipeline == "ingest":
        return build_ingest_pipeline(context, stream_id)
    return run_dag(
        context,
        build_stream_dag(context, stream_id).upto_node_named("stream_transforms"),
    )


def _field_list(value: str | list[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def materialized_metadata_path(output: Path) -> Path:
    return output.with_suffix(".metadata.json")


def materialize_destination_paths(
    runtime: Runtime,
    output: Path,
    as_stream_id: str | None = None,
) -> MaterializeDestinationPaths:
    validate_materialize_output_path(output)
    resolved_output = output.resolve()
    if as_stream_id is None:
        return MaterializeDestinationPaths(
            output=resolved_output,
            metadata=materialized_metadata_path(resolved_output),
        )
    source_config, ingest_config = materialized_stream_config_paths(
        runtime=runtime,
        stream_id=as_stream_id,
        source_id=f"{as_stream_id}.source",
    )
    return MaterializeDestinationPaths(
        output=resolved_output,
        metadata=materialized_metadata_path(resolved_output),
        source_config=source_config.resolve(),
        ingest_config=ingest_config.resolve(),
    )


def check_materialize_destinations(
    destinations: MaterializeDestinationPaths,
    overwrite: bool,
) -> None:
    if overwrite:
        return
    for path in destinations.paths:
        if path.exists():
            raise FileExistsError(
                f"{path} already exists; pass --overwrite to replace it"
            )


def write_materialized_stream_metadata(
    *,
    output: Path,
    rows: int,
    partition_by: list[str] | None,
    feature_id_by: list[str] | None,
    ordered_by: list[str],
    overwrite: bool,
) -> Path:
    path = materialized_metadata_path(output)
    doc = {
        "rows": rows,
        "format": "jsonl",
        "encoding": "utf-8",
        "partition_by": partition_by,
        "feature_id_by": feature_id_by,
        "ordered_by": ordered_by,
    }
    _write_text_atomically(
        path,
        json.dumps(doc, indent=2, ensure_ascii=False) + "\n",
        overwrite=overwrite,
    )
    return path.resolve()


def write_rows(
    rows: Iterable[Any],
    target: OutputTarget,
) -> int:
    writer = writer_factory(target)
    count = 0
    success = False
    try:
        for row in rows:
            writer.write(row)
            count += 1
        writer.close()
        success = True
        return count
    finally:
        if not success:
            writer.abort()


def write_materialized_stream_config(
    *,
    runtime: Runtime,
    stream_id: str,
    source_id: str,
    output: Path,
    partition_by: list[str] | None,
    feature_id_by: list[str] | None,
    ordered_by: list[str],
    overwrite: bool,
) -> tuple[Path, Path]:
    source_path, ingest_path = materialized_stream_config_paths(
        runtime=runtime,
        stream_id=stream_id,
        source_id=source_id,
    )
    source_doc = {
        "id": source_id,
        "parser": {"entrypoint": "core.temporal_record", "args": {}},
        "loader": {
            "entrypoint": "core.io",
            "args": {
                "transport": "fs",
                "format": "jsonl",
                "path": _project_relative_path(output, runtime.project_yaml),
                "encoding": "utf-8",
            },
        },
    }
    ingest_doc = {
        "id": stream_id,
        "from": {"source": source_id},
        "map": {"entrypoint": "identity", "args": {}},
        "ordered_by": ordered_by,
    }
    if partition_by is not None:
        ingest_doc["partition_by"] = partition_by
    if feature_id_by is not None:
        ingest_doc["feature_id_by"] = feature_id_by
    _write_text_atomically(
        source_path,
        yaml.safe_dump(source_doc, sort_keys=False),
        overwrite=overwrite,
    )
    _write_text_atomically(
        ingest_path,
        yaml.safe_dump(ingest_doc, sort_keys=False),
        overwrite=overwrite,
    )
    return source_path.resolve(), ingest_path.resolve()


def materialized_stream_config_paths(
    runtime: Runtime,
    stream_id: str,
    source_id: str,
) -> tuple[Path, Path]:
    return (
        sources_dir(runtime.project_yaml) / f"{source_id}.yaml",
        ingests_dir(runtime.project_yaml) / f"{stream_id}.yaml",
    )


def _project_relative_path(path: Path, project_yaml: Path) -> str:
    return os.path.relpath(path.resolve(), start=project_yaml.parent.resolve())


def validate_materialize_output_path(path: Path) -> None:
    if path.suffix != ".jsonl":
        raise ValueError("materialize stream output must use a .jsonl path")


def _write_text_atomically(path: Path, text: str, *, overwrite: bool) -> None:
    sink = AtomicTextFileSink(path, overwrite=overwrite)
    committed = False
    try:
        sink.write_text(text)
        sink.close()
        committed = True
    finally:
        if not committed:
            sink.abort()
