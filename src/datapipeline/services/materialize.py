from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import os

import yaml

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


def materialize_stream_to_path(
    *,
    runtime: Runtime,
    stream_id: str,
    output: Path,
    as_stream_id: str | None = None,
    force: bool = False,
    dataset: Any | None = None,
) -> MaterializeResult:
    if dataset is not None:
        runtime.sample_keys = dataset.sample_keys
    validate_materialize_output_path(output)
    ordered_by = materialized_order(runtime, stream_id)
    metadata_path = materialized_metadata_path(output)
    _check_overwrite(output, force=force)
    _check_overwrite(metadata_path, force=force)
    if as_stream_id is not None:
        source_path, ingest_path = materialized_stream_config_paths(
            runtime=runtime,
            stream_id=as_stream_id,
            source_id=f"{as_stream_id}.source",
        )
        _check_overwrite(source_path, force=force)
        _check_overwrite(ingest_path, force=force)

    rows = materialized_stream_rows(runtime, stream_id)
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=output.resolve(),
        overwrite=force,
    )
    count = write_rows(rows, target)
    metadata_path = write_materialized_stream_metadata(
        output=output,
        rows=count,
        partition_by=materialized_partition_by(runtime, stream_id),
        feature_id_by=materialized_feature_id_by(runtime, stream_id),
        ordered_by=ordered_by,
        force=force,
    )

    source_config = None
    ingest_config = None
    if as_stream_id is not None:
        source_config, ingest_config = write_materialized_stream_config(
            runtime=runtime,
            stream_id=as_stream_id,
            source_id=f"{as_stream_id}.source",
            output=output,
            partition_by=materialized_partition_by(runtime, stream_id),
            feature_id_by=materialized_feature_id_by(runtime, stream_id),
            ordered_by=ordered_by,
            force=force,
        )

    return MaterializeResult(
        count=count,
        output=output.resolve(),
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


def materialized_order(runtime: Runtime, stream_id: str) -> list[str]:
    partition_by = runtime.registries.partition_by.get(stream_id)
    return required_record_order(partition_by)


def materialized_partition_by(runtime: Runtime, stream_id: str) -> list[str] | None:
    partition_by = runtime.registries.partition_by.get(stream_id)
    return _field_list(partition_by)


def materialized_feature_id_by(runtime: Runtime, stream_id: str) -> list[str] | None:
    feature_id_by = runtime.registries.feature_id_by.get(stream_id)
    return _field_list(feature_id_by)


def _field_list(value: str | list[str] | None) -> list[str] | None:
    if value == []:
        return []
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def _write_field_list(doc: dict[str, Any], key: str, value: list[str] | None) -> None:
    if value is not None:
        doc[key] = value


def materialized_metadata_path(output: Path) -> Path:
    return output.with_suffix(".metadata.json")


def write_materialized_stream_metadata(
    *,
    output: Path,
    rows: int,
    partition_by: list[str] | None,
    feature_id_by: list[str] | None,
    ordered_by: list[str],
    force: bool,
) -> Path:
    path = materialized_metadata_path(output)
    _check_overwrite(path, force=force)
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
        overwrite=force,
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
    force: bool,
) -> tuple[Path, Path]:
    source_path, ingest_path = materialized_stream_config_paths(
        runtime=runtime,
        stream_id=stream_id,
        source_id=source_id,
    )
    _check_overwrite(source_path, force=force)
    _check_overwrite(ingest_path, force=force)

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
    _write_field_list(ingest_doc, "partition_by", partition_by)
    _write_field_list(ingest_doc, "feature_id_by", feature_id_by)
    _write_text_atomically(
        source_path,
        yaml.safe_dump(source_doc, sort_keys=False),
        overwrite=force,
    )
    _write_text_atomically(
        ingest_path,
        yaml.safe_dump(ingest_doc, sort_keys=False),
        overwrite=force,
    )
    return source_path.resolve(), ingest_path.resolve()


def materialized_stream_config_paths(
    *,
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


def _check_overwrite(path: Path, *, force: bool) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"{path} already exists; pass --force to overwrite")


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
