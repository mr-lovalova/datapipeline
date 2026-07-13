from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json

from datapipeline.execution.context import PipelineContext
from datapipeline.domain.stream import canonical_record_order
from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget
from datapipeline.io.sinks import AtomicTextFileSink
from datapipeline.pipelines.stream.pipeline import run_stream_pipeline
from datapipeline.runtime import Runtime, require_runtime_stream


@dataclass(frozen=True)
class MaterializeResult:
    count: int
    output: Path
    metadata: Path


def materialize_stream_to_path(
    *,
    runtime: Runtime,
    stream_id: str,
    output: Path,
    overwrite: bool = False,
) -> MaterializeResult:
    destinations = materialize_destination_paths(output)
    artifacts_root = runtime.artifacts_root.resolve()
    for path in destinations:
        if path.is_relative_to(artifacts_root):
            raise ValueError(
                f"materialize output must be outside the managed artifacts root: {path}"
            )
    check_materialize_destinations(destinations, overwrite)
    output_path = destinations[0]
    stream = require_runtime_stream(runtime, stream_id)
    ordered_by = canonical_record_order(stream.partition_by)
    partition_by = _field_list(stream.partition_by)
    feature_id_by = _field_list(stream.feature_id_by)

    rows = materialized_stream_rows(runtime, stream_id)
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=output_path,
        overwrite=overwrite,
    )
    count = write_rows(rows, target)
    metadata_path = write_materialized_stream_metadata(
        output=output_path,
        rows=count,
        partition_by=partition_by,
        feature_id_by=feature_id_by,
        ordered_by=ordered_by,
        overwrite=overwrite,
    )

    return MaterializeResult(
        count=count,
        output=output_path,
        metadata=metadata_path,
    )


def materialized_stream_rows(runtime: Runtime, stream_id: str) -> Iterator[Any]:
    context = PipelineContext(runtime)
    return run_stream_pipeline(context, stream_id)


def _field_list(value: str | list[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def materialized_metadata_path(output: Path) -> Path:
    return output.with_suffix(".metadata.json")


def materialize_destination_paths(output: Path) -> tuple[Path, Path]:
    validate_materialize_output_path(output)
    resolved_output = output.resolve()
    return (
        resolved_output,
        materialized_metadata_path(resolved_output),
    )


def check_materialize_destinations(
    destinations: Iterable[Path],
    overwrite: bool,
) -> None:
    if overwrite:
        return
    for path in destinations:
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


def validate_materialize_output_path(path: Path) -> None:
    if path.suffix != ".jsonl":
        raise ValueError("materialize output must use a .jsonl path")


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
