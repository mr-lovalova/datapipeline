from collections.abc import Iterable
from pathlib import Path
from typing import Any

from datapipeline.execution.context import PipelineContext
from datapipeline.io.writers.jsonl import JsonLinesFileWriter
from datapipeline.pipelines.stream.pipeline import run_stream_pipeline
from datapipeline.runtime import Runtime


def materialize_stream_to_path(
    runtime: Runtime,
    stream_id: str,
    output: Path,
    overwrite: bool = False,
) -> Path:
    output_path = materialize_destination_path(output)
    artifacts_root = runtime.artifacts_root.resolve()
    if output_path.is_relative_to(artifacts_root):
        raise ValueError(
            f"materialize output must be outside the managed artifacts root: {output_path}"
        )
    check_materialize_destination(output_path, overwrite)

    rows = run_stream_pipeline(PipelineContext(runtime), stream_id)
    _write_rows(
        rows,
        output_path,
        overwrite,
    )
    return output_path


def materialize_destination_path(output: Path) -> Path:
    validate_materialize_output_path(output)
    return output.resolve()


def check_materialize_destination(
    path: Path,
    overwrite: bool,
) -> None:
    if not overwrite and path.exists():
        raise FileExistsError(f"{path} already exists; pass --overwrite to replace it")


def _write_rows(
    rows: Iterable[Any],
    output: Path,
    overwrite: bool,
) -> None:
    writer = JsonLinesFileWriter(output, overwrite=overwrite)
    try:
        for row in rows:
            writer.write(row)
        writer.close()
    except BaseException:
        writer.abort()
        raise


def validate_materialize_output_path(path: Path) -> None:
    if path.suffix != ".jsonl":
        raise ValueError("materialize output must use a .jsonl path")
