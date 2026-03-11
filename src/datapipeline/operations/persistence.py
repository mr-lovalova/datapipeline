import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.io.factory import writer_factory
from datapipeline.io.output import (
    OutputTarget,
    materialized_output_message,
    served_output_message,
)
from datapipeline.operations.runtime.output_rows import row_from_record


@dataclass(frozen=True)
class ArtifactOutput:
    relative_path: str
    meta: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeOutput:
    rows: Iterable[Any] | None = None
    payload: Mapping[str, Any] | None = None
    html_renderer: Callable[[Path], Path | None] | None = None
    target: OutputTarget | None = None
    materialized_key: str | None = None
    materialized_meta: dict[str, object] | None = None


@dataclass(frozen=True)
class RuntimeOutputBatch:
    outputs: Sequence[RuntimeOutput]
    on_complete: Callable[[bool], None] | None = None


def persist_artifact_output(
    result: object,
    *,
    artifact_key: str,
    runtime,
    logger: logging.Logger,
    emit_message: Callable[..., None] = emit_execution_message,
) -> dict[str, object] | None:
    if result is None:
        return None
    if not isinstance(result, ArtifactOutput):
        raise TypeError("Build operation must return ArtifactOutput or None.")
    full_path = (runtime.artifacts_root / result.relative_path).resolve()
    emit_message(
        materialized_output_message(artifact_key, full_path, meta=dict(result.meta)),
        level=logging.INFO,
        logger=logger,
        message_kind="materialized",
    )
    artifacts = getattr(runtime, "artifacts", None)
    if artifacts is not None and hasattr(artifacts, "register"):
        artifacts.register(
            artifact_key,
            relative_path=result.relative_path,
            meta=dict(result.meta),
        )
    output: dict[str, object] = {"relative_path": result.relative_path}
    output.update(dict(result.meta))
    return output


def _persist_runtime_output(
    result: RuntimeOutput,
    *,
    target: OutputTarget | None,
    visuals: str | None,
    logger: logging.Logger,
    emit_message: Callable[..., None],
) -> None:
    effective_target = result.target or target
    if effective_target is None:
        raise ValueError("Runtime operation requires profile output target.")

    if effective_target.format == "html":
        if result.html_renderer is None:
            raise ValueError("html output is not supported for this operation.")
        destination = effective_target.destination
        if destination is None:
            raise ValueError("html output requires fs destination.")
        written = result.html_renderer(destination)
        if written is not None and result.materialized_key:
            emit_message(
                materialized_output_message(
                    result.materialized_key,
                    written,
                    meta=result.materialized_meta,
                ),
                level=logging.INFO,
                logger=logger,
                message_kind="materialized",
            )
        return

    rows = result.rows
    if rows is None:
        if result.payload is None:
            rows = ()
        elif effective_target.format == "txt":
            rows = (
                json.dumps(
                    dict(result.payload),
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                ),
            )
        else:
            rows = (dict(result.payload),)

    writer = writer_factory(effective_target, visuals=visuals)
    count = 0
    try:
        for row in rows:
            writer.write(row_from_record(row))
            count += 1
    finally:
        writer.close()

    emit_message(
        served_output_message(effective_target, count),
        level=logging.INFO,
        logger=logger,
        message_kind="saved",
    )


def persist_runtime_result(
    result: object,
    *,
    target: OutputTarget | None,
    visuals: str | None,
    logger: logging.Logger,
    emit_message: Callable[..., None] = emit_execution_message,
) -> None:
    if result is None:
        return
    if not isinstance(result, RuntimeOutput | RuntimeOutputBatch):
        raise TypeError(
            "Runtime operation must return RuntimeOutput, RuntimeOutputBatch, or None."
        )
    if isinstance(result, RuntimeOutputBatch):
        success = False
        try:
            for output in result.outputs:
                _persist_runtime_output(
                    output,
                    target=target,
                    visuals=visuals,
                    logger=logger,
                    emit_message=emit_message,
                )
            success = True
        finally:
            if result.on_complete is not None:
                result.on_complete(success)
        return

    _persist_runtime_output(
        result,
        target=target,
        visuals=visuals,
        logger=logger,
        emit_message=emit_message,
    )


__all__ = [
    "ArtifactOutput",
    "RuntimeOutput",
    "RuntimeOutputBatch",
    "persist_artifact_output",
    "persist_runtime_result",
]
