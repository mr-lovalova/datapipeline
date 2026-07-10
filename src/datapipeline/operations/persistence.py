import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from datapipeline.execution.observability import (
    OperationProgress,
    emit_operation_info,
)
from datapipeline.dag.runner import resolve_heartbeat_interval_seconds
from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget


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
class SplitRuntimeOutput:
    rows: Iterable[Any]
    targets: Mapping[str, OutputTarget]
    label_for_row: Callable[[Any], str]
    limit_per_target: int | None = None


@dataclass(frozen=True)
class RuntimeOutputBatch:
    outputs: Sequence[RuntimeOutput | SplitRuntimeOutput]
    on_complete: Callable[[bool], None] | None = None


def persist_artifact_output(
    result: object,
    *,
    artifact_key: str,
    expected_relative_path: str | None = None,
    runtime,
    logger: logging.Logger,
) -> ArtifactOutput | None:
    if result is None:
        return None
    if not isinstance(result, ArtifactOutput):
        raise TypeError("Build operation must return ArtifactOutput or None.")
    if expected_relative_path is not None and Path(result.relative_path) != Path(
        expected_relative_path
    ):
        raise ValueError(
            f"Artifact '{artifact_key}' returned path '{result.relative_path}', "
            f"but its task declares '{expected_relative_path}'."
        )
    artifacts_root = Path(runtime.artifacts_root).resolve()
    full_path = (artifacts_root / result.relative_path).resolve()
    try:
        full_path.relative_to(artifacts_root)
    except ValueError as exc:
        raise ValueError(
            f"Artifact '{artifact_key}' output must stay under {artifacts_root}."
        ) from exc
    if not full_path.is_file():
        raise RuntimeError(
            f"Artifact '{artifact_key}' did not create its declared output: {full_path}."
        )
    meta = dict(result.meta)
    line = f"materialized path={full_path}"
    if meta:
        line = f"{line} " + " ".join(f"{key}={value}" for key, value in meta.items())
    emit_operation_info(line)
    artifacts = getattr(runtime, "artifacts", None)
    if artifacts is not None and hasattr(artifacts, "register"):
        artifacts.register(
            artifact_key,
            relative_path=result.relative_path,
            meta=meta,
        )
    return result


def _persist_runtime_output(
    result: RuntimeOutput,
    *,
    target: OutputTarget | None,
    visuals: str | None,
    heartbeat_interval_seconds: float | None,
    logger: logging.Logger,
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
            line = f"materialized path={written}"
            if result.materialized_meta:
                line = f"{line} " + " ".join(
                    f"{key}={value}" for key, value in result.materialized_meta.items()
                )
            emit_operation_info(line)
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
    progress = OperationProgress(
        "write_output",
        resolve_heartbeat_interval_seconds(heartbeat_interval_seconds),
    )
    count = 0
    success = False
    try:
        for row in rows:
            writer.write(row)
            count += 1
            progress.advance()
        writer.close()
        success = True
    finally:
        if not success:
            try:
                writer.abort()
            except Exception:
                logger.debug("Failed to abort runtime output writer", exc_info=True)

    if effective_target.destination:
        emit_operation_info(
            f"saved path={effective_target.destination} items={count}",
        )
    elif effective_target.transport == "stdout":
        emit_operation_info(f"streamed target=stdout items={count}")
    else:
        emit_operation_info(f"emitted items={count}")


def _persist_split_runtime_output(
    result: SplitRuntimeOutput,
    *,
    visuals: str | None,
    heartbeat_interval_seconds: float | None,
    logger: logging.Logger,
) -> None:
    if not result.targets:
        raise ValueError("Split runtime output requires at least one target.")

    writers = {}
    counts: dict[str, int] = {}
    rows = iter(result.rows)
    progress = OperationProgress(
        "write_output",
        resolve_heartbeat_interval_seconds(heartbeat_interval_seconds),
    )
    success = False
    try:
        for label, target in result.targets.items():
            if target.transport != "fs":
                raise ValueError("Split runtime output requires fs targets.")
            writers[label] = writer_factory(target, visuals=visuals)
            counts[label] = 0

        for row in rows:
            label = result.label_for_row(row)
            writer = writers.get(label)
            if writer is None:
                continue
            if (
                result.limit_per_target is not None
                and counts[label] >= result.limit_per_target
            ):
                if all(count >= result.limit_per_target for count in counts.values()):
                    break
                continue
            writer.write(row)
            counts[label] += 1
            progress.advance()
            if result.limit_per_target is not None and all(
                count >= result.limit_per_target for count in counts.values()
            ):
                break

        for writer in writers.values():
            writer.close()
        success = True
    finally:
        close_rows = getattr(rows, "close", None)
        if callable(close_rows):
            try:
                close_rows()
            except Exception:
                logger.debug("Failed to close split runtime rows", exc_info=True)
        if not success:
            for writer in writers.values():
                try:
                    writer.abort()
                except Exception:
                    logger.debug(
                        "Failed to abort split runtime output writer",
                        exc_info=True,
                    )

    for label, target in result.targets.items():
        if target.destination:
            line = (
                f"saved label={label} path={target.destination} items={counts[label]}"
            )
        elif target.transport == "stdout":
            line = f"streamed target=stdout items={counts[label]}"
        else:
            line = f"emitted items={counts[label]}"
        emit_operation_info(line)


def persist_runtime_result(
    result: object,
    *,
    target: OutputTarget | None,
    visuals: str | None,
    heartbeat_interval_seconds: float | None = None,
    logger: logging.Logger,
) -> None:
    if result is None:
        return
    if not isinstance(result, RuntimeOutput | SplitRuntimeOutput | RuntimeOutputBatch):
        raise TypeError(
            "Runtime operation must return RuntimeOutput, SplitRuntimeOutput, "
            "RuntimeOutputBatch, or None."
        )
    if isinstance(result, RuntimeOutputBatch):
        success = False
        try:
            for output in result.outputs:
                if isinstance(output, SplitRuntimeOutput):
                    _persist_split_runtime_output(
                        output,
                        visuals=visuals,
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                        logger=logger,
                    )
                else:
                    _persist_runtime_output(
                        output,
                        target=target,
                        visuals=visuals,
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                        logger=logger,
                    )
            success = True
        finally:
            if result.on_complete is not None:
                result.on_complete(success)
        return

    if isinstance(result, SplitRuntimeOutput):
        _persist_split_runtime_output(
            result,
            visuals=visuals,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            logger=logger,
        )
        return

    _persist_runtime_output(
        result,
        target=target,
        visuals=visuals,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        logger=logger,
    )


__all__ = [
    "ArtifactOutput",
    "RuntimeOutput",
    "RuntimeOutputBatch",
    "SplitRuntimeOutput",
    "persist_artifact_output",
    "persist_runtime_result",
]
