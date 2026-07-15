import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from datapipeline.build.state import ArtifactFileFingerprint
from datapipeline.execution.runner import resolve_heartbeat_interval_seconds
from datapipeline.execution.observability import (
    OperationProgressTracker,
    emit_file_result,
)
from datapipeline.io.factory import writer_factory
from datapipeline.io.normalization import json_text, raw_payload
from datapipeline.io.output import OutputTarget, output_destination_key
from datapipeline.io.sinks import AtomicTextFileSink


@dataclass(frozen=True)
class ArtifactOutput:
    relative_path: str
    companion_paths: tuple[str, ...] = ()
    meta: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class PersistedArtifact:
    relative_path: str
    files: tuple[ArtifactFileFingerprint, ...]
    meta: Mapping[str, object]


@dataclass(frozen=True)
class RuntimeOutput:
    rows: Iterable[Any] | None = None
    payload: Mapping[str, Any] | None = None
    render_html: Callable[[], str] | None = None
    target: OutputTarget | None = None


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
) -> PersistedArtifact | None:
    if result is None:
        return None
    if not isinstance(result, ArtifactOutput):
        raise TypeError("Build operation must return ArtifactOutput or None.")
    if expected_relative_path is not None and Path(result.relative_path) != Path(
        expected_relative_path
    ):
        raise ValueError(
            f"Artifact '{artifact_key}' returned path '{result.relative_path}', "
            f"but its operation declares '{expected_relative_path}'."
        )
    relative_paths = (result.relative_path, *result.companion_paths)
    normalized_paths = tuple(Path(relative_path) for relative_path in relative_paths)
    path_keys = {output_destination_key(path) for path in normalized_paths}
    if len(normalized_paths) != len(path_keys):
        raise ValueError(f"Artifact '{artifact_key}' output paths must be unique.")

    artifacts_root = Path(runtime.artifacts_root).resolve()
    files: list[ArtifactFileFingerprint] = []
    for relative_path, normalized_path in zip(relative_paths, normalized_paths):
        if normalized_path.is_absolute() or ".." in normalized_path.parts:
            raise ValueError(
                f"Artifact '{artifact_key}' output path '{relative_path}' must be "
                "relative to the artifacts root."
            )
        full_path = (artifacts_root / normalized_path).resolve()
        try:
            full_path.relative_to(artifacts_root)
        except ValueError as exc:
            raise ValueError(
                f"Artifact '{artifact_key}' output must stay under {artifacts_root}."
            ) from exc
        if not full_path.is_file():
            raise RuntimeError(
                f"Artifact '{artifact_key}' did not create its declared output: "
                f"{full_path}."
            )
        files.append(ArtifactFileFingerprint.from_path(str(normalized_path), full_path))

    persisted = PersistedArtifact(
        relative_path=str(normalized_paths[0]),
        files=tuple(files),
        meta=dict(result.meta),
    )
    return persisted


def _persist_runtime_output(
    result: RuntimeOutput,
    *,
    target: OutputTarget | None,
    heartbeat_interval_seconds: float | None,
    logger: logging.Logger,
) -> None:
    effective_target = result.target or target
    if effective_target is None:
        raise ValueError("Runtime operation requires profile output target.")

    if effective_target.format == "html":
        if result.render_html is None:
            raise ValueError("html output is not supported for this operation.")
        destination = effective_target.destination
        if destination is None:
            raise ValueError("html output requires fs destination.")
        document = result.render_html()
        sink = AtomicTextFileSink(
            destination,
            encoding=effective_target.encoding or "utf-8",
        )
        try:
            sink.write_text(document)
            sink.close()
        except BaseException:
            sink.abort()
            raise
        emit_file_result("Output", destination)
        return

    rows = result.rows
    if rows is None:
        if result.payload is None:
            rows = ()
        elif effective_target.format == "txt":
            rows = (json_text(raw_payload(result.payload), indent=2),)
        else:
            rows = (dict(result.payload),)

    writer = writer_factory(effective_target)
    progress = OperationProgressTracker(
        "write_output",
        resolve_heartbeat_interval_seconds(heartbeat_interval_seconds),
    )
    success = False
    try:
        for row in rows:
            writer.write(row)
            progress.advance()
        writer.close()
        success = True
    finally:
        if not success:
            try:
                writer.abort()
            except Exception:
                logger.debug("Failed to abort runtime output writer", exc_info=True)

    if effective_target.destination is not None:
        emit_file_result("Output", effective_target.destination)
    elif effective_target.transport == "stdout":
        logger.info("Output: stdout")


def _persist_split_runtime_output(
    result: SplitRuntimeOutput,
    *,
    heartbeat_interval_seconds: float | None,
    logger: logging.Logger,
) -> None:
    if not result.targets:
        raise ValueError("Split runtime output requires at least one target.")

    planned_targets: list[tuple[str, OutputTarget, Path]] = []
    destination_owners: dict[str, str] = {}
    for label, target in result.targets.items():
        destination = target.destination
        if target.transport != "fs" or destination is None:
            raise ValueError("Split runtime output requires fs destinations.")
        destination_key = output_destination_key(destination)
        previous = destination_owners.get(destination_key)
        if previous is not None:
            raise ValueError(
                f"Split outputs {previous!r} and {label!r} resolve to the same "
                f"destination: {destination}"
            )
        destination_owners[destination_key] = label
        planned_targets.append((label, target, destination))

    writers = {}
    counts: dict[str, int] = {}
    destinations: dict[str, Path] = {}
    rows = iter(result.rows)
    progress = OperationProgressTracker(
        "write_output",
        resolve_heartbeat_interval_seconds(heartbeat_interval_seconds),
    )
    success = False
    try:
        for label, target, destination in planned_targets:
            writers[label] = writer_factory(target)
            counts[label] = 0
            destinations[label] = destination

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

    for label, destination in destinations.items():
        emit_file_result(label, destination)


def persist_runtime_result(
    result: object,
    *,
    target: OutputTarget | None,
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
                        heartbeat_interval_seconds=heartbeat_interval_seconds,
                        logger=logger,
                    )
                else:
                    _persist_runtime_output(
                        output,
                        target=target,
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
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            logger=logger,
        )
        return

    _persist_runtime_output(
        result,
        target=target,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        logger=logger,
    )
