import logging
from collections.abc import Iterator
from contextlib import contextmanager
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
from datapipeline.io.protocols import Writer
from datapipeline.io.sinks.files import AtomicTextFileSink


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
class RoutedRuntimeOutput:
    rows: Iterable[Any]
    targets: Mapping[str, OutputTarget]
    output_for_row: Callable[[Any], str | None]
    limit_per_output: int | None = None


@dataclass(frozen=True)
class RuntimeOutputBatch:
    outputs: Sequence[RuntimeOutput | RoutedRuntimeOutput]
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


def _close_runtime_rows(rows: Iterable[Any]) -> None:
    close = getattr(rows, "close", None)
    if callable(close):
        close()


@contextmanager
def _runtime_rows(
    rows: Iterable[Any],
    logger: logging.Logger,
) -> Iterator[Iterator[Any]]:
    iterator: Iterator[Any] | None = None
    failed = False
    try:
        iterator = iter(rows)
        yield iterator
    except BaseException:
        failed = True
        raise
    finally:
        try:
            try:
                if iterator is not None:
                    _close_runtime_rows(iterator)
            finally:
                if iterator is not rows:
                    _close_runtime_rows(rows)
        except BaseException:
            if not failed:
                raise
            logger.debug("Failed to close runtime output rows", exc_info=True)


def _payload_rows(result: RuntimeOutput, target: OutputTarget) -> Iterator[Any]:
    if result.payload is None:
        return iter(())
    if target.format == "txt":
        return iter((json_text(raw_payload(result.payload), indent=2),))
    return iter((dict(result.payload),))


def _write_html_output(
    result: RuntimeOutput,
    target: OutputTarget,
    logger: logging.Logger,
) -> None:
    if result.render_html is None:
        raise ValueError("html output is not supported for this operation.")
    destination = target.destination
    if destination is None:
        raise ValueError("html output requires fs destination.")

    sink = AtomicTextFileSink(
        destination,
        encoding=target.encoding or "utf-8",
    )
    try:
        sink.write_text(result.render_html())
        sink.close()
    except BaseException:
        try:
            sink.abort()
        except BaseException:
            logger.debug("Failed to abort html output writer", exc_info=True)
        raise
    emit_file_result("Output", destination)


def _persist_runtime_output(
    result: RuntimeOutput,
    *,
    target: OutputTarget | None,
    heartbeat_interval_seconds: float | None,
    logger: logging.Logger,
) -> None:
    supplied_rows = result.rows
    owned_rows = supplied_rows if supplied_rows is not None else ()
    writer = None
    try:
        with _runtime_rows(owned_rows, logger) as rows:
            effective_target = result.target or target
            if effective_target is None:
                raise ValueError("Runtime operation requires profile output target.")

            if effective_target.format != "html":
                if supplied_rows is None:
                    rows = _payload_rows(result, effective_target)
                writer = writer_factory(effective_target)
                progress = OperationProgressTracker(
                    "write_output",
                    "rows",
                    resolve_heartbeat_interval_seconds(heartbeat_interval_seconds),
                )
                for row in rows:
                    writer.write(row)
                    progress.advance()

        if effective_target.format == "html":
            _write_html_output(result, effective_target, logger)
            return

        assert writer is not None
        writer.close()
    except BaseException:
        if writer is not None:
            try:
                writer.abort()
            except BaseException:
                logger.debug("Failed to abort runtime output writer", exc_info=True)
        raise

    if effective_target.destination is not None:
        emit_file_result("Output", effective_target.destination)
    elif effective_target.transport == "stdout":
        logger.info("Output: stdout")


def _planned_routed_targets(
    result: RoutedRuntimeOutput,
) -> list[tuple[str, OutputTarget, Path]]:
    if not result.targets:
        raise ValueError("Routed runtime output requires at least one target.")

    planned: list[tuple[str, OutputTarget, Path]] = []
    destination_owners: dict[str, str] = {}
    for output_id, target in result.targets.items():
        destination = target.destination
        if target.transport != "fs" or destination is None:
            raise ValueError("Routed runtime output requires fs destinations.")
        destination_key = output_destination_key(destination)
        previous = destination_owners.get(destination_key)
        if previous is not None:
            raise ValueError(
                f"Routed outputs {previous!r} and {output_id!r} resolve to the same "
                f"destination: {destination}"
            )
        destination_owners[destination_key] = output_id
        planned.append((output_id, target, destination))
    return planned


def _route_runtime_rows(
    result: RoutedRuntimeOutput,
    rows: Iterator[Any],
    writers: Mapping[str, Writer],
    counts: dict[str, int],
    progress: OperationProgressTracker,
) -> None:
    for row in rows:
        routed_id = result.output_for_row(row)
        if routed_id is None:
            continue
        writer = writers.get(routed_id)
        if writer is None:
            raise ValueError(
                f"output_for_row returned unknown output ID {routed_id!r}."
            )
        if (
            result.limit_per_output is not None
            and counts[routed_id] >= result.limit_per_output
        ):
            if all(count >= result.limit_per_output for count in counts.values()):
                break
            continue
        writer.write(row)
        counts[routed_id] += 1
        progress.advance()
        if result.limit_per_output is not None and all(
            count >= result.limit_per_output for count in counts.values()
        ):
            break


def _persist_routed_runtime_output(
    result: RoutedRuntimeOutput,
    *,
    heartbeat_interval_seconds: float | None,
    logger: logging.Logger,
) -> None:
    writers: dict[str, Writer] = {}
    counts: dict[str, int] = {}
    destinations: dict[str, Path] = {}
    try:
        with _runtime_rows(result.rows, logger) as rows:
            for output_id, target, destination in _planned_routed_targets(result):
                writers[output_id] = writer_factory(target)
                counts[output_id] = 0
                destinations[output_id] = destination

            progress = OperationProgressTracker(
                "write_output",
                "rows",
                resolve_heartbeat_interval_seconds(heartbeat_interval_seconds),
            )
            _route_runtime_rows(result, rows, writers, counts, progress)

        for writer in writers.values():
            writer.close()
    except BaseException:
        for writer in writers.values():
            try:
                writer.abort()
            except BaseException:
                logger.debug(
                    "Failed to abort routed runtime output writer",
                    exc_info=True,
                )
        raise

    for output_id, destination in destinations.items():
        emit_file_result(output_id, destination)


def _close_pending_runtime_outputs(
    outputs: Sequence[RuntimeOutput | RoutedRuntimeOutput],
    logger: logging.Logger,
) -> None:
    for output in outputs:
        if output.rows is None:
            continue
        try:
            _close_runtime_rows(output.rows)
        except BaseException:
            logger.debug(
                "Failed to close pending runtime output rows",
                exc_info=True,
            )


def _persist_runtime_batch(
    result: RuntimeOutputBatch,
    target: OutputTarget | None,
    heartbeat_interval_seconds: float | None,
    logger: logging.Logger,
) -> None:
    success = False
    attempted = 0
    try:
        for output in result.outputs:
            attempted += 1
            if isinstance(output, RoutedRuntimeOutput):
                _persist_routed_runtime_output(
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
        if not success:
            _close_pending_runtime_outputs(result.outputs[attempted:], logger)
        if result.on_complete is not None:
            try:
                result.on_complete(success)
            except BaseException:
                if success:
                    raise
                logger.debug(
                    "Runtime output completion callback failed after an earlier "
                    "persistence failure",
                    exc_info=True,
                )


def persist_runtime_result(
    result: object,
    *,
    target: OutputTarget | None,
    heartbeat_interval_seconds: float | None = None,
    logger: logging.Logger,
) -> None:
    if result is None:
        return
    if not isinstance(result, RuntimeOutput | RoutedRuntimeOutput | RuntimeOutputBatch):
        raise TypeError(
            "Runtime operation must return RuntimeOutput, RoutedRuntimeOutput, "
            "RuntimeOutputBatch, or None."
        )
    if isinstance(result, RuntimeOutputBatch):
        _persist_runtime_batch(
            result,
            target,
            heartbeat_interval_seconds,
            logger,
        )
        return

    if isinstance(result, RoutedRuntimeOutput):
        _persist_routed_runtime_output(
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
