from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from datapipeline.config.tasks import ServeOutputConfig
from datapipeline.config.workspace import WorkspaceContext


def cascade(*values, fallback=None):
    """Return the first non-None value from a list, or fallback."""
    for value in values:
        if value is not None:
            return value
    return fallback


def _normalize_lower(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text.lower() if text else None


def _normalize_upper(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, int):
        return logging.getLevelName(value).upper()
    text = str(value).strip()
    return text.upper() if text else None


def _level_value(value: Any) -> Optional[int]:
    name = _normalize_upper(value)
    return logging._nameToLevel.get(name) if name else None


@dataclass(frozen=True)
class VisualSettings:
    visuals: str
    progress: str


def resolve_visuals(
    *,
    cli_visuals: str | None,
    config_visuals: str | None,
    workspace_visuals: str | None,
    cli_progress: str | None,
    config_progress: str | None,
    workspace_progress: str | None,
    default_visuals: str = "auto",
    default_progress: str = "auto",
) -> VisualSettings:
    visuals = cascade(
        _normalize_lower(cli_visuals),
        _normalize_lower(config_visuals),
        _normalize_lower(workspace_visuals),
        default_visuals,
    ) or default_visuals
    progress = cascade(
        _normalize_lower(cli_progress),
        _normalize_lower(config_progress),
        _normalize_lower(workspace_progress),
        default_progress,
    ) or default_progress
    return VisualSettings(visuals=visuals, progress=progress)


@dataclass(frozen=True)
class LogLevelDecision:
    name: str
    value: int


def resolve_log_level(
    *levels: Any,
    fallback: str = "INFO",
) -> LogLevelDecision:
    name = None
    for level in levels:
        normalized = _normalize_upper(level)
        if normalized:
            name = normalized
            break
    if not name:
        name = _normalize_upper(fallback) or "INFO"
    value = logging._nameToLevel.get(name, logging.INFO)
    return LogLevelDecision(name=name, value=value)


def minimum_level(*levels: Any, start: int | None = None) -> int | None:
    """Return the lowest numeric logging level among the provided values."""
    current = start
    for level in levels:
        value = _level_value(level)
        if value is None:
            continue
        if current is None or value < current:
            current = value
    return current


def workspace_output_defaults(
    workspace: WorkspaceContext | None,
) -> ServeOutputConfig | None:
    if workspace is None:
        return None
    serve_defaults = getattr(workspace.config, "serve", None)
    if not serve_defaults or not serve_defaults.output:
        return None
    od = serve_defaults.output
    output_dir = None
    if od.directory:
        candidate = Path(od.directory)
        output_dir = (
            candidate
            if candidate.is_absolute()
            else (workspace.root / candidate).resolve()
        )
    return ServeOutputConfig(
        transport=od.transport,
        format=od.format,
        payload=od.payload,
        directory=output_dir,
    )
