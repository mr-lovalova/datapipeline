from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from datapipeline.config.run import OutputConfig
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
    provider: str
    progress_style: str


def resolve_visuals(
    *,
    cli_provider: str | None,
    config_provider: str | None,
    workspace_provider: str | None,
    cli_style: str | None,
    config_style: str | None,
    workspace_style: str | None,
    default_provider: str = "auto",
    default_style: str = "auto",
) -> VisualSettings:
    provider = cascade(
        _normalize_lower(cli_provider),
        _normalize_lower(config_provider),
        _normalize_lower(workspace_provider),
        default_provider,
    ) or default_provider
    progress_style = cascade(
        _normalize_lower(cli_style),
        _normalize_lower(config_style),
        _normalize_lower(workspace_style),
        default_style,
    ) or default_style
    return VisualSettings(provider=provider, progress_style=progress_style)


@dataclass(frozen=True)
class LogLevelDecision:
    name: str
    value: int


def resolve_log_level(
    *levels: Any,
    fallback: str = "WARNING",
) -> LogLevelDecision:
    name = None
    for level in levels:
        normalized = _normalize_upper(level)
        if normalized:
            name = normalized
            break
    if not name:
        name = _normalize_upper(fallback) or "WARNING"
    value = logging._nameToLevel.get(name, logging.WARNING)
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
) -> OutputConfig | None:
    if workspace is None:
        return None
    serve_defaults = getattr(workspace.config, "serve", None)
    if not serve_defaults or not serve_defaults.output_defaults:
        return None
    od = serve_defaults.output_defaults
    output_path = None
    if od.path:
        candidate = Path(od.path)
        output_path = (
            candidate
            if candidate.is_absolute()
            else (workspace.root / candidate).resolve()
        )
    return OutputConfig(
        transport=od.transport,
        format=od.format,
        path=output_path,
    )
