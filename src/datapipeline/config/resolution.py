import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from datapipeline.config.observability import ObservabilityConfig
from datapipeline.config.options import LOG_SCOPE_CHOICES, LOG_TRANSPORT_CHOICES
from datapipeline.services.path_policy import sanitize_path_segment

LOG_TRANSPORT_SET = set(LOG_TRANSPORT_CHOICES)
LOG_SCOPE_SET = set(LOG_SCOPE_CHOICES)


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


def observability_value(observability, field: str):
    if observability is None:
        return None
    return getattr(observability, field, None)


def logging_value(observability, field: str):
    logging_cfg = observability_value(observability, "logging")
    if logging_cfg is None:
        return None
    return getattr(logging_cfg, field, None)


def resolve_visuals(
    cli_visuals: str | None,
    config_visuals: str | None,
    default_visuals: str = "on",
) -> str:
    return cascade(
        _normalize_lower(cli_visuals),
        _normalize_lower(config_visuals),
        default_visuals,
    )


def resolve_heartbeat_interval_seconds(
    cli_heartbeat_interval_seconds: float | None,
    config_heartbeat_interval_seconds: float | None,
) -> float | None:
    value = cascade(
        cli_heartbeat_interval_seconds,
        config_heartbeat_interval_seconds,
    )
    if value is None:
        return None
    interval = float(value)
    if interval < 0:
        raise ValueError("heartbeat_interval_seconds must be non-negative")
    return interval


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


@dataclass(frozen=True)
class LogOutputTarget:
    transport: str
    destination: Path | None = None
    scope: str = "global"


@dataclass(frozen=True)
class LogOutputSettings:
    outputs: tuple[LogOutputTarget, ...]


@dataclass(frozen=True)
class ObservabilitySettings:
    visuals: str
    heartbeat_interval_seconds: float | None
    log_decision: LogLevelDecision
    log_output: LogOutputSettings

    def effective_config(self) -> dict[str, object]:
        return {
            "visuals": self.visuals,
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "log_level": self.log_decision.name,
            "log_outputs": [
                {
                    "transport": output.transport,
                    "scope": output.scope,
                    "destination": (
                        str(output.destination)
                        if output.destination is not None
                        else None
                    ),
                }
                for output in self.log_output.outputs
            ],
        }


def log_output_targets_from_config(
    outputs,
    resolve_global_path: Callable[[str], Path],
) -> list[LogOutputTarget]:
    """Map validated config models into normalized LogOutputTarget values."""
    if not outputs:
        return []
    resolved: list[LogOutputTarget] = []
    for item in outputs:
        transport = _normalize_lower(getattr(item, "transport", None)) or ""
        scope = _normalize_lower(getattr(item, "scope", None)) or "global"
        destination: Path | None = None
        path_value = getattr(item, "path", None)
        if transport == "fs" and path_value:
            destination = (
                resolve_global_path(path_value)
                if scope == "global"
                else Path(path_value)
            )
        resolved.append(
            LogOutputTarget(
                transport=transport,
                destination=destination,
                scope=scope,
            )
        )
    return resolved


def resolve_project_log_outputs(
    outputs,
    project_path: Path,
) -> list[LogOutputTarget]:
    return log_output_targets_from_config(
        outputs,
        resolve_global_path=lambda value: (project_path.parent / value).resolve(),
    )


def parse_log_output_specs(
    specs: Sequence[str] | None,
    resolve_global_path: Callable[[str], Path],
) -> list[LogOutputTarget]:
    """Parse CLI --log-output specs into normalized targets."""
    if not specs:
        return []
    parsed: list[LogOutputTarget] = []
    for raw_spec in specs:
        spec = str(raw_spec or "").strip()
        if not spec:
            continue
        lower = spec.lower()
        if lower in {"stderr", "stdout"}:
            parsed.append(LogOutputTarget(transport=lower))
            continue
        if lower in {"execution", "fs:execution", "fs=execution"}:
            parsed.append(LogOutputTarget(transport="fs", scope="execution"))
            continue
        if lower.startswith("execution:") or lower.startswith("execution="):
            raw_path = spec[10:].strip()
            if not raw_path:
                raise ValueError(
                    "--log-output execution target requires a relative path"
                )
            parsed.append(
                LogOutputTarget(
                    transport="fs",
                    destination=Path(raw_path),
                    scope="execution",
                )
            )
            continue
        if lower.startswith("fs:"):
            raw_path = spec.split(":", 1)[1].strip()
        elif lower.startswith("fs="):
            raw_path = spec.split("=", 1)[1].strip()
        else:
            raise ValueError(
                "invalid --log-output value. Use 'stderr', 'stdout', 'fs:<path>', or 'execution[:<relative-path>]'"
            )
        if not raw_path:
            raise ValueError("--log-output fs target requires a path")
        parsed.append(
            LogOutputTarget(
                transport="fs",
                destination=resolve_global_path(raw_path),
                scope="global",
            )
        )
    return parsed


def _normalize_log_outputs(
    output_candidates: Sequence[Sequence[LogOutputTarget] | None],
    allow_execution_scope: bool,
) -> list[LogOutputTarget]:
    for candidate in output_candidates:
        if not candidate:
            continue
        normalized: list[LogOutputTarget] = []
        for target in candidate:
            transport = _normalize_lower(target.transport)
            scope = _normalize_lower(target.scope) or "global"
            destination = target.destination
            if not transport:
                continue
            if transport not in LOG_TRANSPORT_SET:
                choices = ", ".join(LOG_TRANSPORT_CHOICES)
                raise ValueError(
                    f"log transport must be one of {choices}, got {transport!r}"
                )
            if scope not in LOG_SCOPE_SET:
                choices = ", ".join(LOG_SCOPE_CHOICES)
                raise ValueError(f"log scope must be one of {choices}, got {scope!r}")
            if scope == "execution":
                if not allow_execution_scope:
                    raise ValueError(
                        "log scope 'execution' is only valid for execution-scoped operations"
                    )
                if transport != "fs":
                    raise ValueError("log scope 'execution' requires transport='fs'")
                if destination is not None and Path(destination).is_absolute():
                    raise ValueError("execution-scoped log path must be relative")
                normalized.append(
                    LogOutputTarget(
                        transport="fs",
                        destination=Path(destination)
                        if destination is not None
                        else None,
                        scope="execution",
                    )
                )
                continue
            if transport == "fs":
                if destination is None:
                    raise ValueError("log transport 'fs' requires a log path")
                normalized.append(
                    LogOutputTarget(
                        transport=transport,
                        destination=Path(destination),
                        scope="global",
                    )
                )
            else:
                normalized.append(
                    LogOutputTarget(
                        transport=transport,
                        destination=None,
                        scope="global",
                    )
                )
        if normalized:
            return normalized
    return []


def resolve_log_output(
    output_candidates: Sequence[Sequence[LogOutputTarget] | None] = (),
    default_transport: str = "stderr",
    allow_execution_scope: bool = False,
) -> LogOutputSettings:
    explicit_outputs = _normalize_log_outputs(
        output_candidates,
        allow_execution_scope=allow_execution_scope,
    )
    if explicit_outputs:
        return LogOutputSettings(outputs=tuple(explicit_outputs))

    transport = _normalize_lower(default_transport) or "stderr"
    if transport not in LOG_TRANSPORT_SET:
        transport = "stderr"
    return LogOutputSettings(
        outputs=(LogOutputTarget(transport=transport, scope="global"),)
    )


def resolve_observability_settings(
    project_path: Path | None,
    observability: ObservabilityConfig | None,
    *,
    cli_visuals: str | None,
    cli_heartbeat_interval_seconds: float | None,
    cli_log_level: str | None,
    cli_log_outputs: Sequence[LogOutputTarget] | None,
    base_log_level: str,
) -> ObservabilitySettings:
    configured_output_specs = logging_value(observability, "outputs")
    if configured_output_specs and project_path is None:
        raise ValueError("project_path is required for configured log outputs")
    configured_outputs = (
        resolve_project_log_outputs(configured_output_specs, project_path)
        if project_path is not None
        else []
    )
    return ObservabilitySettings(
        visuals=resolve_visuals(
            cli_visuals,
            observability_value(observability, "visuals"),
        ),
        heartbeat_interval_seconds=resolve_heartbeat_interval_seconds(
            cli_heartbeat_interval_seconds,
            observability_value(observability, "heartbeat_interval_seconds"),
        ),
        log_decision=resolve_log_level(
            cli_log_level,
            logging_value(observability, "level"),
            base_log_level,
        ),
        log_output=resolve_log_output(
            output_candidates=(cli_log_outputs, configured_outputs),
            allow_execution_scope=True,
        ),
    )


def resolve_execution_log_outputs(
    settings: LogOutputSettings,
    execution_dir: Path,
    *,
    command: str,
    label: str,
) -> LogOutputSettings:
    outputs: list[LogOutputTarget] = []
    for target in settings.outputs:
        if target.scope != "execution":
            outputs.append(target)
            continue
        relative_path = target.destination
        if relative_path is None:
            name = sanitize_path_segment(label)
            prefix = sanitize_path_segment(command)
            relative_path = Path("logs") / f"{prefix}.{name}.log"
        if relative_path.is_absolute():
            raise ValueError("execution-scoped log path must be relative")
        outputs.append(
            LogOutputTarget(
                transport="fs",
                destination=(execution_dir / relative_path).resolve(),
                scope="global",
            )
        )
    return LogOutputSettings(outputs=tuple(outputs))


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
