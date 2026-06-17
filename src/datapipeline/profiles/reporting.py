import logging
from pathlib import Path
from typing import Any, Mapping

from datapipeline.services.execution_artifacts import write_profile_artifact

from .models import ExecutionProfile

logger = logging.getLogger(__name__)


def runtime_profile_report_payload(profile) -> dict[str, object]:
    entry = profile.entry
    payload: dict[str, object] = {
        "label": profile.label,
        "idx": profile.idx,
        "total": profile.total,
        "entry": {
            "name": entry.name,
            "path": str(entry.path) if entry.path else None,
        },
        "preview_index": profile.preview_index,
        "limit": profile.limit,
        "throttle_ms": profile.throttle_ms,
        "log_level": {
            "name": profile.log_decision.name,
            "value": profile.log_decision.value,
        },
        "log_output": {
            "outputs": [
                {
                    "transport": target.transport,
                    "scope": target.scope,
                    "destination": (
                        str(target.destination)
                        if target.destination is not None
                        else None
                    ),
                }
                for target in profile.log_output.outputs
            ],
        },
        "visuals": {
            "provider": profile.visuals.visuals,
        },
        "output": {
            "transport": profile.output.transport,
            "format": profile.output.format,
            "view": profile.output.view,
            "encoding": profile.output.encoding,
            "destination": str(profile.output.destination)
            if profile.output.destination
            else None,
        },
    }
    cfg = entry.config
    if cfg is not None:
        payload["run_config"] = cfg.model_dump(
            exclude_unset=True, exclude_none=True
        )
    payload["target"] = entry.target_id
    return payload


def persist_profile_report(
    profile_kind: str,
    profile: ExecutionProfile,
    payload: Mapping[str, Any] | None,
) -> Path | None:
    if payload is None:
        return None
    output = profile.output
    execution = profile.execution
    if execution is None:
        return None
    try:
        return write_profile_artifact(
            execution=execution,
            profile_kind=profile_kind,
            profile_name=profile.label or profile.name,
            payload=payload,
        )
    except Exception:
        logger.warning(
            "Failed to persist %s profile artifact for %s",
            profile_kind,
            profile.name,
            exc_info=True,
        )
        return None


__all__ = ["persist_profile_report", "runtime_profile_report_payload"]
