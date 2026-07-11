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
        "heartbeat_interval_seconds": profile.heartbeat_interval_seconds,
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
        payload["run_config"] = cfg.model_dump(exclude_unset=True, exclude_none=True)
    payload["target"] = entry.target_id
    return payload
