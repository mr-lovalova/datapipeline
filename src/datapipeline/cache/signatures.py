import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from pydantic import ValidationError


def resolve_record_stream_signature(
    project_yaml: Path,
    stream_id: str,
) -> str | None:
    payload = _record_stream_signature_payload(project_yaml, stream_id)
    if payload is None:
        return None
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _record_stream_signature_payload(
    project_yaml: Path,
    stream_id: str,
) -> dict[str, Any] | None:
    from datapipeline.services.bootstrap.core import load_streams

    # Some unit/runtime tests build an in-memory Runtime without a full
    # project.yaml. In that case record-stream caching is unavailable.
    try:
        streams = load_streams(project_yaml)
    except (FileNotFoundError, ValidationError):
        return None
    return _stream_payload_from_config(streams, stream_id, {})


def _stream_payload_from_config(
    streams,
    stream_id: str,
    memo: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    cached = memo.get(stream_id)
    if cached is not None:
        return cached

    spec = streams.streams.get(stream_id)
    if spec is None:
        return None

    payload: dict[str, Any] = {
        "stream": spec.model_dump(mode="json", by_alias=True),
        "boundary": "record_stream:stream_transforms",
    }
    if spec.reads_source:
        source = spec.from_.source
        if source is not None and source in streams.raw:
            payload["source"] = streams.raw[source].model_dump(mode="json")
    else:
        inputs: dict[str, Any] = {}
        for ref in spec.input_refs().values():
            child = _stream_payload_from_config(streams, ref, memo)
            if child is None:
                return None
            inputs[ref] = child
        payload["inputs"] = inputs

    memo[stream_id] = payload
    return payload
