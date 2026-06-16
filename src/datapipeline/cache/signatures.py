import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from pydantic import ValidationError


def resolve_record_stream_cache_ref(
    project_yaml: Path,
    stream_id: str,
) -> tuple[str, str, str] | None:
    payload = _record_stream_signature_payload(project_yaml, stream_id)
    if payload is None:
        return None
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return (
        str(payload["cache_kind"]),
        str(payload["cache_stage"]),
        sha256(encoded).hexdigest(),
    )


def _record_stream_signature_payload(
    project_yaml: Path,
    stream_id: str,
) -> dict[str, Any] | None:
    from datapipeline.config.dataset.loader import load_dataset
    from datapipeline.services.bootstrap.core import load_streams

    # Some unit/runtime tests build an in-memory Runtime without a full
    # project.yaml. In that case record-stream caching is unavailable.
    try:
        streams = load_streams(project_yaml)
    except (FileNotFoundError, ValidationError):
        return None
    payload = _stream_payload_from_config(streams, stream_id, {})
    if payload is None:
        return None
    try:
        dataset = load_dataset(project_yaml, "vectors")
    except (FileNotFoundError, ValidationError, ValueError):
        return payload
    if dataset.sample_keys:
        payload["sample_keys"] = dataset.sample_keys
    return payload


def _stream_payload_from_config(
    streams,
    stream_id: str,
    memo: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    cached = memo.get(stream_id)
    if cached is not None:
        return cached

    ingest = streams.ingests.get(stream_id)
    if ingest is not None:
        payload: dict[str, Any] = {
            "ingest": ingest.model_dump(mode="json", by_alias=True),
            "cache_kind": "ingest_stream",
            "cache_stage": "ordered_records",
            "boundary": "ingest:ordered_records",
        }
        source = ingest.from_.source
        if source in streams.raw:
            payload["source"] = streams.raw[source].model_dump(mode="json")
        memo[stream_id] = payload
        return payload

    spec = streams.streams.get(stream_id)
    if spec is None:
        return None

    payload: dict[str, Any] = {
        "stream": spec.model_dump(mode="json", by_alias=True),
        "cache_kind": "stream",
        "cache_stage": "stream_transforms",
        "boundary": "stream:stream_transforms",
    }
    inputs: dict[str, Any] = {}
    for ref in spec.input_refs().values():
        child = _stream_payload_from_config(streams, ref, memo)
        if child is None:
            return None
        inputs[ref] = child
    payload["inputs"] = inputs

    memo[stream_id] = payload
    return payload
