import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from datapipeline.io.sinks import AtomicTextFileSink


def read_json_artifact(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in artifact '{path}'")
    return payload


def write_json_artifact(path: Path, payload: Mapping[str, Any]) -> None:
    sink = AtomicTextFileSink(path)
    try:
        json.dump(payload, sink.fh, indent=2, sort_keys=True)
        sink.close()
    except BaseException:
        sink.abort()
        raise
