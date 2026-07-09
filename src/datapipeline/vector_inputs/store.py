import gzip
import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.io.sinks import GzipBinarySink
from datapipeline.utils.time import parse_datetime


@dataclass(frozen=True)
class CachedVectorInputShard:
    id: str
    path: str
    rows: int


@dataclass(frozen=True)
class CachedVectorInputsManifest:
    format: str
    group_by: str
    sample_keys: tuple[str, ...]
    feature_shards: tuple[CachedVectorInputShard, ...]
    target_shards: tuple[CachedVectorInputShard, ...]


def _to_iso(value: datetime) -> str:
    text = value.isoformat()
    if text.endswith("+00:00"):
        return text[:-6] + "Z"
    return text


def _record_time(item: FeatureRecord | FeatureRecordSequence) -> datetime:
    rec = getattr(item, "record", None)
    if rec is not None:
        return rec.time
    records = getattr(item, "records", None) or []
    if not records:
        raise ValueError(f"Feature record '{item.id}' has no records to anchor time.")
    return records[-1].time


def feature_record_to_vector_input_row(
    item: FeatureRecord | FeatureRecordSequence,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": item.id,
        "time": _to_iso(_record_time(item)),
        "entity_key": list(getattr(item, "entity_key", ()) or ()),
    }
    if isinstance(item, FeatureRecordSequence):
        row["kind"] = "sequence"
        row["values"] = list(item.values)
    else:
        row["kind"] = "record"
        row["value"] = item.value
    return row


def write_vector_input_rows(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    sink = GzipBinarySink(path)
    count = 0
    try:
        for row in rows:
            line = json.dumps(row, default=str, separators=(",", ":")) + "\n"
            sink.write_bytes(line.encode("utf-8"))
            count += 1
    except BaseException:
        sink.abort()
        raise
    sink.close()
    return count


def load_vector_inputs_manifest(path: Path) -> CachedVectorInputsManifest:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected vector inputs manifest object in '{path}'.")
    return CachedVectorInputsManifest(
        format=_required_string(payload, "format", path),
        group_by=_required_string(payload, "group_by", path),
        sample_keys=tuple(_string_list(payload.get("sample_keys"), "sample_keys", path)),
        feature_shards=_shards(payload.get("features"), "features", path),
        target_shards=_shards(payload.get("targets"), "targets", path),
    )


def open_vector_input_records(path: Path) -> Iterator[FeatureRecord | FeatureRecordSequence]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError(f"Expected vector input row object in '{path}'.")
            yield _row_to_feature_record(row, path)


def _row_to_feature_record(
    row: Mapping[str, Any],
    path: Path,
) -> FeatureRecord | FeatureRecordSequence:
    feature_id = _required_string(row, "id", path)
    time_value = parse_datetime(_required_string(row, "time", path))
    entity_key = _entity_key(row, path)
    record = TemporalRecord(time=time_value)
    kind = _required_string(row, "kind", path)
    if kind == "record":
        return FeatureRecord(
            record=record,
            id=feature_id,
            value=row.get("value"),
            entity_key=entity_key,
        )
    if kind == "sequence":
        values = row.get("values")
        if not isinstance(values, list):
            raise ValueError(f"Vector input sequence row in '{path}' must define values.")
        return FeatureRecordSequence(
            records=[record],
            id=feature_id,
            values=values,
            entity_key=entity_key,
        )
    raise ValueError(f"Unsupported vector input row kind '{kind}' in '{path}'.")


def _entity_key(row: Mapping[str, Any], path: Path) -> tuple:
    value = row.get("entity_key")
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"Vector input row in '{path}' must define list 'entity_key'.")
    return tuple(value)


def _required_string(
    payload: Mapping[str, Any],
    key: str,
    path: Path,
) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Vector inputs manifest '{path}' must define '{key}'.")
    return value


def _string_list(value: Any, key: str, path: Path) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"Vector inputs manifest '{path}' must define list '{key}'.")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(
                f"Vector inputs manifest '{path}' has non-string item in '{key}'."
            )
        items.append(item)
    return items


def _shards(value: Any, key: str, path: Path) -> tuple[CachedVectorInputShard, ...]:
    if not isinstance(value, list):
        raise ValueError(f"Vector inputs manifest '{path}' must define list '{key}'.")
    shards: list[CachedVectorInputShard] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError(f"Vector inputs manifest '{path}' has invalid '{key}' item.")
        rows = item.get("rows")
        if not isinstance(rows, int):
            raise ValueError(
                f"Vector inputs manifest '{path}' shard in '{key}' must define rows."
            )
        shards.append(
            CachedVectorInputShard(
                id=_required_string(item, "id", path),
                path=_required_string(item, "path", path),
                rows=rows,
            )
        )
    return tuple(shards)
