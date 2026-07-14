import gzip
import hashlib
import json
import shutil
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Final, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample_key import (
    SampleKeyValueType,
    sample_key_value_type,
)
from datapipeline.io.sinks import GzipBinarySink
from datapipeline.utils.time import CADENCE_PATTERN, parse_datetime

VECTOR_INPUTS_MANIFEST_VERSION: Final = 4
_JSON_SCALAR_TYPES = {type(None), bool, int, float, str}
_NonEmptyString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


@dataclass(frozen=True)
class WrittenVectorInputShard:
    rows: int
    content_hash: str


class CachedVectorInputShard(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: _NonEmptyString
    path: _NonEmptyString
    rows: int = Field(strict=True, ge=0)

    @field_validator("path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("shard path must be relative to the manifest")
        return str(path)


class CachedVectorInputsManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal[4] = VECTOR_INPUTS_MANIFEST_VERSION
    format: Literal["jsonl.gz"] = "jsonl.gz"
    cadence: str = Field(pattern=CADENCE_PATTERN)
    sample_keys: tuple[_NonEmptyString, ...] = ()
    sample_key_types: tuple[SampleKeyValueType, ...] = ()
    features: tuple[CachedVectorInputShard, ...] = ()
    targets: tuple[CachedVectorInputShard, ...] = ()

    @field_validator("sample_keys")
    @classmethod
    def validate_unique_sample_keys(cls, keys: tuple[str, ...]) -> tuple[str, ...]:
        if len(keys) != len(set(keys)):
            raise ValueError("sample keys must be unique")
        return keys

    @model_validator(mode="after")
    def validate_unique_shards(self) -> Self:
        if len(self.sample_key_types) != len(self.sample_keys):
            raise ValueError("sample key type count must match sample keys")
        for name, shards in (("feature", self.features), ("target", self.targets)):
            identifiers = [shard.id for shard in shards]
            if len(identifiers) != len(set(identifiers)):
                raise ValueError(f"{name} shard ids must be unique")
        paths = [shard.path for shard in (*self.features, *self.targets)]
        if len(paths) != len(set(paths)):
            raise ValueError("shard paths must be unique")
        return self


def _to_iso(value: datetime) -> str:
    text = value.isoformat()
    if text.endswith("+00:00"):
        return text[:-6] + "Z"
    return text


def feature_record_to_vector_input_row(
    item: FeatureRecord | FeatureSequence,
) -> dict[str, Any]:
    if type(item.entity_key) is not tuple:
        raise TypeError(f"Feature '{item.id}' entity key must be a tuple.")
    row: dict[str, Any] = {
        "id": item.id,
        "time": _to_iso(item.time),
        "entity_key": list(item.entity_key),
    }
    if isinstance(item, FeatureSequence):
        if type(item.values) is not list:
            raise TypeError(f"Feature sequence '{item.id}' values must be a list.")
        row["kind"] = "sequence"
        row["values"] = item.values
    else:
        row["kind"] = "record"
        row["value"] = item.value
    return row


def _require_json_value(value: Any) -> None:
    value_type = type(value)
    if value_type in _JSON_SCALAR_TYPES:
        return
    if value_type is list:
        for item in value:
            _require_json_value(item)
        return
    if value_type is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError(
                    "Vector input mappings require string keys; "
                    f"got {type(key).__name__}."
                )
            _require_json_value(item)
        return
    raise TypeError(
        f"Vector inputs require JSON-native values; got {value_type.__name__}."
    )


def write_vector_input_rows(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
) -> WrittenVectorInputShard:
    sink = GzipBinarySink(path)
    count = 0
    digest = hashlib.sha256()
    try:
        for row in rows:
            payload = row if type(row) is dict else dict(row)
            entity_key = payload.get("entity_key")
            if type(entity_key) is not list:
                raise TypeError("Vector input rows require list 'entity_key'.")
            for index, component in enumerate(entity_key):
                sample_key_value_type(f"entity_key[{index}]", component)
            kind = payload.get("kind")
            if kind == "record":
                if "value" not in payload:
                    raise TypeError("Vector input record rows require 'value'.")
            elif kind == "sequence":
                values = payload.get("values")
                if type(values) is not list:
                    raise TypeError("Vector input sequence rows require list 'values'.")
            else:
                raise TypeError(f"Unsupported vector input row kind {kind!r}.")
            _require_json_value(payload)
            line = json.dumps(payload, separators=(",", ":")) + "\n"
            encoded = line.encode("utf-8")
            digest.update(encoded)
            sink.write_bytes(encoded)
            count += 1
        sink.close()
    except BaseException:
        sink.abort()
        raise
    return WrittenVectorInputShard(
        rows=count,
        content_hash=digest.hexdigest(),
    )


def load_vector_inputs_manifest(path: Path) -> CachedVectorInputsManifest:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected vector inputs manifest object in '{path}'.")
    version = payload.get("version")
    if type(version) is not int or version != VECTOR_INPUTS_MANIFEST_VERSION:
        raise ValueError(
            f"Unsupported vector inputs manifest version {version!r} in '{path}'. "
            "Rebuild vector inputs and dependent artifacts in FORCE mode."
        )
    try:
        manifest = CachedVectorInputsManifest.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(
            f"Invalid vector inputs manifest '{path}'. Rebuild vector inputs and "
            "dependent artifacts in FORCE mode."
        ) from exc

    root = path.parent.resolve()
    resolved_paths: list[Path] = []
    for shard in (*manifest.features, *manifest.targets):
        try:
            resolved = (root / shard.path).resolve()
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError(
                f"Vector inputs shard '{shard.path}' escapes manifest directory "
                f"'{root}'."
            ) from exc
        resolved_paths.append(resolved)
    if len(resolved_paths) != len(set(resolved_paths)):
        raise ValueError(f"Vector inputs manifest '{path}' has duplicate shard paths.")
    return manifest


def prune_vector_input_cache(manifest_path: Path) -> tuple[Path, ...]:
    """Remove generations unreachable from the current manifest."""

    if not manifest_path.is_file():
        return ()
    manifest = load_vector_inputs_manifest(manifest_path)
    cache_root = manifest_path.parent / f"{manifest_path.stem}.shards"
    if not cache_root.exists():
        return ()
    if cache_root.is_symlink() or not cache_root.is_dir():
        raise RuntimeError(f"Vector inputs shard path is not a directory: {cache_root}")

    retained: set[str] = set()
    for shard in (*manifest.features, *manifest.targets):
        parts = Path(shard.path).parts
        if len(parts) < 3 or parts[0] != cache_root.name:
            return ()
        retained.add(parts[1])

    removed: list[Path] = []
    for path in cache_root.iterdir():
        if path.name in retained:
            continue
        if path.is_symlink() or not path.is_dir():
            path.unlink()
        else:
            shutil.rmtree(path)
        removed.append(path)
    return tuple(sorted(removed))


def open_vector_input_records(
    path: Path,
) -> Iterator[FeatureRecord | FeatureSequence]:
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
) -> FeatureRecord | FeatureSequence:
    feature_id = _required_string(row, "id", path)
    time_value = parse_datetime(_required_string(row, "time", path))
    entity_key = _entity_key(row, path)
    kind = _required_string(row, "kind", path)
    if kind == "record":
        if "value" not in row:
            raise ValueError(
                f"Vector input record row in '{path}' must define 'value'."
            )
        return FeatureRecord(
            record=TemporalRecord(time=time_value),
            id=feature_id,
            value=row["value"],
            entity_key=entity_key,
        )
    if kind == "sequence":
        values = row.get("values")
        if not isinstance(values, list):
            raise ValueError(
                f"Vector input sequence row in '{path}' must define values."
            )
        return FeatureSequence(
            time=time_value,
            id=feature_id,
            values=values,
            entity_key=entity_key,
        )
    raise ValueError(f"Unsupported vector input row kind '{kind}' in '{path}'.")


def _entity_key(row: Mapping[str, Any], path: Path) -> tuple:
    value = row.get("entity_key")
    if not isinstance(value, list):
        raise ValueError(f"Vector input row in '{path}' must define list 'entity_key'.")
    for index, component in enumerate(value):
        try:
            sample_key_value_type(f"entity_key[{index}]", component)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Vector input row in '{path}' has an invalid entity key."
            ) from exc
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
