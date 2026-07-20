import gzip
import json
import shutil
from collections.abc import Iterable, Iterator, Mapping
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

from datapipeline.domain.sample_key import (
    SampleKeyValueType,
    sample_key_value_type,
)
from datapipeline.domain.variable import VariableRecord, VariableSequence
from datapipeline.io.sinks.files import GzipBinarySink
from datapipeline.utils.time import CADENCE_PATTERN, parse_datetime

VARIABLE_RECORDS_MANIFEST_VERSION: Final = 6
_JSON_SCALAR_TYPES = {type(None), bool, int, float, str}
_NonEmptyString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class VariableShard(BaseModel):
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


class VariableRecordsManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: Literal[6] = VARIABLE_RECORDS_MANIFEST_VERSION
    format: Literal["jsonl.gz"] = "jsonl.gz"
    cadence: str = Field(pattern=CADENCE_PATTERN)
    sample_keys: tuple[_NonEmptyString, ...] = ()
    sample_key_types: tuple[SampleKeyValueType, ...] = ()
    features: tuple[VariableShard, ...] = ()
    targets: tuple[VariableShard, ...] = ()

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


def variable_record_to_row(
    item: VariableRecord | VariableSequence,
) -> dict[str, Any]:
    if type(item.entity_key) is not tuple:
        raise TypeError(f"Variable '{item.id}' entity key must be a tuple.")
    row: dict[str, Any] = {
        "id": item.id,
        "time": _to_iso(item.time),
        "entity_key": list(item.entity_key),
    }
    if isinstance(item, VariableSequence):
        if type(item.values) is not list:
            raise TypeError(f"Variable sequence '{item.id}' values must be a list.")
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
                    "Variable record mappings require string keys; "
                    f"got {type(key).__name__}."
                )
            _require_json_value(item)
        return
    raise TypeError(
        f"Variable records require JSON-native values; got {value_type.__name__}."
    )


def write_variable_rows(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    sink = GzipBinarySink(path)
    count = 0
    try:
        for row in rows:
            payload = row if type(row) is dict else dict(row)
            entity_key = payload.get("entity_key")
            if type(entity_key) is not list:
                raise TypeError("Variable record rows require list 'entity_key'.")
            for index, component in enumerate(entity_key):
                sample_key_value_type(f"entity_key[{index}]", component)
            kind = payload.get("kind")
            if kind == "record":
                if "value" not in payload:
                    raise TypeError("Scalar variable rows require 'value'.")
            elif kind == "sequence":
                values = payload.get("values")
                if type(values) is not list:
                    raise TypeError(
                        "Variable record sequence rows require list 'values'."
                    )
            else:
                raise TypeError(f"Unsupported variable record row kind {kind!r}.")
            _require_json_value(payload)
            line = json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n"
            encoded = line.encode("utf-8")
            sink.write_bytes(encoded)
            count += 1
        sink.close()
    except BaseException:
        sink.abort()
        raise
    return count


def load_variable_records_manifest(path: Path) -> VariableRecordsManifest:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected variable records manifest object in '{path}'.")
    version = payload.get("version")
    if type(version) is not int or version != VARIABLE_RECORDS_MANIFEST_VERSION:
        raise ValueError(
            f"Unsupported variable records manifest version {version!r} in '{path}'. "
            "Rebuild variable records and dependent artifacts in FORCE mode."
        )
    try:
        manifest = VariableRecordsManifest.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(
            f"Invalid variable records manifest '{path}'. Rebuild variable records and "
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
                f"Variable records shard '{shard.path}' escapes manifest directory "
                f"'{root}'."
            ) from exc
        resolved_paths.append(resolved)
    if len(resolved_paths) != len(set(resolved_paths)):
        raise ValueError(
            f"Variable records manifest '{path}' has duplicate shard paths."
        )
    return manifest


def prune_variable_record_cache(manifest_path: Path) -> tuple[Path, ...]:
    """Remove generations unreachable from the current manifest."""

    if not manifest_path.is_file():
        return ()
    manifest = load_variable_records_manifest(manifest_path)
    cache_root = manifest_path.parent / f"{manifest_path.stem}.shards"
    if not cache_root.exists():
        return ()
    if cache_root.is_symlink() or not cache_root.is_dir():
        raise RuntimeError(
            f"Variable records shard path is not a directory: {cache_root}"
        )

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


def open_variable_records(
    path: Path,
    expected_rows: int | None = None,
) -> Iterator[VariableRecord | VariableSequence]:
    rows = 0
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line, parse_constant=_reject_json_constant)
            if not isinstance(row, dict):
                raise ValueError(f"Expected variable record row object in '{path}'.")
            rows += 1
            if expected_rows is not None and rows > expected_rows:
                raise ValueError(
                    f"Variable records shard '{path}' contains more than its "
                    f"declared {expected_rows} rows."
                )
            yield _row_to_variable(row, path)
    if expected_rows is not None and rows != expected_rows:
        raise ValueError(
            f"Variable records shard '{path}' declares {expected_rows} rows but "
            f"contains {rows}."
        )


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Non-standard JSON number {value!r} is not supported.")


def _row_to_variable(
    row: Mapping[str, Any],
    path: Path,
) -> VariableRecord | VariableSequence:
    variable_id = _required_string(row, "id", path)
    time_value = parse_datetime(_required_string(row, "time", path))
    entity_key = _entity_key(row, path)
    kind = _required_string(row, "kind", path)
    if kind == "record":
        if "value" not in row:
            raise ValueError(f"Scalar variable row in '{path}' must define 'value'.")
        return VariableRecord(
            id=variable_id,
            time=time_value,
            value=row["value"],
            entity_key=entity_key,
        )
    if kind == "sequence":
        values = row.get("values")
        if not isinstance(values, list):
            raise ValueError(
                f"Variable record sequence row in '{path}' must define values."
            )
        return VariableSequence(
            time=time_value,
            id=variable_id,
            values=values,
            entity_key=entity_key,
        )
    raise ValueError(f"Unsupported variable record row kind '{kind}' in '{path}'.")


def _entity_key(row: Mapping[str, Any], path: Path) -> tuple:
    value = row.get("entity_key")
    if not isinstance(value, list):
        raise ValueError(
            f"Variable record row in '{path}' must define list 'entity_key'."
        )
    for index, component in enumerate(value):
        try:
            sample_key_value_type(f"entity_key[{index}]", component)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Variable record row in '{path}' has an invalid entity key."
            ) from exc
    return tuple(value)


def _required_string(
    payload: Mapping[str, Any],
    key: str,
    path: Path,
) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Variable record row in '{path}' must define '{key}'.")
    return value
