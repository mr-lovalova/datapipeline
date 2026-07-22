import gzip
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

import pytest

from datapipeline.artifacts.series import (
    SERIES_MANIFEST_VERSION,
    load_series_manifest,
    open_series,
    prune_series_cache,
    series_record_to_row,
    write_series_rows,
)
from datapipeline.domain.series import SeriesRecord, SeriesSequence


def _time(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc)


def test_series_record_rows_round_trip_json_native_values(tmp_path: Path) -> None:
    record = SeriesRecord(
        id="record",
        time=_time(0),
        value={"values": [None, True, 1, 1.5, "text"]},
        entity_key=("A", 7, "north"),
    )
    null_record = SeriesRecord(
        id="null_record",
        time=_time(1),
        value=None,
    )
    sequence = SeriesSequence(
        time=_time(2),
        id="sequence",
        values=[None, False, 2, 2.5, "other"],
        entity_key=("B", 8, "south"),
    )
    destination = tmp_path / "inputs.jsonl.gz"

    written = write_series_rows(
        destination,
        (
            MappingProxyType(series_record_to_row(item))
            for item in (record, null_record, sequence)
        ),
    )

    assert written == 3
    assert list(open_series(destination, expected_rows=3)) == [
        record,
        null_record,
        sequence,
    ]


def test_series_record_row_preserves_wire_shape() -> None:
    row = series_record_to_row(
        SeriesRecord(
            id="price",
            time=_time(0),
            value=1.0,
            entity_key=("AAPL",),
        )
    )

    assert list(row) == ["id", "time", "entity_key", "kind", "value"]
    assert row == {
        "id": "price",
        "time": "2024-01-01T00:00:00Z",
        "entity_key": ["AAPL"],
        "kind": "record",
        "value": 1.0,
    }


def test_series_record_gzip_is_reproducible(tmp_path: Path) -> None:
    row = {
        "id": "price",
        "time": _time(0).isoformat(),
        "kind": "record",
        "entity_key": [],
        "value": 1.0,
    }
    first = tmp_path / "first.jsonl.gz"
    second = tmp_path / "second.jsonl.gz"

    first_result = write_series_rows(first, [row])
    second_result = write_series_rows(second, [row])

    assert first.read_bytes() == second.read_bytes()
    assert first_result == second_result


def test_series_record_writer_aborts_when_atomic_commit_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    def fail_commit(*_args):
        raise OSError("commit failed")

    monkeypatch.setattr(
        "datapipeline.io.sinks.files._commit_temp_file",
        fail_commit,
    )
    destination = tmp_path / "rows.jsonl.gz"
    row = {
        "id": "price",
        "time": _time(0).isoformat(),
        "kind": "record",
        "entity_key": [],
        "value": 1.0,
    }

    with pytest.raises(OSError, match="commit failed"):
        write_series_rows(destination, [row])

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "value",
    [
        {"nested": [Decimal("1.25")]},
        (1, 2),
        {1: "one"},
    ],
    ids=["unsupported-object", "tuple", "non-string-mapping-key"],
)
def test_series_record_writer_rejects_lossy_values_atomically(
    tmp_path: Path,
    value: object,
) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    destination.write_bytes(b"existing")
    records = [
        SeriesRecord(
            id="valid",
            time=_time(0),
            value=1.0,
        ),
        SeriesRecord(
            id="invalid",
            time=_time(1),
            value=value,
        ),
    ]

    with pytest.raises(TypeError, match="Series record"):
        write_series_rows(
            destination,
            (series_record_to_row(item) for item in records),
        )

    assert destination.read_bytes() == b"existing"
    assert list(tmp_path.iterdir()) == [destination]


def test_series_record_writer_rejects_non_scalar_entity_key(tmp_path: Path) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    row = {
        "id": "price",
        "time": _time(0).isoformat(),
        "kind": "record",
        "entity_key": [["A"]],
        "value": 1.0,
    }

    with pytest.raises(TypeError, match="Sample key field.*list"):
        write_series_rows(destination, [row])

    assert not destination.exists()
    assert list(tmp_path.iterdir()) == []


def test_series_record_row_rejects_non_tuple_entity_key() -> None:
    record = SeriesRecord(
        id="price",
        time=_time(0),
        value=1.0,
        entity_key=cast(Any, "AAPL"),
    )

    with pytest.raises(TypeError, match="price.*entity key must be a tuple"):
        series_record_to_row(record)


def test_series_record_row_rejects_non_list_sequence_values() -> None:
    sequence = SeriesSequence(
        time=_time(0),
        id="prices",
        values=cast(Any, (1.0, 2.0)),
    )

    with pytest.raises(TypeError, match="prices.*values must be a list"):
        series_record_to_row(sequence)


@pytest.mark.parametrize("kind", [None, "records"])
def test_series_record_writer_rejects_unknown_kind(
    tmp_path: Path,
    kind: object,
) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    row = {
        "id": "price",
        "time": _time(0).isoformat(),
        "kind": kind,
        "entity_key": [],
        "value": (1.0, 2.0),
    }

    with pytest.raises(TypeError, match="Unsupported series row kind"):
        write_series_rows(destination, [row])

    assert not destination.exists()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_series_record_writer_rejects_non_finite_values_atomically(
    tmp_path: Path,
    value: float,
) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    destination.write_bytes(b"existing")
    record = SeriesRecord(
        id="price",
        time=_time(0),
        value={"values": [1.0, value]},
    )
    row = series_record_to_row(record)

    with pytest.raises(ValueError, match="Out of range float values"):
        write_series_rows(destination, [row])

    assert row["value"] is record.value
    assert destination.read_bytes() == b"existing"
    assert list(tmp_path.iterdir()) == [destination]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_series_record_writer_rejects_non_finite_entity_keys(
    tmp_path: Path,
    value: float,
) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    record = SeriesRecord(
        id="price",
        time=_time(0),
        value=1.0,
        entity_key=(value,),
    )

    with pytest.raises(ValueError, match="finite floats"):
        write_series_rows(
            destination,
            [series_record_to_row(record)],
        )

    assert not destination.exists()


def test_series_record_writer_removes_temp_file_on_interrupt(tmp_path: Path) -> None:
    class InterruptedRows:
        def __init__(self) -> None:
            self.count = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.count == 0:
                self.count += 1
                return {
                    "id": "price",
                    "time": _time(0).isoformat(),
                    "kind": "record",
                    "entity_key": [],
                    "value": 1.0,
                }
            raise KeyboardInterrupt

    destination = tmp_path / "price.jsonl.gz"

    with pytest.raises(KeyboardInterrupt):
        write_series_rows(destination, InterruptedRows())

    assert not destination.exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("version", [None, 1, 2, 3, 4, 5, 6, 6.0, True])
def test_series_manifest_rejects_incompatible_version(
    tmp_path: Path,
    version: object,
) -> None:
    manifest = tmp_path / "manifest.json"
    payload = {} if version is None else {"version": version}
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="FORCE mode"):
        load_series_manifest(manifest)


def _valid_manifest() -> dict[str, Any]:
    return {
        "version": SERIES_MANIFEST_VERSION,
        "format": "jsonl.gz",
        "cadence": "1h",
        "sample_keys": ["security_id"],
        "sample_key_types": ["string"],
        "features": [
            {
                "id": "price",
                "path": "manifest.shards/features/000000.jsonl.gz",
                "rows": 1,
            }
        ],
        "targets": [],
    }


def test_cache_pruning_rejects_symlinked_manifest_parent(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    outside = tmp_path / "outside"
    cache_root = outside / "manifest.shards"
    stale = cache_root / "stale"
    stale.mkdir(parents=True)
    sentinel = stale / "keep"
    sentinel.write_text("keep", encoding="utf-8")
    payload = _valid_manifest()
    payload["features"][0]["path"] = "manifest.shards/current/features/000000.jsonl.gz"
    (outside / "manifest.json").write_text(json.dumps(payload), encoding="utf-8")
    (artifacts_root / "build").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="must stay under artifacts root"):
        prune_series_cache(
            artifacts_root / "build/manifest.json",
            artifacts_root,
        )

    assert sentinel.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize(
    ("change", "value"),
    [
        ("format", "jsonl"),
        ("cadence", "hourly"),
        ("sample_keys", ["security_id", "security_id"]),
        ("sample_key_types", []),
        ("rows", True),
        ("rows", -1),
        ("path", "../outside.jsonl.gz"),
    ],
)
def test_series_manifest_rejects_invalid_contract(
    tmp_path: Path,
    change: str,
    value: object,
) -> None:
    payload = _valid_manifest()
    if change in {"rows", "path"}:
        payload["features"][0][change] = value
    else:
        payload[change] = value
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid series manifest"):
        load_series_manifest(manifest)


def test_series_manifest_rejects_duplicate_shards(tmp_path: Path) -> None:
    payload = _valid_manifest()
    payload["features"] = [payload["features"][0], payload["features"][0]]
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid series manifest"):
        load_series_manifest(manifest)


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (
            {
                "id": "price",
                "time": "2024-01-01T00:00:00Z",
                "kind": "record",
                "entity_key": [],
            },
            "define 'value'",
        ),
        (
            {
                "id": "price",
                "time": "2024-01-01T00:00:00Z",
                "kind": "record",
                "value": None,
            },
            "list 'entity_key'",
        ),
        (
            {
                "id": "price",
                "time": "2024-01-01T00:00:00Z",
                "kind": "record",
                "entity_key": None,
                "value": None,
            },
            "list 'entity_key'",
        ),
        (
            {
                "id": "price",
                "time": "2024-01-01T00:00:00Z",
                "kind": "record",
                "entity_key": [["A"]],
                "value": None,
            },
            "invalid entity key",
        ),
    ],
    ids=["missing-value", "missing-entity-key", "null-entity-key", "nested-key"],
)
def test_series_record_reader_rejects_malformed_rows(
    tmp_path: Path,
    row: dict[str, object],
    message: str,
) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    with gzip.open(destination, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")

    with pytest.raises(ValueError, match=message):
        list(open_series(destination))


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_series_record_reader_rejects_non_standard_numbers(
    tmp_path: Path,
    value: str,
) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    with gzip.open(destination, "wt", encoding="utf-8") as handle:
        handle.write(
            '{"id":"price","time":"2024-01-01T00:00:00Z",'
            f'"kind":"record","entity_key":[],"value":{value}}}\n'
        )

    with pytest.raises(ValueError, match="Non-standard JSON"):
        list(open_series(destination))


def test_series_record_reader_rejects_fewer_rows_than_declared(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    write_series_rows(
        destination,
        [series_record_to_row(SeriesRecord("price", _time(0), 1.0))],
    )

    with pytest.raises(ValueError, match="declares 2 rows but contains 1"):
        list(open_series(destination, expected_rows=2))


def test_series_record_reader_rejects_extra_row_before_yielding_it(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    records = [SeriesRecord("price", _time(hour), float(hour)) for hour in range(2)]
    write_series_rows(destination, map(series_record_to_row, records))
    opened = open_series(destination, expected_rows=1)

    assert next(opened) == records[0]
    with pytest.raises(ValueError, match="more than its declared 1 rows"):
        next(opened)


def test_series_record_reader_does_not_verify_unread_rows(tmp_path: Path) -> None:
    destination = tmp_path / "inputs.jsonl.gz"
    record = SeriesRecord("price", _time(0), 1.0)
    write_series_rows(destination, [series_record_to_row(record)])
    opened = open_series(destination, expected_rows=2)

    assert next(opened) == record
    opened.close()
