from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector


def make_time_record(value: float | None, hour: int) -> TemporalRecord:
    return TemporalRecord(
        time=datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc),
        value=value,
    )


def make_feature_record(value: float | None, hour: int, feature_id: str) -> FeatureRecord:
    return FeatureRecord(
        record=make_time_record(value, hour),
        id=feature_id,
    )


def make_vector(group: int, values: dict[str, Any]) -> Sample:
    return Sample(key=(group,), features=Vector(values=values))


class StubVectorContext:
    def __init__(
        self,
        expected: list[str] | dict[str, list[str]],
        *,
        schema: dict[str, list[dict]] | None = None,
    ):
        if isinstance(expected, dict):
            self._expected_map = {k: list(v) for k, v in expected.items()}
        else:
            self._expected_map = {"features": list(expected)}
        self._schema_map = schema or {}
        self._cache: dict[str, Any] = {}
        self._metadata_doc: dict | None = None

    def load_expected_ids(self, *, payload: str = "features") -> list[str]:
        ids = list(self._expected_map.get(payload, []))
        self._cache[f"expected_ids:{payload}"] = list(ids)
        return ids

    def load_schema(self, *, payload: str = "features") -> list[dict]:
        entries = self._schema_map.get(payload)
        if entries is not None:
            snapshot = [dict(item) for item in entries]
        else:
            expected = self._expected_map.get(payload)
            if expected is None:
                snapshot = []
            else:
                snapshot = [{"id": fid} for fid in expected]
        self._cache[f"schema:{payload}"] = [dict(item) for item in snapshot]
        self._cache[f"expected_ids:{payload}"] = [
            entry["id"] for entry in snapshot if isinstance(entry.get("id"), str)
        ]
        return snapshot

    def set_metadata(self, doc: dict) -> None:
        self._metadata_doc = doc

    def require_artifact(self, spec) -> dict:
        if self._metadata_doc is None:
            raise RuntimeError("artifact missing")
        return self._metadata_doc
