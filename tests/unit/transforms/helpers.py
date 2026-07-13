from datetime import datetime, timezone
from typing import Any

from datapipeline.artifacts.models import (
    SchemaPayload,
    VectorSchemaArtifact,
)
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector


def make_time_record(value: float | None, hour: int) -> TemporalRecord:
    record = TemporalRecord(
        time=datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc),
    )
    setattr(record, "value", value)
    return record


def make_feature_record(value: float | None, hour: int, feature_id: str) -> FeatureRecord:
    return FeatureRecord(
        record=make_time_record(value, hour),
        id=feature_id,
        value=value,
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
        expected_map = expected if isinstance(expected, dict) else {"features": expected}
        schema = schema or {}
        features = schema.get(
            "features",
            [{"id": identifier} for identifier in expected_map.get("features", [])],
        )
        targets = schema.get(
            "targets",
            [{"id": identifier} for identifier in expected_map.get("targets", [])],
        )
        self._schema = VectorSchemaArtifact.model_validate(
            {
                "schema_version": 2,
                "features": [{"kind": "scalar", **entry} for entry in features],
                "targets": [{"kind": "scalar", **entry} for entry in targets],
            }
        )
        self._metadata_doc: dict | None = None

    def load_schema(self) -> VectorSchemaArtifact:
        return self._schema

    def remove_schema_ids(
        self,
        payload: SchemaPayload,
        identifiers: set[str],
    ) -> None:
        schema = self._schema
        if payload == "features":
            entries = schema.features
        elif payload == "targets":
            entries = schema.targets
        else:
            raise ValueError("schema payload must be 'features' or 'targets'")
        self._schema = schema.model_copy(
            update={
                payload: tuple(
                    entry for entry in entries if entry.id not in identifiers
                )
            }
        )

    def set_metadata(self, doc: dict) -> None:
        self._metadata_doc = doc

    def require_artifact(self, spec) -> dict:
        if self._metadata_doc is None:
            raise RuntimeError("artifact missing")
        return self._metadata_doc
