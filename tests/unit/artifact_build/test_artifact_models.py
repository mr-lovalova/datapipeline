from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from datapipeline.artifacts.models import (
    ListVectorMetadataEntry,
    SampleMetadata,
    ScalarVectorMetadataEntry,
    VectorMetadata,
    Window,
)


def _time(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "change",
    [
        {"start": _time(2), "end": _time(1)},
        {"mode": "unknown"},
        {"mode": "relaxed"},
        {"size": 0},
    ],
)
def test_metadata_window_rejects_invalid_contract(change: dict[str, object]) -> None:
    payload = {
        "start": _time(1),
        "end": _time(2),
        "mode": "union",
        "size": 2,
        **change,
    }

    with pytest.raises(ValidationError):
        Window.model_validate(payload)


@pytest.mark.parametrize(
    ("first", "last"),
    [
        (_time(1), None),
        (None, _time(1)),
        (_time(2), _time(1)),
    ],
)
def test_vector_metadata_rejects_invalid_observation_bounds(first, last) -> None:
    with pytest.raises(ValidationError, match="observation bounds|first_observed"):
        ScalarVectorMetadataEntry(
            id="price",
            base_id="price",
            kind="scalar",
            present_count=1,
            null_count=0,
            first_observed=first,
            last_observed=last,
        )


@pytest.mark.parametrize("length", [0, -1, True, 2.0, "2"])
def test_list_metadata_requires_a_positive_integer_length(length: object) -> None:
    with pytest.raises(ValidationError, match="length"):
        ListVectorMetadataEntry.model_validate(
            {
                "id": "history",
                "base_id": "history",
                "kind": "list",
                "present_count": 2,
                "null_count": 0,
                "first_observed": _time(1),
                "last_observed": _time(2),
                "length": length,
                "observed_elements": 3,
            }
        )


def test_list_metadata_rejects_observed_elements_above_capacity() -> None:
    with pytest.raises(ValidationError, match="sequence capacity"):
        ListVectorMetadataEntry.model_validate(
            {
                "id": "history",
                "base_id": "history",
                "kind": "list",
                "present_count": 3,
                "null_count": 1,
                "length": 2,
                "observed_elements": 5,
            }
        )


@pytest.mark.parametrize(
    "domain",
    [
        [{"key": [None], "start": _time(1), "end": _time(1)}],
        [{"key": [float("nan")], "start": _time(1), "end": _time(1)}],
        [
            {"key": [True], "start": _time(1), "end": _time(1)},
            {"key": [1], "start": _time(2), "end": _time(2)},
        ],
    ],
)
def test_sample_metadata_rejects_unstable_identity_values(domain) -> None:
    with pytest.raises(ValidationError, match="Sample key field"):
        SampleMetadata.model_validate(
            {"cadence": "1h", "keys": ["security_id"], "domain": domain}
        )


def _metadata_payload() -> dict[str, object]:
    return {
        "schema_version": 3,
        "features": [],
        "targets": [],
        "counts": {"feature_vectors": 1, "target_vectors": 1},
    }


@pytest.mark.parametrize("version", [1, 2, 4, "3"])
def test_vector_metadata_rejects_unsupported_schema_versions(version: object) -> None:
    with pytest.raises(ValidationError, match="schema_version"):
        VectorMetadata.model_validate(
            {**_metadata_payload(), "schema_version": version}
        )


def test_metadata_ids_share_one_feature_and_target_namespace() -> None:
    with pytest.raises(ValidationError, match="across features and targets"):
        VectorMetadata.model_validate(
            {
                **_metadata_payload(),
                "schema_version": 3,
                "features": [
                    {
                        "id": "price",
                        "base_id": "price",
                        "kind": "scalar",
                        "present_count": 1,
                        "null_count": 0,
                    }
                ],
                "targets": [
                    {
                        "id": "price",
                        "base_id": "return",
                        "kind": "scalar",
                        "present_count": 1,
                        "null_count": 0,
                    }
                ],
            }
        )
