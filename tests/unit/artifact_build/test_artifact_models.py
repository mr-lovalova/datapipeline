from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from datapipeline.artifacts.models import (
    ListVectorMetadataEntry,
    SampleMetadata,
    ScalarVectorMetadataEntry,
    VectorSchemaArtifact,
    Window,
)


def _time(hour: int) -> datetime:
    return datetime(2024, 1, 1, hour, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "change",
    [
        {"start": _time(2), "end": _time(1)},
        {"mode": "unknown"},
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


def test_list_metadata_lengths_must_match_fixed_cadence() -> None:
    with pytest.raises(ValidationError, match="list lengths"):
        ListVectorMetadataEntry.model_validate(
            {
                "id": "history",
                "base_id": "history",
                "kind": "list",
                "present_count": 2,
                "null_count": 0,
                "first_observed": _time(1),
                "last_observed": _time(2),
                "lengths": {"3": 1},
                "cadence": {"target": 3},
                "observed_elements": 3,
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


def test_schema_ids_share_one_feature_and_target_namespace() -> None:
    with pytest.raises(ValidationError, match="across features and targets"):
        VectorSchemaArtifact.model_validate(
            {
                "schema_version": 2,
                "features": [{"id": "price", "kind": "scalar"}],
                "targets": [{"id": "price", "kind": "scalar"}],
            }
        )
