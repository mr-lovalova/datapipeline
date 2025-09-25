from __future__ import annotations

from datapipeline.config.dataset.feature import FeatureRecordConfig


def _group_by_hour():
    return {
        "keys": [
            {
                "type": "time",
                "field": "time",
                "resolution": "1h",
            }
        ]
    }


def test_feature_pipeline_expands_into_legacy_fields():
    config = FeatureRecordConfig.model_validate(
        {
            "id": "time",
            "stream": "time_linear",
            "transforms": [
                {"record": {"filter": "ge", "args": {"time": "2021-01-01T00:00:00Z"}}},
                {"record": {"transform": "time_lag", "args": "1h"}},
                {
                    "feature": {
                        "transform": "standard_scale",
                        "with_mean": True,
                        "with_std": True,
                    }
                },
                {
                    "sequence": {
                        "transform": "time_window",
                        "size": 5,
                        "stride": 1,
                    }
                },
            ],
        }
    )

    assert config.feature_id == "time"
    assert config.record_filters == [
        {"ge": {"time": "2021-01-01T00:00:00Z"}}
    ]
    assert config.record_transforms == [{"time_lag": "1h"}]
    assert config.feature_transforms == [
        {"standard_scale": {"with_mean": True, "with_std": True}}
    ]
    assert config.sequence_transforms == [
        {"time_window": {"size": 5, "stride": 1}}
    ]
