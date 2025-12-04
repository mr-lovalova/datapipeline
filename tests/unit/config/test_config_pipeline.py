from __future__ import annotations

from datapipeline.config.dataset.feature import FeatureRecordConfig


def test_feature_config_simple_fields():
    config = FeatureRecordConfig.model_validate(
        {
            "id": "time",
            "record_stream": "time_linear",
            "scale": {"with_mean": True, "with_std": True},
            "sequence": {"size": 5, "stride": 1},
        }
    )

    assert config.id == "time"
    assert config.record_stream == "time_linear"
    assert isinstance(config.scale, dict)
    assert isinstance(config.sequence, dict)
