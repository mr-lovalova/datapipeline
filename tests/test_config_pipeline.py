from __future__ import annotations

from datapipeline.config.dataset.feature import FeatureRecordConfig


def test_feature_config_simple_fields():
    config = FeatureRecordConfig.model_validate(
        {
            "id": "time",
            "stream": "time_linear",
            "filters": {"operator": "ge", "field": "time", "value": "2021-01-01T00:00:00Z"},
            "lag": "1h",
            "scale": {"with_mean": True, "with_std": True},
            "sequence": {"size": 5, "stride": 1},
        }
    )

    assert config.id == "time"
    assert config.stream == "time_linear"
    assert isinstance(config.filters, dict)
    assert config.filters["operator"] == "ge"
    assert config.filters["field"] == "time"
    assert config.lag == "1h"
    assert isinstance(config.scale, dict)
    assert isinstance(config.sequence, dict)
