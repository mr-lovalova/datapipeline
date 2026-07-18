import pytest
from pydantic import ValidationError

from datapipeline.config.dataset.variable import VariableConfig, SequenceConfig


def test_feature_config_simple_fields():
    config = VariableConfig.model_validate(
        {
            "id": "time",
            "stream": "time_linear",
            "field": "value",
            "scale": True,
            "sequence": {"size": 5, "stride": 1},
        }
    )

    assert config.id == "time"
    assert config.stream == "time_linear"
    assert config.scale is True
    assert config.sequence == SequenceConfig(size=5, stride=1)


def test_feature_config_rejects_record_stream() -> None:
    with pytest.raises(ValidationError, match="record_stream"):
        VariableConfig.model_validate(
            {
                "id": "time",
                "record_stream": "time_linear",
                "field": "value",
            }
        )


def test_feature_config_rejects_reserved_partition_separator() -> None:
    with pytest.raises(ValidationError, match="reserved separator"):
        VariableConfig.model_validate(
            {
                "id": "price__region",
                "stream": "prices",
                "field": "close",
            }
        )
