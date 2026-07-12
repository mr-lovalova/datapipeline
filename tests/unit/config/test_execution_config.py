import pytest
from pydantic import ValidationError

from datapipeline.config.execution import ExecutionConfig


@pytest.mark.parametrize("value", [0, True, 1.5, "100"])
def test_sort_batch_records_requires_a_positive_integer(value) -> None:
    with pytest.raises(ValidationError):
        ExecutionConfig(sort_batch_records=value)


def test_execution_config_rejects_unknown_settings() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ExecutionConfig.model_validate({"workers": 4})
