import pytest
from pydantic import ValidationError

from datapipeline.config.dataset.postprocess import CoverageConfig, PostprocessConfig


def test_postprocess_parses_selection_and_sample_filters() -> None:
    config = PostprocessConfig.model_validate(
        {
            "columns": {"features": {"threshold": 0.8, "ids": ["price"]}},
            "samples": {"targets": {"threshold": 1.0}},
        }
    )

    assert config.columns.features == CoverageConfig(
        threshold=0.8,
        ids=["price"],
    )
    assert config.samples.targets == CoverageConfig(threshold=1.0)


def test_postprocess_rejects_value_transforms() -> None:
    with pytest.raises(ValidationError):
        PostprocessConfig.model_validate(
            {
                "columns": {
                    "transforms": [
                        {"operation": "fill", "window": 5},
                    ]
                }
            }
        )


@pytest.mark.parametrize("ids", [[], ["price", "price"]])
def test_postprocess_rejects_invalid_id_selection(ids: list[str]) -> None:
    with pytest.raises(ValidationError):
        PostprocessConfig.model_validate(
            {"columns": {"features": {"threshold": 0.8, "ids": ids}}}
        )
