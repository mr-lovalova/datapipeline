import pytest
from pydantic import ValidationError

from datapipeline.config.split import HashSplitConfig


def test_hash_split_accepts_group_key() -> None:
    cfg = HashSplitConfig(ratios={"train": 1.0}, key="group")

    assert cfg.key == "group"


def test_hash_split_accepts_feature_key() -> None:
    cfg = HashSplitConfig(ratios={"train": 1.0}, key="feature:ticker")

    assert cfg.key == "feature:ticker"


def test_hash_split_rejects_unknown_key() -> None:
    with pytest.raises(ValidationError, match="hash split key must be"):
        HashSplitConfig(ratios={"train": 1.0}, key="unknown")


def test_hash_split_rejects_empty_feature_key() -> None:
    with pytest.raises(ValidationError, match="include a feature id"):
        HashSplitConfig(ratios={"train": 1.0}, key="feature:")
