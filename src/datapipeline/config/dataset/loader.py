
from typing import Literal
from datapipeline.config.dataset.dataset import RecordDatasetConfig, FeatureDatasetConfig
from datapipeline.services.bootstrap import _load_by_key  # your helper

Stage = Literal["records", "features", "vectors"]


def load_dataset(project_yaml, stage: Stage):
    ds_doc = _load_by_key(project_yaml, "dataset")

    if stage == "records":
        return RecordDatasetConfig.model_validate(ds_doc)
    elif stage == "features":
        return FeatureDatasetConfig.model_validate(ds_doc)
    elif stage == "vectors":
        return FeatureDatasetConfig.model_validate(ds_doc)
    else:
        raise ValueError(f"Unknown stage: {stage}")
