from typing import Literal

from datapipeline.config.dataset.dataset import (
    RecordDatasetConfig,
    FeatureDatasetConfig,
)
from datapipeline.services.bootstrap import _load_by_key, _globals, _interpolate

Stage = Literal["records", "features", "vectors"]


def _normalize_dataset_doc(doc):
    if not isinstance(doc, dict):
        return doc
    normalized = dict(doc)
    for key in ("features", "targets"):
        if normalized.get(key) is None:
            normalized[key] = []
    return normalized


def load_dataset(project_yaml, stage: Stage):
    raw = _load_by_key(project_yaml, "dataset")
    vars_ = _globals(project_yaml)
    if vars_:
        raw = _interpolate(raw, vars_)
    ds_doc = _normalize_dataset_doc(raw)

    if stage == "records":
        return RecordDatasetConfig.model_validate(ds_doc)
    elif stage == "features":
        return FeatureDatasetConfig.model_validate(ds_doc)
    elif stage == "vectors":
        return FeatureDatasetConfig.model_validate(ds_doc)
    else:
        raise ValueError(f"Unknown stage: {stage}")
