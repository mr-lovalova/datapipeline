from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.services.bootstrap.config import _globals, _interpolate, _load_by_key


def load_dataset(project_yaml) -> FeatureDatasetConfig:
    raw = _load_by_key(project_yaml, "dataset")
    vars_ = _globals(project_yaml)
    if vars_:
        raw = _interpolate(raw, vars_)
    return FeatureDatasetConfig.model_validate(raw)
