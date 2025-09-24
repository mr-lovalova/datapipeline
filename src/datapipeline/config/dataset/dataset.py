from collections.abc import Mapping
from typing import Any, List

from pydantic import BaseModel, Field, root_validator
from datapipeline.config.dataset.group_by import GroupBy
from datapipeline.config.dataset.feature import BaseRecordConfig, FeatureRecordConfig


class RecordDatasetConfig(BaseModel):
    features: List[BaseRecordConfig] = Field(default_factory=list)
    targets:  List[BaseRecordConfig] = Field(default_factory=list)


class FeatureDatasetConfig(BaseModel):
    group_by: GroupBy
    features: List[FeatureRecordConfig] = Field(default_factory=list)
    targets:  List[FeatureRecordConfig] = Field(default_factory=list)
    vector_transforms: List[Any] | None = Field(default=None)

    @root_validator(pre=True)
    def _normalize_vector_pipeline(cls, values: dict[str, Any]) -> dict[str, Any]:
        cleaning = values.get("vector_cleaning")
        if not cleaning:
            return values

        if values.get("vector_transforms"):
            raise ValueError("Use either vector_pipeline or vector_transforms, not both")
        if not isinstance(cleaning, list):
            raise TypeError("vector_pipeline must be declared as a list of transforms")

        transforms: list[dict[str, Any]] = []
        for item in cleaning:
            if not isinstance(item, Mapping):
                raise TypeError("Each vector_pipeline entry must be a mapping")
            if "transform" not in item:
                raise ValueError("Vector pipeline step requires a 'transform' key")
            name = item["transform"]
            config = {k: v for k, v in item.items() if k not in {"transform", "args"}}
            args = item.get("args")
            if config and args is not None:
                raise ValueError("Vector transform cannot mix 'args' with keyword parameters")
            params: Any
            if config:
                params = config
            else:
                params = args
            transforms.append({name: params})

        if transforms:
            values["vector_transforms"] = transforms
        values.pop("vector_cleaning", None)
        return values


class VectorDatasetConfig(FeatureDatasetConfig):
    pass
