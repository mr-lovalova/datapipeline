from typing import Any, Mapping, Optional, Sequence, Union, Iterator

from pydantic import BaseModel, Field, model_validator


class BaseRecordConfig(BaseModel):
    record_stream: str


class FeatureCombineConfig(BaseModel):
    inputs: Sequence[str] = Field(default_factory=list)
    transform: Mapping[str, Any]

    @model_validator(mode="after")
    def _validate_transform(self) -> "FeatureCombineConfig":
        if not isinstance(self.transform, Mapping) or len(self.transform) != 1:
            raise TypeError(
                "combine.transform must be a one-key mapping (e.g. {name: {...}})"
            )
        return self

    @property
    def transform_name(self) -> str:
        return next(iter(self.transform.keys()))

    @property
    def transform_params(self) -> Any:
        return next(iter(self.transform.values()))

    def build_clause(
        self,
        *,
        dependencies: Mapping[str, Iterator[Any]],
        target_id: str,
    ) -> dict[str, Any]:
        params = self.transform_params
        if params is None:
            params_dict: dict[str, Any] = {}
        elif isinstance(params, Mapping):
            params_dict = dict(params)
        else:
            raise TypeError("combine.transform expects mapping params or null")
        return {
            "name": self.transform_name,
            "init": params_dict,
            "inputs": dict(dependencies),
            "target_id": target_id,
        }


class FeatureRecordConfig(BaseRecordConfig):
    id: str
    scale: Optional[Union[bool, Mapping[str, Any]]] = Field(default=False)
    sequence: Optional[Mapping[str, Any]] = Field(default=None)
    combine: Optional[FeatureCombineConfig] = Field(default=None)
