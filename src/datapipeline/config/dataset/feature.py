"""Dataset feature configuration models."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, List, Optional, Sequence, Union

from pydantic import BaseModel, Field, root_validator


STAGE_ORDER: Sequence[str] = ("record", "feature", "sequence")


def _ensure_feature_id_alias(values: dict[str, Any]) -> dict[str, Any]:
    """Support ``id`` as shorthand for ``feature_id`` in the DSL."""

    if "feature_id" not in values and "id" in values:
        values["feature_id"] = values.pop("id")
    return values


def _extract_transform_params(block: dict[str, Any]) -> tuple[str, Any]:
    """Return the transform name and parameters for any stage."""

    config = dict(block)
    if "transform" not in config:
        raise ValueError("Pipeline step is missing required 'transform' key")

    transform = config.pop("transform")
    args = config.pop("args", None)

    if config and args is not None:
        raise ValueError(
            "Use either 'args' for positional parameters or keyword fields, not both"
        )

    params: Any
    if config:
        params = config
    else:
        params = args
    return transform, params


def _extract_filter_params(block: dict[str, Any]) -> tuple[str, Mapping[str, Any]]:
    """Return the filter name and field mapping for record steps."""

    config = dict(block)
    if "filter" not in config:
        raise ValueError("Record pipeline step requires 'filter' or 'transform'")

    name = config.pop("filter")
    args = config.pop("args", None)
    if config:
        raise ValueError("Unexpected keys for record filter step: " + ", ".join(config))
    if not isinstance(args, Mapping):
        raise TypeError("Record filter 'args' must be a mapping of field -> value")
    return name, args


def _expand_pipeline(
    values: dict[str, Any], *, allow_feature: bool
) -> dict[str, Any]:
    """Translate the list-based pipeline DSL into legacy config fields."""

    steps = values.get("transforms")

    if not steps:
        return values

    values.pop("transforms", None)

    if not isinstance(steps, list):
        raise TypeError("Pipeline must be a list of stage steps")

    for key in ("record_filters", "record_transforms", "feature_transforms", "sequence_transforms"):
        if values.get(key):
            raise ValueError("Use either the pipeline DSL or legacy transform fields, not both")

    record_filters: list[dict[str, Any]] = []
    record_transforms: list[dict[str, Any]] = []
    feature_transforms: list[dict[str, Any]] = []
    sequence_transforms: list[dict[str, Any]] = []

    highest_stage_index = 0

    for step in steps:
        if not isinstance(step, Mapping) or len(step) != 1:
            raise TypeError("Each pipeline entry must be a single-key mapping")

        stage, block_raw = next(iter(step.items()))
        if stage not in STAGE_ORDER:
            raise ValueError(f"Unknown pipeline stage: {stage}")

        stage_index = STAGE_ORDER.index(stage)
        if stage_index < highest_stage_index:
            raise ValueError("Pipeline stages must be in record -> feature -> sequence order")
        highest_stage_index = stage_index

        block = dict(block_raw or {})

        if stage == "record":
            if "filter" in block:
                name, mapping = _extract_filter_params(block)
                record_filters.append({name: mapping})
            elif "transform" in block:
                name, params = _extract_transform_params(block)
                record_transforms.append({name: params})
            else:
                raise ValueError("Record step requires either 'filter' or 'transform'")
        elif stage == "feature":
            if not allow_feature:
                continue
            name, params = _extract_transform_params(block)
            feature_transforms.append({name: params})
        else:  # sequence
            if not allow_feature:
                continue
            name, params = _extract_transform_params(block)
            sequence_transforms.append({name: params})

    # Normalize empty lists to None to match legacy defaults
    if record_filters:
        values["record_filters"] = record_filters
    if record_transforms:
        values["record_transforms"] = record_transforms
    if feature_transforms:
        values["feature_transforms"] = feature_transforms
    if sequence_transforms:
        values["sequence_transforms"] = sequence_transforms
    return values


class BaseRecordConfig(BaseModel):
    """Configuration shared by record- and feature-level pipelines."""

    stream: str
    feature_id: str
    partition_by: Optional[Union[str, List[str]]] = None
    record_filters: Optional[List[Any]] = Field(default=None)
    record_transforms: Optional[List[Any]] = Field(default=None)

    @root_validator(pre=True)
    def _normalize_pipeline(cls, values: dict[str, Any]) -> dict[str, Any]:
        data = _ensure_feature_id_alias(dict(values))
        if cls is BaseRecordConfig:
            return _expand_pipeline(data, allow_feature=False)
        return data


class FeatureRecordConfig(BaseRecordConfig):
    """Configuration for feature-level pipelines (post record stage)."""

    feature_transforms: Optional[List[Any]] = None
    sequence_transforms: Optional[List[Any]] = None

    @root_validator(pre=True)
    def _normalize_feature_pipeline(cls, values: dict[str, Any]) -> dict[str, Any]:
        data = _ensure_feature_id_alias(dict(values))
        return _expand_pipeline(data, allow_feature=True)
