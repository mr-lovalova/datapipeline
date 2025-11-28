from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, Literal

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.context import PipelineContext
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap

GroupFormat = Literal["mapping", "tuple", "list", "flat"]


def _normalize_group(
    group_key: Sequence[Any],
    group_by: str,
    fmt: GroupFormat,
) -> Any:
    if fmt == "tuple":
        return tuple(group_key)
    if fmt == "list":
        return list(group_key)
    if fmt == "flat":
        if len(group_key) != 1:
            raise ValueError(
                "group_format='flat' requires exactly one group key value",
            )
        return group_key[0]
    # Default: mapping of field names -> values. With simplified GroupBy, use 'time'.
    fields = ["time"]
    return {field: value for field, value in zip(fields, group_key)}


def _ensure_features(dataset: FeatureDatasetConfig) -> list:
    features = list(dataset.features or [])
    if not features:
        raise ValueError(
            "Dataset does not define any features. Configure at least one feature "
            "stream before attempting to stream vectors.",
        )
    return features


@dataclass
class VectorAdapter:
    """Bootstrap a project once and provide ML-friendly iterators."""

    dataset: FeatureDatasetConfig
    runtime: Runtime

    @classmethod
    def from_project(
        cls,
        project_yaml: str | Path,
    ) -> "VectorAdapter":
        project_path = Path(project_yaml)
        dataset = load_dataset(project_path, "vectors")
        runtime = bootstrap(project_path)
        return cls(dataset=dataset, runtime=runtime)

    def stream(
        self,
        *,
        limit: int | None = None,
    ) -> Iterator[tuple[Sequence[Any], Vector]]:
        features = list(_ensure_features(self.dataset))
        target_cfgs = list(getattr(self.dataset, "targets", []) or [])
        context = PipelineContext(self.runtime)
        vectors = build_vector_pipeline(
            context,
            features,
            self.dataset.group_by,
            target_configs=target_cfgs,
        )
        base_stream = post_process(context, vectors)
        sample_iter = base_stream
        if limit is not None:
            sample_iter = islice(sample_iter, limit)
        return ((sample.key, sample.features) for sample in sample_iter)

    def iter_rows(
        self,
        *,
        limit: int | None = None,
        include_group: bool = True,
        group_format: GroupFormat = "mapping",
        group_column: str = "group",
        flatten_sequences: bool = False,
    ) -> Iterator[dict[str, Any]]:
        features = list(_ensure_features(self.dataset))
        target_cfgs = list(getattr(self.dataset, "targets", []) or [])
        context = PipelineContext(self.runtime)
        vectors = build_vector_pipeline(
            context,
            features,
            self.dataset.group_by,
            target_configs=target_cfgs,
        )
        base_stream = post_process(context, vectors)
        if limit is not None:
            base_stream = islice(base_stream, limit)
        group_by = self.dataset.group_by

        def _rows() -> Iterator[dict[str, Any]]:
            for sample in base_stream:
                row: dict[str, Any] = {}
                if include_group:
                    row[group_column] = _normalize_group(
                        sample.key, group_by, group_format
                    )
                vectors = [sample.features]
                if sample.targets:
                    vectors.append(sample.targets)
                for vector in vectors:
                    for feature_id, value in vector.values.items():
                        if flatten_sequences and isinstance(value, list):
                            for idx, item in enumerate(value):
                                row[f"{feature_id}[{idx}]"] = item
                        else:
                            row[feature_id] = value
                yield row

        return _rows()


__all__ = ["GroupFormat", "VectorAdapter", "_normalize_group"]
