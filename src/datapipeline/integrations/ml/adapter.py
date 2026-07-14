from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, Literal

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.full.nodes import apply_postprocess
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.vector.pipeline import build_vector_pipeline
from datapipeline.runtime import Runtime
from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_pipeline
from datapipeline.services.pipeline import load_pipeline
from datapipeline.services.runtime_compiler import compile_runtime

GroupFormat = Literal["mapping", "tuple", "list", "flat"]


def _normalize_group(
    group_key: Sequence[Any],
    sample_keys: Sequence[str],
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
    fields = ["time", *sample_keys]
    if len(group_key) != len(fields):
        raise ValueError(
            f"Group has {len(group_key)} values; expected {len(fields)} "
            "for time and sample keys."
        )
    return {field: value for field, value in zip(fields, group_key)}


def _ensure_features(dataset: FeatureDatasetConfig) -> list[FeatureRecordConfig]:
    features = list(dataset.features)
    if not features:
        raise ValueError(
            "Dataset does not define any features. Configure at least one feature "
            "stream before attempting to stream vectors.",
        )
    return features


@dataclass
class VectorAdapter:
    """Load a project once and provide ML-friendly iterators."""

    dataset: FeatureDatasetConfig
    runtime: Runtime

    @classmethod
    def from_project(
        cls,
        project_yaml: str | Path,
    ) -> "VectorAdapter":
        project_path = Path(project_yaml)
        definition = load_pipeline(project_path)
        runtime = compile_runtime(definition)
        hydrate_runtime_artifacts_for_pipeline(runtime, definition)
        return cls(dataset=definition.dataset, runtime=runtime)

    def stream(
        self,
        *,
        limit: int | None = None,
    ) -> Iterator[tuple[Sequence[Any], Vector]]:
        features = _ensure_features(self.dataset)
        target_cfgs = list(self.dataset.targets)
        context = PipelineContext(self.runtime)
        vectors = build_vector_pipeline(
            context,
            features,
            self.dataset.sample.cadence,
            target_configs=target_cfgs,
            sample_keys=self.dataset.sample.keys,
        )
        base_stream = apply_postprocess(context, vectors)
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
        features = _ensure_features(self.dataset)
        target_cfgs = list(self.dataset.targets)
        context = PipelineContext(self.runtime)
        vectors = build_vector_pipeline(
            context,
            features,
            self.dataset.sample.cadence,
            target_configs=target_cfgs,
            sample_keys=self.dataset.sample.keys,
        )
        base_stream = apply_postprocess(context, vectors)
        if limit is not None:
            base_stream = islice(base_stream, limit)
        sample_keys = self.dataset.sample.keys

        def _rows() -> Iterator[dict[str, Any]]:
            for sample in base_stream:
                row: dict[str, Any] = {}
                if include_group:
                    row[group_column] = _normalize_group(
                        sample.key, sample_keys, group_format
                    )
                vectors = [sample.features]
                if sample.targets is not None:
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
