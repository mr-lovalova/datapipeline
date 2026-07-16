from collections.abc import Generator, Iterator, Sequence
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, Literal

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_pipeline
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.dataset.pipeline import run_dataset_pipeline
from datapipeline.runtime import Runtime
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
        samples = self._samples(limit)
        try:
            for sample in samples:
                yield sample.key, sample.features
        finally:
            samples.close()

    def iter_rows(
        self,
        *,
        limit: int | None = None,
        include_group: bool = True,
        group_format: GroupFormat = "mapping",
        group_column: str = "group",
        flatten_sequences: bool = False,
    ) -> Iterator[dict[str, Any]]:
        samples = self._samples(limit)
        sample_keys = self.dataset.sample.keys
        try:
            for sample in samples:
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
        finally:
            samples.close()

    def _samples(self, limit: int | None) -> Generator[Sample, None, None]:
        features = list(self.dataset.features)
        if not features:
            raise ValueError(
                "Dataset does not define any features. Configure at least one feature "
                "stream before attempting to stream vectors."
            )
        target_cfgs = list(self.dataset.targets)
        context = PipelineContext(self.runtime)
        samples = run_dataset_pipeline(
            context,
            features,
            self.dataset.sample.cadence,
            target_configs=target_cfgs,
            sample_keys=self.dataset.sample.keys,
        )
        try:
            if limit is None:
                yield from samples
            else:
                yield from islice(samples, limit)
        finally:
            samples.close()
