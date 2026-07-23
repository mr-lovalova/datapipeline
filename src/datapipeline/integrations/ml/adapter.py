from collections.abc import Generator, Sequence
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Any, Literal

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_pipeline
from datapipeline.artifacts.models import VectorMetadataEntry
from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.dataset.dataset import DatasetConfig
from datapipeline.config.dataset.split import (
    DatasetFold,
    SplitConfig,
    resolve_fold_output,
    split_output_ids,
)
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.dataset.postprocess import build_postprocess_plan
from datapipeline.pipelines.dataset.pipeline import (
    run_dataset_pipeline,
    run_fold_dataset_pipeline,
    run_scaled_dataset_pipeline,
)
from datapipeline.runtime import Runtime
from datapipeline.services.project_definition import load_project_definition
from datapipeline.services.runtime_compiler import compile_runtime

GroupFormat = Literal["mapping", "tuple", "list", "flat"]


def _entry_columns(
    entries: Sequence[VectorMetadataEntry],
    flatten_sequences: bool,
) -> list[str]:
    columns: list[str] = []
    for entry in entries:
        if flatten_sequences and entry.kind == "list":
            columns.extend(f"{entry.id}[{index}]" for index in range(entry.length))
        else:
            columns.append(entry.id)
    return columns


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

    dataset: DatasetConfig
    runtime: Runtime
    output_id: str | None = None

    def __post_init__(self) -> None:
        split = self.dataset.split
        if split is None:
            if self.output_id is not None:
                raise ValueError(
                    "output_id is only valid when dataset.split is configured."
                )
            return
        self._fold_output(split)

    @classmethod
    def from_project(
        cls,
        project_yaml: str | Path,
        output_id: str | None = None,
    ) -> "VectorAdapter":
        project_path = Path(project_yaml)
        definition = load_project_definition(project_path)
        runtime = compile_runtime(definition)
        hydrate_runtime_artifacts_for_pipeline(runtime, definition)
        return cls(
            dataset=definition.dataset,
            runtime=runtime,
            output_id=output_id,
        )

    def stream(
        self,
        *,
        limit: int | None = None,
    ) -> Generator[tuple[Sequence[Any], Vector], None, None]:
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
    ) -> Generator[dict[str, Any], None, None]:
        if include_group or flatten_sequences:
            feature_columns, target_columns = self.row_columns(flatten_sequences)
            if include_group and (
                group_column in feature_columns or group_column in target_columns
            ):
                raise ValueError(
                    f"Group column {group_column!r} conflicts with a dataset row "
                    "column."
                )
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

    def row_columns(
        self,
        flatten_sequences: bool = False,
    ) -> tuple[list[str], list[str]]:
        context = PipelineContext(self.runtime)
        postprocess = build_postprocess_plan(context)
        feature_columns = _entry_columns(
            postprocess.feature_entries,
            flatten_sequences,
        )
        target_columns = _entry_columns(
            postprocess.target_entries,
            flatten_sequences,
        )
        if flatten_sequences:
            seen: set[str] = set()
            for column in [*feature_columns, *target_columns]:
                if column in seen:
                    raise ValueError(
                        f"Flattened row column {column!r} is produced by multiple "
                        "postprocessed metadata entries."
                    )
                seen.add(column)
        return feature_columns, target_columns

    def _samples(self, limit: int | None) -> Generator[Sample, None, None]:
        features = list(self.dataset.features)
        if not features:
            raise ValueError(
                "Dataset does not define any features. Configure at least one feature "
                "stream before attempting to stream vectors."
            )
        target_cfgs = list(self.dataset.targets)
        context = PipelineContext(self.runtime)
        split = self.dataset.split
        if split is not None:
            fold, labels = self._fold_output(split)
            samples = run_fold_dataset_pipeline(
                context,
                features,
                self.dataset.sample.cadence,
                fold,
                labels,
                target_configs=target_cfgs,
                sample_keys=self.dataset.sample.keys,
            )
        elif dataset_requires_scaler(self.dataset):
            samples = run_scaled_dataset_pipeline(
                context,
                features,
                self.dataset.sample.cadence,
                target_configs=target_cfgs,
                sample_keys=self.dataset.sample.keys,
            )
        else:
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

    def _fold_output(
        self,
        split: SplitConfig,
    ) -> tuple[DatasetFold, tuple[str, ...]]:
        output_id = self.output_id
        available = split_output_ids(split)
        if output_id is None:
            raise ValueError(
                "Dataset defines split outputs; pass output_id with one of: "
                + ", ".join(available)
            )
        try:
            return resolve_fold_output(split, output_id)
        except KeyError as exc:
            raise ValueError(
                f"Dataset output {output_id!r} is not defined; choose one of: "
                + ", ".join(available)
            ) from exc
