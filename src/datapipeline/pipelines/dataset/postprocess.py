from collections.abc import Collection, Iterator, Sequence
from dataclasses import dataclass

from datapipeline.artifacts.models import ListVectorMetadataEntry, VectorMetadataEntry
from datapipeline.artifacts.registry import VECTOR_METADATA_SPEC
from datapipeline.domain.sample import Sample
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.pipeline import Stage
from datapipeline.pipelines.dataset.split import HashLabeler, TimeLabeler
from datapipeline.transforms.vector.drop.horizontal import (
    DropSamplesTransform,
    DropTargetSamplesTransform,
)
from datapipeline.transforms.vector.drop.vertical import (
    ColumnCoverage,
    SelectFeaturesTransform,
    SelectTargetsTransform,
)
from datapipeline.transforms.vector.normalize import (
    NormalizeFeaturesTransform,
    NormalizeTargetsTransform,
)


@dataclass(frozen=True)
class PostprocessPlan:
    feature_entries: tuple[VectorMetadataEntry, ...]
    target_entries: tuple[VectorMetadataEntry, ...]
    stages: tuple[Stage, ...]

    def apply(self, samples: Iterator[Sample]) -> Iterator[Sample]:
        stream = samples
        for stage in self.stages:
            stream = iter(stage.apply(stream))
        return stream


def select_fold_samples(
    labeler: HashLabeler | TimeLabeler,
    labels: Collection[str],
    samples: Iterator[Sample],
) -> Iterator[Sample]:
    included = frozenset(labels)
    for sample in samples:
        if labeler.label(sample.key) in included:
            yield sample


def build_postprocess_plan(context: PipelineContext) -> PostprocessPlan:
    metadata = context.require_artifact(VECTOR_METADATA_SPEC)
    if not metadata.features:
        raise RuntimeError(
            "Metadata has no feature entries. Rebuild build/metadata.json."
        )

    config = context.runtime.dataset.postprocess
    feature_entries = metadata.features
    target_entries = metadata.targets
    stages: list[Stage] = []

    if config.columns.features is not None:
        policy = config.columns.features
        feature_selection = SelectFeaturesTransform(
            [entry.id for entry in feature_entries],
            _column_coverage(
                feature_entries,
                policy.ids,
            ),
            metadata.counts.feature_vectors,
            policy.threshold,
            policy.ids,
        )
        feature_entries = _retained_entries(
            feature_entries,
            feature_selection.retained_ids,
        )
        if not feature_entries:
            raise ValueError("Feature selection removed every metadata entry.")
        stages.append(
            Stage(
                name="select_features",
                apply=feature_selection.apply,
            )
        )

    if config.columns.targets is not None:
        policy = config.columns.targets
        target_selection = SelectTargetsTransform(
            [entry.id for entry in target_entries],
            _column_coverage(
                target_entries,
                policy.ids,
            ),
            metadata.counts.target_vectors,
            policy.threshold,
            policy.ids,
        )
        target_entries = _retained_entries(
            target_entries,
            target_selection.retained_ids,
        )
        if not target_entries:
            raise ValueError("Target selection removed every metadata entry.")
        stages.append(
            Stage(
                name="select_targets",
                apply=target_selection.apply,
            )
        )

    stages.append(
        Stage(
            name="normalize_features",
            apply=NormalizeFeaturesTransform(feature_entries).apply,
        )
    )

    if target_entries:
        target_normalizer = NormalizeTargetsTransform(target_entries).apply
        target_stage_name = "normalize_targets"
    else:
        target_normalizer = _reject_undeclared_targets
        target_stage_name = "reject_undeclared_targets"
    stages.append(
        Stage(
            name=target_stage_name,
            apply=target_normalizer,
        )
    )

    if config.samples.features is not None:
        feature_filter = DropSamplesTransform(
            [entry.id for entry in feature_entries],
            config.samples.features.threshold,
            config.samples.features.ids,
        )
        stages.append(
            Stage(
                name="filter_samples_by_features",
                apply=feature_filter.apply,
            )
        )

    if config.samples.targets is not None:
        target_filter = DropTargetSamplesTransform(
            [entry.id for entry in target_entries],
            config.samples.targets.threshold,
            config.samples.targets.ids,
        )
        stages.append(
            Stage(
                name="filter_samples_by_targets",
                apply=target_filter.apply,
            )
        )

    return PostprocessPlan(
        feature_entries=feature_entries,
        target_entries=target_entries,
        stages=tuple(stages),
    )


def apply_postprocess(
    context: PipelineContext,
    samples: Iterator[Sample],
) -> Iterator[Sample]:
    """Apply the same ordered postprocess stages used by the dataset pipeline."""

    return build_postprocess_plan(context).apply(samples)


def _column_coverage(
    metadata_entries: Sequence[VectorMetadataEntry],
    selected_ids: Sequence[str] | None,
) -> tuple[ColumnCoverage, ...]:
    selected = None if selected_ids is None else frozenset(selected_ids)
    coverage: list[ColumnCoverage] = []

    for entry in metadata_entries:
        if selected is not None and entry.id not in selected:
            continue

        if isinstance(entry, ListVectorMetadataEntry):
            sequence_length = entry.length
            observed_elements = entry.observed_elements
        else:
            sequence_length = None
            observed_elements = None

        coverage.append(
            ColumnCoverage(
                entry.id,
                entry.kind,
                entry.present_count,
                entry.null_count,
                sequence_length,
                observed_elements,
            )
        )
    return tuple(coverage)


def _retained_entries(
    entries: Sequence[VectorMetadataEntry],
    retained_ids: Sequence[str],
) -> tuple[VectorMetadataEntry, ...]:
    retained = frozenset(retained_ids)
    return tuple(entry for entry in entries if entry.id in retained)


def _reject_undeclared_targets(stream: Iterator[Sample]) -> Iterator[Sample]:
    for sample in stream:
        if sample.targets is not None:
            raise RuntimeError(
                "Metadata has no target entries, but the pipeline produced targets. "
                "Rebuild build/metadata.json."
            )
        yield sample
