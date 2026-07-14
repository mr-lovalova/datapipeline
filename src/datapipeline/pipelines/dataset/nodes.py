from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from datapipeline.artifacts.models import (
    ListVectorMetadataEntry,
    VectorMetadata,
    VectorMetadataEntry,
    VectorSchemaEntry,
)
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import PipelineNode
from datapipeline.domain.sample import Sample
from datapipeline.services.artifacts import VECTOR_METADATA_SPEC
from datapipeline.transforms.vector.drop.horizontal import (
    DropSamplesTransform,
    DropTargetSamplesTransform,
)
from datapipeline.transforms.vector.drop.vertical import (
    ColumnCoverage,
    SelectFeaturesTransform,
    SelectTargetsTransform,
)
from datapipeline.transforms.vector.ensure_schema import (
    NormalizeFeaturesTransform,
    NormalizeTargetsTransform,
)


@dataclass(frozen=True)
class PostprocessPlan:
    feature_ids: tuple[str, ...]
    target_ids: tuple[str, ...]
    nodes: tuple[PipelineNode, ...]

    def apply(self, samples: Iterator[Sample]) -> Iterator[Sample]:
        stream = samples
        for node in self.nodes:
            stream = iter(node.apply(stream))
        return stream

    def select_metadata(
        self,
        metadata: VectorMetadata,
    ) -> tuple[tuple[VectorMetadataEntry, ...], tuple[VectorMetadataEntry, ...]]:
        feature_by_id = {entry.id: entry for entry in metadata.features}
        missing_features = [
            identifier
            for identifier in self.feature_ids
            if identifier not in feature_by_id
        ]
        if missing_features:
            raise ValueError(
                f"Postprocess feature IDs are missing from metadata: {missing_features!r}."
            )

        target_by_id = {entry.id: entry for entry in metadata.targets}
        missing_targets = [
            identifier
            for identifier in self.target_ids
            if identifier not in target_by_id
        ]
        if missing_targets:
            raise ValueError(
                f"Postprocess target IDs are missing from metadata: {missing_targets!r}."
            )

        return (
            tuple(feature_by_id[identifier] for identifier in self.feature_ids),
            tuple(target_by_id[identifier] for identifier in self.target_ids),
        )


def build_postprocess_plan(context: PipelineContext) -> PostprocessPlan:
    schema = context.load_schema()
    if not schema.features:
        raise RuntimeError("Schema has no feature entries. Rebuild build/schema.json.")

    config = context.runtime.dataset.postprocess
    feature_entries = schema.features
    target_entries = schema.targets
    nodes: list[PipelineNode] = []

    if config.columns.features is not None or config.columns.targets is not None:
        metadata = context.require_artifact(VECTOR_METADATA_SPEC)

        if config.columns.features is not None:
            policy = config.columns.features
            feature_selection = SelectFeaturesTransform(
                [entry.id for entry in feature_entries],
                _column_coverage(
                    feature_entries,
                    metadata.features,
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
                raise ValueError("Feature selection removed every schema entry.")
            nodes.append(
                PipelineNode(
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
                    metadata.targets,
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
                raise ValueError("Target selection removed every schema entry.")
            nodes.append(
                PipelineNode(
                    name="select_targets",
                    apply=target_selection.apply,
                )
            )

    nodes.append(
        PipelineNode(
            name="normalize_features",
            apply=NormalizeFeaturesTransform(feature_entries).apply,
        )
    )

    if target_entries:
        target_normalizer = NormalizeTargetsTransform(target_entries).apply
        target_node_name = "normalize_targets"
    else:
        target_normalizer = _reject_undeclared_targets
        target_node_name = "reject_undeclared_targets"
    nodes.append(
        PipelineNode(
            name=target_node_name,
            apply=target_normalizer,
        )
    )

    if config.samples.features is not None:
        feature_filter = DropSamplesTransform(
            [entry.id for entry in feature_entries],
            config.samples.features.threshold,
            config.samples.features.ids,
        )
        nodes.append(
            PipelineNode(
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
        nodes.append(
            PipelineNode(
                name="filter_samples_by_targets",
                apply=target_filter.apply,
            )
        )

    return PostprocessPlan(
        feature_ids=tuple(entry.id for entry in feature_entries),
        target_ids=tuple(entry.id for entry in target_entries),
        nodes=tuple(nodes),
    )


def apply_postprocess(
    context: PipelineContext,
    samples: Iterator[Sample],
) -> Iterator[Sample]:
    """Apply the same ordered postprocess stages used by the dataset pipeline."""

    return build_postprocess_plan(context).apply(samples)


def _column_coverage(
    schema_entries: Sequence[VectorSchemaEntry],
    metadata_entries: Sequence[VectorMetadataEntry],
    selected_ids: Sequence[str] | None,
) -> tuple[ColumnCoverage, ...]:
    schema_by_id = {entry.id: entry for entry in schema_entries}
    selected = None if selected_ids is None else frozenset(selected_ids)
    coverage: list[ColumnCoverage] = []

    for metadata in metadata_entries:
        if selected is not None and metadata.id not in selected:
            continue
        schema = schema_by_id.get(metadata.id)
        if schema is None:
            raise ValueError(f"Vector metadata contains unexpected id {metadata.id!r}.")
        if metadata.kind != schema.kind:
            raise ValueError(
                f"Vector metadata kind for {metadata.id!r} does not match the schema."
            )

        if isinstance(metadata, ListVectorMetadataEntry):
            assert schema.cadence is not None
            if metadata.cadence.target != schema.cadence.target:
                raise ValueError(
                    f"Vector metadata length for {metadata.id!r} "
                    "does not match the schema."
                )
            sequence_length = metadata.cadence.target
            observed_elements = metadata.observed_elements
        else:
            sequence_length = None
            observed_elements = None

        coverage.append(
            ColumnCoverage(
                metadata.id,
                metadata.kind,
                metadata.present_count,
                metadata.null_count,
                sequence_length,
                observed_elements,
            )
        )
    return tuple(coverage)


def _retained_entries(
    entries: Sequence[VectorSchemaEntry],
    retained_ids: Sequence[str],
) -> tuple[VectorSchemaEntry, ...]:
    retained = frozenset(retained_ids)
    return tuple(entry for entry in entries if entry.id in retained)


def _reject_undeclared_targets(stream: Iterator[Sample]) -> Iterator[Sample]:
    for sample in stream:
        if sample.targets is not None:
            raise RuntimeError(
                "Schema has no target entries, but the pipeline produced targets. "
                "Rebuild build/schema.json."
            )
        yield sample
