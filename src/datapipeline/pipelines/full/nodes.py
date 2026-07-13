from collections.abc import Iterator

from datapipeline.domain.sample import Sample
from datapipeline.dag.context import PipelineContext
from datapipeline.transforms.engine import apply_transforms
from datapipeline.plugins import VECTOR_TRANSFORMS_EP
from datapipeline.services.constants import POSTPROCESS_TRANSFORMS
from datapipeline.transforms.vector import VectorEnsureSchemaTransform


def post_process(
    context: PipelineContext,
    stream: Iterator[Sample],
) -> Iterator[Sample]:
    stream = _apply_vector_schema(context, stream)
    runtime = context.runtime
    transforms = runtime.registries.postprocesses.get(POSTPROCESS_TRANSFORMS)
    if not transforms:
        return stream
    return apply_transforms(stream, VECTOR_TRANSFORMS_EP, transforms, context)


def _apply_vector_schema(
    context: PipelineContext,
    stream: Iterator[Sample],
) -> Iterator[Sample]:
    with context.activate():
        schema = context.load_schema()

        if not schema.features:
            raise RuntimeError(
                "Schema has no feature entries. Rebuild build/schema.json."
            )
        feature_schema = VectorEnsureSchemaTransform(
            on_missing="fill", on_extra="drop"
        )
        feature_schema.bind_context(context)
        feature_stream = feature_schema(stream)

        if not schema.targets:
            return _reject_undeclared_targets(feature_stream)
        target_schema = VectorEnsureSchemaTransform(
            payload="targets", on_missing="fill", on_extra="drop"
        )
        target_schema.bind_context(context)
        return target_schema(feature_stream)


def _reject_undeclared_targets(stream: Iterator[Sample]) -> Iterator[Sample]:
    for sample in stream:
        if sample.targets is not None:
            raise RuntimeError(
                "Schema has no target entries, but the pipeline produced targets. "
                "Rebuild build/schema.json."
            )
        yield sample
