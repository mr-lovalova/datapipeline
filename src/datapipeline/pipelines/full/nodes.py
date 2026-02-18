from itertools import chain
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
        feature_entries = context.load_schema(payload="features")
        target_entries = context.load_schema(payload="targets")

        if not feature_entries:
            if context.schema_required:
                raise RuntimeError(
                    "Schema missing for payload 'features'. Run `jerry build` to materialize schema.json."
                )
            feature_stream = stream
        else:
            feature_schema = VectorEnsureSchemaTransform(
                on_missing="fill", on_extra="drop"
            )
            feature_schema.bind_context(context)
            feature_stream = feature_schema(stream)

        def _apply_targets(upstream: Iterator[Sample]) -> Iterator[Sample]:
            if target_entries:
                target_schema = VectorEnsureSchemaTransform(
                    payload="targets", on_missing="fill", on_extra="drop"
                )
                target_schema.bind_context(context)
                return target_schema(upstream)
            if not context.schema_required:
                return upstream
            iterator = iter(upstream)
            try:
                first = next(iterator)
            except StopIteration:
                return iter(())
            if first.targets is None:
                return chain([first], iterator)
            raise RuntimeError(
                "Schema missing for payload 'targets'. Run `jerry build` to materialize schema.json."
            )

        return _apply_targets(feature_stream)
