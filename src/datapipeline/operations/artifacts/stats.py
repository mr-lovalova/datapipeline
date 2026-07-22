from pathlib import Path

from datapipeline.analysis.vector.stats import VectorStatsAccumulator
from datapipeline.artifacts.models import VectorStatsArtifact
from datapipeline.artifacts.registry import VECTOR_METADATA_SPEC
from datapipeline.config.tasks import StatsTask
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.dataset.postprocess import build_postprocess_plan
from datapipeline.pipelines.sample.input import open_samples
from datapipeline.runtime import Runtime
from datapipeline.utils.json_artifact import write_json_artifact


def materialize_vector_stats(
    runtime: Runtime,
    task_cfg: StatsTask,
) -> ArtifactOutput:
    dataset = runtime.dataset
    context = PipelineContext(runtime)
    metadata = context.require_artifact(VECTOR_METADATA_SPEC)
    context.window_bounds(rectangular_required=True)

    samples = open_samples(
        context,
        dataset.features,
        dataset.sample.cadence,
        target_configs=dataset.targets,
        rectangular=True,
        sample_keys=dataset.sample.keys,
    )
    feature_entries = metadata.features
    target_entries = metadata.targets
    if task_cfg.stage == "postprocessed":
        plan = build_postprocess_plan(context)
        feature_entries = plan.feature_entries
        target_entries = plan.target_entries
        samples = plan.apply(samples)

    feature_stats = VectorStatsAccumulator(feature_entries)
    target_stats = VectorStatsAccumulator(target_entries)
    total_samples = 0
    empty_samples = 0
    try:
        for sample in samples:
            total_samples += 1
            target_values = sample.targets.values if sample.targets is not None else {}
            if not sample.features.values and not target_values:
                empty_samples += 1
            feature_stats.update(sample.features.values)
            target_stats.update(target_values)
    finally:
        close = getattr(samples, "close", None)
        if callable(close):
            close()

    artifact = VectorStatsArtifact(
        stage=task_cfg.stage,
        total_samples=total_samples,
        empty_samples=empty_samples,
        features=feature_stats.finish(),
        targets=target_stats.finish(),
    )
    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    write_json_artifact(destination, artifact.model_dump(mode="json"))

    return ArtifactOutput(
        relative_path=str(relative_path),
        meta={
            "stage": task_cfg.stage,
            "samples": total_samples,
            "features": len(artifact.features.columns),
            "targets": len(artifact.targets.columns),
        },
    )
