from pathlib import Path

from datapipeline.analysis.vector.coverage_stats import CoverageStatsAccumulator
from datapipeline.artifacts.models import CoverageStatsArtifact
from datapipeline.artifacts.registry import VECTOR_METADATA_SPEC
from datapipeline.config.tasks import CoverageStatsTask
from datapipeline.execution.context import PipelineContext
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.dataset.postprocess import build_postprocess_plan
from datapipeline.pipelines.sample.input import open_samples
from datapipeline.runtime import Runtime
from datapipeline.utils.json_artifact import write_json_artifact


def build_coverage_stats_artifact(
    runtime: Runtime,
    task_cfg: CoverageStatsTask,
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

    feature_accumulator = CoverageStatsAccumulator(feature_entries)
    target_accumulator = CoverageStatsAccumulator(target_entries)
    total_samples = 0
    empty_samples = 0
    try:
        for sample in samples:
            total_samples += 1
            target_values = sample.targets.values if sample.targets is not None else {}
            if not sample.features.values and not target_values:
                empty_samples += 1
            feature_accumulator.update(sample.features.values)
            target_accumulator.update(target_values)
    finally:
        close = getattr(samples, "close", None)
        if callable(close):
            close()

    artifact = CoverageStatsArtifact(
        stage=task_cfg.stage,
        total_samples=total_samples,
        empty_samples=empty_samples,
        features=feature_accumulator.finish(),
        targets=target_accumulator.finish(),
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
