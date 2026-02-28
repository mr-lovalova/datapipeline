from dataclasses import dataclass
from pathlib import Path

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.dag.context import PipelineContext
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap


@dataclass(frozen=True)
class DatasetContext:
    project: Path
    dataset: FeatureDatasetConfig
    runtime: Runtime
    pipeline_context: PipelineContext

    @property
    def features(self):
        return list(self.dataset.features or [])

    @property
    def targets(self):
        return list(self.dataset.targets or [])


def load_dataset_context(project: Path | str) -> DatasetContext:
    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    runtime = bootstrap(project_path)
    context = PipelineContext(runtime)
    return DatasetContext(
        project=project_path,
        dataset=dataset,
        runtime=runtime,
        pipeline_context=context,
    )
