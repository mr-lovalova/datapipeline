from collections.abc import Callable

from datapipeline.execution.events import PipelineEvent


PipelineObserver = Callable[[PipelineEvent], None]


def ignore_pipeline_event(event: PipelineEvent) -> None:
    pass
