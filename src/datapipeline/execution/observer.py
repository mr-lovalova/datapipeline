from typing import Protocol

from datapipeline.execution.events import (
    NodeExecutionEvent,
    NodeProgressEvent,
    PipelineRunEvent,
)


class PipelineObserver(Protocol):
    def on_pipeline_start(
        self,
        pipeline_name: str,
        node_count: int,
        summary: str | None = None,
    ) -> None: ...

    def on_node_start(
        self,
        pipeline_name: str,
        node_name: str,
        node_index: int,
    ) -> None: ...

    def on_node_end(self, event: NodeExecutionEvent) -> None: ...

    def on_node_progress(self, event: NodeProgressEvent) -> None: ...

    def on_pipeline_end(self, event: PipelineRunEvent) -> None: ...


class NoopPipelineObserver:
    def on_pipeline_start(
        self,
        pipeline_name: str,
        node_count: int,
        summary: str | None = None,
    ) -> None:
        pass

    def on_node_start(
        self,
        pipeline_name: str,
        node_name: str,
        node_index: int,
    ) -> None:
        pass

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        pass

    def on_node_progress(self, event: NodeProgressEvent) -> None:
        pass

    def on_pipeline_end(self, event: PipelineRunEvent) -> None:
        pass
