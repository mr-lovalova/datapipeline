import logging
from dataclasses import dataclass

from datapipeline.dag.events import DagRunEvent, NodeRunEvent
from datapipeline.dag.observer import ExecutionObserver
from datapipeline.cli.visuals.execution_context import set_current_dag_depth


@dataclass
class _DagFrame:
    name: str
    prefix: str
    depth: int
    ended: bool = False
    end_event: DagRunEvent | None = None


class HierarchicalExecutionObserver(ExecutionObserver):
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._dag_stack: list[_DagFrame] = []
        set_current_dag_depth(0)

    @staticmethod
    def _indent(depth: int) -> str:
        return "  " * max(depth, 0)

    @staticmethod
    def _prefix(dag_name: str) -> str:
        return dag_name.split(":", 1)[0] if ":" in dag_name else dag_name

    def _active_depth(self) -> int:
        if not self._dag_stack:
            return 0
        return self._dag_stack[-1].depth + 1

    def _find_frame_index(self, dag_name: str) -> int:
        for idx in range(len(self._dag_stack) - 1, -1, -1):
            frame = self._dag_stack[idx]
            if frame.name == dag_name and not frame.ended:
                return idx
        return -1

    def _flush_completed(self) -> None:
        while self._dag_stack and self._dag_stack[-1].ended:
            frame = self._dag_stack.pop()
            event = frame.end_event
            if event is None:
                continue
            if self._logger.isEnabledFor(logging.INFO):
                self._logger.info(
                    "%sDAG finished name=%s status=%s items=%d elapsed=%.6fs",
                    self._indent(frame.depth),
                    event.dag_name,
                    event.status,
                    event.output_items,
                    event.elapsed_seconds,
                )
        set_current_dag_depth(self._active_depth())

    def on_dag_start(self, *, dag_name: str, node_count: int) -> None:
        if not self._dag_stack:
            depth = 0
        else:
            parent = self._dag_stack[-1]
            if parent.prefix == self._prefix(dag_name):
                depth = parent.depth
            else:
                depth = parent.depth + 1
        if self._logger.isEnabledFor(logging.INFO):
            self._logger.info(
                "%sDAG started name=%s nodes=%d",
                self._indent(depth),
                dag_name,
                node_count,
            )
        self._dag_stack.append(
            _DagFrame(
                name=dag_name,
                prefix=self._prefix(dag_name),
                depth=depth,
            )
        )
        set_current_dag_depth(self._active_depth())

    def on_node_start(self, *, dag_name: str, node_name: str, stage: int) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            depth = self._active_depth()
            self._logger.debug(
                "%sNode started dag=%s node=%s stage=%d",
                self._indent(depth),
                dag_name,
                node_name,
                stage,
            )

    def on_node_end(self, event: NodeRunEvent) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            depth = self._active_depth()
            self._logger.debug(
                "%sNode finished dag=%s node=%s stage=%d status=%s items=%d elapsed=%.6fs",
                self._indent(depth),
                event.dag_name,
                event.node_name,
                event.stage,
                event.status,
                event.output_items,
                event.elapsed_seconds,
            )

    def on_dag_end(self, event: DagRunEvent) -> None:
        idx = self._find_frame_index(event.dag_name)
        if idx < 0:
            if self._logger.isEnabledFor(logging.INFO):
                self._logger.info(
                    "DAG finished name=%s status=%s items=%d elapsed=%.6fs",
                    event.dag_name,
                    event.status,
                    event.output_items,
                    event.elapsed_seconds,
                )
            return

        self._dag_stack[idx].ended = True
        self._dag_stack[idx].end_event = event
        self._flush_completed()


def make_execution_observer(logger: logging.Logger | None = None) -> ExecutionObserver:
    return HierarchicalExecutionObserver(logger or logging.getLogger(__name__))
