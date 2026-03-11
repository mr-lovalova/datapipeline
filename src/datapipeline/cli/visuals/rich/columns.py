from collections import deque
from math import ceil
from typing import Any, Deque, Dict, Optional, Tuple

from rich.progress import ProgressColumn, Task
from rich.table import Column
from rich.text import Text


class AverageTimeRemainingColumn(ProgressColumn):
    """ETA column that blends long-term and recent throughput for stability."""

    max_refresh = 0.5

    def __init__(
        self,
        compact: bool = False,
        elapsed_when_finished: bool = False,
        table_column: Optional[Any] = None,
        window_seconds: float = 300.0,
    ) -> None:
        self.compact = compact
        self.elapsed_when_finished = elapsed_when_finished
        self.window_seconds = max(0.0, float(window_seconds))
        self._history: Dict[int, Deque[Tuple[float, float]]] = {}
        super().__init__(table_column=table_column)

    def _format_seconds(self, seconds: int) -> str:
        minutes, secs = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if self.compact and not hours:
            return f"{minutes:02d}:{secs:02d}"
        return f"{hours:d}:{minutes:02d}:{secs:02d}"

    def _recent_seconds_per_item(self, task: Task) -> Optional[float]:
        if self.window_seconds <= 0:
            return None
        if task.start_time is None:
            return None
        history = self._history.setdefault(int(task.id), deque())
        now = task.get_time()
        completed = float(task.completed)
        if not history or history[-1][1] != completed:
            history.append((now, completed))
        cutoff = now - self.window_seconds
        while history and history[0][0] < cutoff:
            history.popleft()
        if len(history) < 2:
            return None
        start_time, start_completed = history[0]
        delta_completed = completed - start_completed
        delta_time = now - start_time
        if delta_completed <= 0 or delta_time <= 0:
            return None
        return delta_time / delta_completed

    def render(self, task: Task) -> Text:
        if self.elapsed_when_finished and task.finished:
            self._history.pop(int(task.id), None)
            elapsed = task.finished_time
            if elapsed is None:
                return Text("-:--:--", style="progress.elapsed")
            return Text(self._format_seconds(int(elapsed)), style="progress.elapsed")

        style = "progress.remaining"
        total = task.total
        if total is None:
            return Text("", style=style)
        elapsed = task.elapsed
        completed = task.completed
        remaining = task.remaining
        if not completed or elapsed is None or remaining is None:
            return Text("--:--" if self.compact else "-:--:--", style=style)
        recent = self._recent_seconds_per_item(task)
        avg_seconds_per_item = recent if recent is not None else (elapsed / completed)
        if avg_seconds_per_item <= 0:
            return Text("--:--" if self.compact else "-:--:--", style=style)
        eta_seconds = int(max(0, ceil(remaining * avg_seconds_per_item)))
        return Text(self._format_seconds(eta_seconds), style=style)


def styled_source_label(line: str) -> Text:
    text = Text()
    working = str(line or "")
    indent_len = len(working) - len(working.lstrip(" "))
    if indent_len:
        text.append(working[:indent_len])
    working = working[indent_len:]
    if working.startswith("[") and "]" in working:
        end = working.find("]")
        stream_id = working[1:end]
        rest = working[end + 1:]
        text.append("[", style="cyan")
        text.append(stream_id, style="bold cyan")
        text.append("]", style="cyan")
        text.append(rest)
        return text
    text.append(working)
    return text


class SourceLabelColumn(ProgressColumn):
    def get_table_column(self):
        # Keep one physical terminal line per task row so Live transient cleanup
        # can reliably clear the rendered table at context exit.
        return Column(no_wrap=True, overflow="ellipsis")

    def render(self, task: Task) -> Text:
        raw = task.fields.get("text", "")
        return styled_source_label(str(raw))


__all__ = [
    "AverageTimeRemainingColumn",
    "SourceLabelColumn",
    "styled_source_label",
]
