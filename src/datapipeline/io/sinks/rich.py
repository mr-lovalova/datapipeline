from typing import Protocol

from .stdout import StdoutTextSink


class RichFormatter(Protocol):
    def render(self, console, text: str) -> None: ...


class ReprRichFormatter:
    def __init__(self):
        from rich.highlighter import ReprHighlighter

        self._highlighter = ReprHighlighter()

    def render(self, console, text: str) -> None:
        console.print(self._highlighter(text))


class JsonRichFormatter:
    def render(self, console, text: str) -> None:
        import json as _json

        stripped = text.strip()
        if not stripped:
            return
        try:
            data = _json.loads(stripped)
            console.print_json(data=data)
        except Exception:
            console.print(stripped)


class PlainRichFormatter:
    def render(self, console, text: str) -> None:
        console.print(text)


class RichStdoutSink(StdoutTextSink):
    def __init__(self, formatter: RichFormatter):
        super().__init__()
        try:
            from rich.console import Console
        except Exception:  # pragma: no cover
            self.console = None
        else:
            self.console = Console(
                file=self.stream, markup=False, highlight=False, soft_wrap=True
            )
        self._formatter = formatter

    def write_text(self, s: str) -> None:
        if not self.console:
            super().write_text(s)
            return
        text = s.rstrip("\n")
        self._formatter.render(self.console, text)
