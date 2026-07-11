from contextlib import contextmanager
import sys

from rich.console import Console
from rich.rule import Rule


def _is_tty() -> bool:
    return sys.stderr.isatty()


class VisualsBackend:
    def on_job_start(
        self, sections: tuple[str, ...], label: str, idx: int, total: int
    ) -> bool:
        return False

    @contextmanager
    def wrap_execution(self, log_level: int):
        yield


class _RichBackend(VisualsBackend):
    def _render_sections(self, console, sections: tuple[str, ...]) -> None:
        if not sections:
            return
        console.print(Rule(sections[0].title(), style="bold white"))
        if len(sections) > 1:
            for level, name in enumerate(sections[1:], start=1):
                indent = "  " * level
                console.print(f"{indent}[cyan]{name}[/cyan]")
            console.print()

    def on_job_start(
        self, sections: tuple[str, ...], label: str, idx: int, total: int
    ) -> bool:
        console = Console(file=sys.stderr, markup=True)
        self._render_sections(console, sections)
        indent = "  " * max(len(sections), 1)
        if total == 1:
            console.print(f"{indent}── {label} ──")
        else:
            console.print(f"{indent}── {label} ({idx}/{total}) ──")
        console.print()
        return True

    def wrap_execution(self, log_level: int):
        from .rich.progress import visual_execution

        return visual_execution(log_level)


def _rich_live_supported() -> bool:
    console = Console(file=sys.stderr, markup=False, highlight=False)
    return bool(
        console.is_terminal
        and console.is_interactive
        and not console.is_dumb_terminal
        and console.color_system is not None
    )


def get_visuals_backend(provider: str | None) -> VisualsBackend:
    mode = (provider or "on").lower()
    if mode == "off":
        return VisualsBackend()
    if _is_tty() and _rich_live_supported():
        return _RichBackend()
    return VisualsBackend()
