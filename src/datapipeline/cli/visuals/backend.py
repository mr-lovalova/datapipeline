from contextlib import contextmanager
import sys

from rich.console import Console


def _is_tty() -> bool:
    return sys.stderr.isatty()


class VisualsBackend:
    @contextmanager
    def wrap_execution(self, log_level: int):
        yield


class _RichBackend(VisualsBackend):
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
