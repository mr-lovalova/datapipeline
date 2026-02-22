from contextlib import contextmanager
import sys
from typing import Optional, Tuple

from datapipeline.runtime import Runtime


def _is_tty() -> bool:
    try:
        return hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
    except Exception:
        return False


class VisualsBackend:
    """Interface for visuals backends.

    - on_build_start/on_job_start return True if the backend handled the headline, False to let caller log it.
    - wrap_sources returns a contextmanager that enables streaming visuals.
    """

    def on_build_start(self, path) -> bool:  # Path-like
        return False

    def on_job_start(self, sections: Tuple[str, ...], label: str, idx: int, total: int) -> bool:
        return False

    def on_streams_complete(self) -> bool:
        """Return True if backend surfaced a final completion line visually."""
        return False

    def wrap_sources(self, runtime: Runtime, log_level: int):  # contextmanager
        @contextmanager
        def _noop():
            yield

        return _noop()


class _BasicBackend(VisualsBackend):
    def wrap_sources(self, runtime: Runtime, log_level: int):
        from .streams_basic import visual_sources as basic
        return basic(runtime, log_level)


class _RichBackend(VisualsBackend):
    def _render_sections(self, console, sections: tuple[str, ...]) -> None:
        if not sections:
            return
        from rich.rule import Rule as _Rule
        console.print(_Rule(sections[0].title(), style="bold white"))
        if len(sections) > 1:
            for level, name in enumerate(sections[1:], start=1):
                indent = "  " * level
                console.print(f"{indent}[cyan]{name}[/cyan]")
            console.print()

    def on_job_start(self, sections: tuple[str, ...], label: str, idx: int, total: int) -> bool:
        try:
            from rich.console import Console as _Console
            import sys as _sys
            console = _Console(file=_sys.stderr, markup=True)
            self._render_sections(console, sections)
            indent = "  " * max(len(sections), 1)
            console.print(f"{indent}── {label} ({idx}/{total}) ──")
            console.print()
            return True
        except Exception:
            return False

    def on_build_start(self, path) -> bool:
        try:
            from rich.console import Console as _Console
            from rich.rule import Rule as _Rule
            import sys as _sys
            from pathlib import Path as _Path
            import os as _os
            console = _Console(file=_sys.stderr, markup=True)
            console.print(_Rule("Info", style="bold white"))
            # Subheader with compact path to project.yaml
            p = _Path(path)
            try:
                cwd = _Path(_os.getcwd())
                rel = p.relative_to(cwd)
                parts = [part for part in rel.as_posix().split("/") if part]
            except Exception:
                parts = [part for part in p.as_posix().split("/") if part]
            if len(parts) > 3:
                parts = ["..."] + parts[-3:]
            compact = "/".join(parts) if parts else p.name
            console.print(f"[cyan]project:[/cyan] {compact}")
            console.print()  # spacer
            return True
        except Exception:
            return False

    def wrap_sources(self, runtime: Runtime, log_level: int):
        from .streams_rich import visual_sources as rich_vs
        return rich_vs(runtime, log_level)

    def on_streams_complete(self) -> bool:
        # Rich backend manages its own persistent final line; signal handled
        return True


class _OffBackend(VisualsBackend):
    def wrap_sources(self, runtime: Runtime, log_level: int):
        @contextmanager
        def _noop():
            yield

        return _noop()


def _rich_available() -> bool:
    try:
        import rich  # noqa: F401
        return True
    except Exception:
        return False


def _rich_live_supported() -> bool:
    try:
        from rich.console import Console as _Console
        import sys as _sys
        console = _Console(file=_sys.stderr, markup=False, highlight=False)
        return bool(
            console.is_terminal
            and console.is_interactive
            and not console.is_dumb_terminal
            and console.color_system is not None
        )
    except Exception:
        return False


def get_visuals_backend(provider: Optional[str]) -> VisualsBackend:
    mode = (provider or "on").lower()
    if mode == "off":
        return _OffBackend()
    # on
    if _rich_available() and _is_tty() and _rich_live_supported():
        return _RichBackend()
    return _BasicBackend()


@contextmanager
def visual_sources(runtime: Runtime, log_level: int, provider: Optional[str] = None):
    backend = get_visuals_backend(provider)
    with backend.wrap_sources(runtime, log_level):
        yield
