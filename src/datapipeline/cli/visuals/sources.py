from contextlib import contextmanager
import logging
import sys
from typing import Optional

from datapipeline.runtime import Runtime

logger = logging.getLogger(__name__)


def _is_tty() -> bool:
    try:
        return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
    except Exception:
        return False


class VisualsBackend:
    """Interface for visuals backends.

    - on_build_start/on_run_start return True if the backend handled the headline, False to let caller log it.
    - wrap_sources returns a contextmanager that enables streaming visuals.
    """

    def on_build_start(self, path) -> bool:  # Path-like
        return False

    def on_run_start(self, label: str, idx: int, total: int) -> bool:
        return False

    def on_streams_complete(self) -> bool:
        """Return True if backend surfaced a final completion line visually."""
        return False

    def requires_logging_redirect(self) -> bool:
        """Return True when console logging should be routed via tqdm."""
        return True

    def wrap_sources(self, runtime: Runtime, log_level: int):  # contextmanager
        @contextmanager
        def _noop():
            yield

        return _noop()


class _BasicBackend(VisualsBackend):
    def wrap_sources(self, runtime: Runtime, log_level: int):
        from .sources_basic import visual_sources as basic
        return basic(runtime, log_level)


class _RichBackend(VisualsBackend):
    def on_build_start(self, path) -> bool:
        try:
            from rich.console import Console as _Console
            from rich.rule import Rule as _Rule
            import sys as _sys
            from pathlib import Path as _Path
            import os as _os
            console = _Console(file=_sys.stderr, markup=True)
            console.print(_Rule("Build", style="bold white"))
            # Subheader with compact path
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

    def on_run_start(self, label: str, idx: int, total: int) -> bool:
        try:
            from rich.console import Console as _Console
            from rich.rule import Rule as _Rule
            import sys as _sys
            console = _Console(file=_sys.stderr, markup=True)
            console.print(_Rule("Run", style="bold white"))
            console.print(f"[cyan]run:[/cyan] '{label}' ({idx}/{total})")
            console.print()
            return True
        except Exception:
            return False

    def wrap_sources(self, runtime: Runtime, log_level: int):
        from .sources_rich import visual_sources as rich_vs
        return rich_vs(runtime, log_level)

    def on_streams_complete(self) -> bool:
        # Rich backend manages its own persistent final line; signal handled
        return True

    def requires_logging_redirect(self) -> bool:
        return False


class _OffBackend(VisualsBackend):
    def wrap_sources(self, runtime: Runtime, log_level: int):
        from .sources_off import visual_sources as off_vs
        return off_vs(runtime, log_level)


def _rich_available() -> bool:
    try:
        import rich  # noqa: F401
        return True
    except Exception:
        return False


def get_visuals_backend(visuals: Optional[str]) -> VisualsBackend:
    mode = (visuals or "auto").lower()
    if mode == "off":
        return _OffBackend()
    if mode == "basic":
        return _BasicBackend()
    if mode == "rich":
        return _RichBackend() if _rich_available() else _BasicBackend()
    # auto
    if _rich_available() and _is_tty():
        return _RichBackend()
    return _BasicBackend()


@contextmanager
def visual_sources(runtime: Runtime, log_level: int, visuals: Optional[str] = None):
    backend = get_visuals_backend(visuals)
    with backend.wrap_sources(runtime, log_level):
        yield
