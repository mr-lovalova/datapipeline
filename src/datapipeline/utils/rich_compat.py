from __future__ import annotations


def suppress_file_proxy_shutdown_errors() -> None:
    """Patch rich.file_proxy.FileProxy.flush to ignore shutdown ImportErrors.

    Rich leaves behind FileProxy instances that may flush while the interpreter
    is tearing down, which triggers `ImportError: sys.meta_path is None`.
    Swallow those benign errors so CLI commands exit cleanly.
    """
    try:
        from rich.file_proxy import FileProxy
    except Exception:
        return

    if getattr(FileProxy, "_datapipeline_safe_flush", False):
        return

    original_flush = FileProxy.flush

    def _safe_flush(self) -> None:  # type: ignore[override]
        try:
            original_flush(self)
        except ImportError as exc:
            if "sys.meta_path is None" in str(exc):
                return
            raise
        except RuntimeError as exc:
            message = str(exc)
            if "shutting down" in message.lower():
                return
            raise

    FileProxy.flush = _safe_flush  # type: ignore[assignment]
    setattr(FileProxy, "_datapipeline_safe_flush", True)


__all__ = ["suppress_file_proxy_shutdown_errors"]
