from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import logging
from unittest.mock import patch

from datapipeline.cli.visuals import runner
from datapipeline.runtime import Runtime


class _StubBackend:
    def __init__(self, *, redirect: bool):
        self._redirect = redirect

    def requires_logging_redirect(self) -> bool:
        return self._redirect

    def wrap_sources(self, runtime, level):
        @contextmanager
        def _cm():
            yield
        return _cm()

    def on_run_start(self, *args, **kwargs):
        return False

    def on_streams_complete(self) -> bool:
        return True


def _runtime() -> Runtime:
    return Runtime(project_yaml=Path("."), artifacts_root=Path("."))


@patch("datapipeline.cli.visuals.runner.logging_redirect_tqdm")
def test_run_job_auto_mode_skips_redirect_when_backend_is_rich(mock_redirect):
    mock_redirect.return_value.__enter__.return_value = None
    mock_redirect.return_value.__exit__.return_value = None
    backend = _StubBackend(redirect=False)
    with patch("datapipeline.cli.visuals.runner.get_visuals_backend", return_value=backend):
        runner.run_job(
            kind="run",
            label="demo",
            visuals="auto",
            level=logging.INFO,
            runtime=_runtime(),
            work=lambda: None,
            idx=1,
            total=1,
        )
    mock_redirect.assert_not_called()


@patch("datapipeline.cli.visuals.runner.logging_redirect_tqdm")
def test_run_with_backend_uses_redirect_for_basic_backends(mock_redirect):
    mock_redirect.return_value.__enter__.return_value = None
    mock_redirect.return_value.__exit__.return_value = None
    backend = _StubBackend(redirect=True)
    with patch("datapipeline.cli.visuals.runner.get_visuals_backend", return_value=backend):
        runner.run_with_backend(
            visuals="basic",
            runtime=_runtime(),
            level=logging.INFO,
            work=lambda: None,
        )
    mock_redirect.assert_called_once()
