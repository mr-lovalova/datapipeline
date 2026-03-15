from contextlib import contextmanager
from pathlib import Path
import logging
from unittest.mock import patch

from datapipeline.cli.visuals import runner
from datapipeline.runtime import Runtime


class _StubBackend:
    def wrap_sources(self, runtime, level):
        @contextmanager
        def _cm():
            yield
        return _cm()

    def on_job_start(self, *args, **kwargs):
        return False

    def on_streams_complete(self) -> bool:
        return True


def _runtime() -> Runtime:
    return Runtime(project_yaml=Path("."), artifacts_root=Path("."))


def test_run_job_executes_work():
    backend = _StubBackend()
    called = {"ok": False}
    with patch("datapipeline.cli.visuals.runner.get_visuals_backend", return_value=backend):
        runner.run_job(
            sections=("Runs",),
            label="demo",
            visuals="on",
            level=logging.INFO,
            runtime=_runtime(),
            work=lambda: called.__setitem__("ok", True),
            idx=1,
            total=1,
        )
    assert called["ok"] is True


def test_run_with_backend_executes_work():
    backend = _StubBackend()
    called = {"ok": False}
    with patch("datapipeline.cli.visuals.runner.get_visuals_backend", return_value=backend):
        runner.run_with_backend(
            visuals="on",
            runtime=_runtime(),
            level=logging.INFO,
            work=lambda: called.__setitem__("ok", True),
        )
    assert called["ok"] is True


def test_run_job_cleans_runtime_cache_on_success():
    backend = _StubBackend()
    runtime = _runtime()
    calls = {"cleanup": 0}

    def _cleanup():
        calls["cleanup"] += 1

    runtime.cleanup_cache = _cleanup  # type: ignore[method-assign]

    with patch("datapipeline.cli.visuals.runner.get_visuals_backend", return_value=backend):
        runner.run_job(
            sections=("Runs",),
            label="demo",
            visuals="on",
            level=logging.INFO,
            runtime=runtime,
            work=lambda: None,
            idx=1,
            total=1,
        )

    assert calls["cleanup"] == 1


def test_run_job_cleans_runtime_cache_on_failure():
    backend = _StubBackend()
    runtime = _runtime()
    calls = {"cleanup": 0}

    def _cleanup():
        calls["cleanup"] += 1

    runtime.cleanup_cache = _cleanup  # type: ignore[method-assign]

    with patch("datapipeline.cli.visuals.runner.get_visuals_backend", return_value=backend):
        try:
            runner.run_job(
                sections=("Runs",),
                label="demo",
                visuals="on",
                level=logging.INFO,
                runtime=runtime,
                work=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
                idx=1,
                total=1,
            )
        except RuntimeError:
            pass

    assert calls["cleanup"] == 1
