from contextlib import contextmanager
import logging
from pathlib import Path
from unittest.mock import patch

from datapipeline.cli.visuals import runner
from datapipeline.runtime import Runtime


class _StubBackend:
    def __init__(self) -> None:
        self.levels: list[int] = []

    def wrap_execution(self, level):
        self.levels.append(level)

        @contextmanager
        def context():
            yield

        return context()


def _runtime() -> Runtime:
    return Runtime(project_yaml=Path("."), artifacts_root=Path("."))


def test_run_with_backend_executes_work_inside_backend_context():
    backend = _StubBackend()
    called = {"ok": False}
    with patch(
        "datapipeline.cli.visuals.runner.get_visuals_backend", return_value=backend
    ):
        runner.run_with_backend(
            visuals="on",
            runtime=_runtime(),
            level=logging.INFO,
            work=lambda: called.__setitem__("ok", True),
        )

    assert called["ok"] is True
    assert backend.levels == [logging.INFO]
