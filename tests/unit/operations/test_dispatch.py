import pytest

from datapipeline.config.tasks import OperationTask
from datapipeline.operations import dispatch


def _operation() -> OperationTask:
    return OperationTask(id="report", entrypoint="plugin.runtime.report")


def test_load_operation_runner_returns_entrypoint_callable(monkeypatch) -> None:
    def runner():
        return "result"

    monkeypatch.setattr(dispatch, "load_ep", lambda _group, _entrypoint: runner)

    assert dispatch.load_operation_runner(_operation(), "runtime") is runner


def test_load_operation_runner_names_unknown_operation(monkeypatch) -> None:
    def missing(_group, _entrypoint):
        raise ValueError("missing")

    monkeypatch.setattr(dispatch, "load_ep", missing)

    with pytest.raises(
        ValueError,
        match="Unknown entrypoint 'plugin.runtime.report' for operation 'report'",
    ):
        dispatch.load_operation_runner(_operation(), "runtime")


def test_load_operation_runner_rejects_noncallable_entrypoint(monkeypatch) -> None:
    monkeypatch.setattr(dispatch, "load_ep", lambda _group, _entrypoint: object())

    with pytest.raises(TypeError, match="must resolve to a callable"):
        dispatch.load_operation_runner(_operation(), "runtime")
