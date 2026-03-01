from types import SimpleNamespace

from datapipeline.operations import inspect as inspect_ops


def test_iter_merged_vectors_applies_postprocess_toggle(monkeypatch) -> None:
    calls = {"window_bounds": 0, "post_process": 0}

    class _Ctx:
        def __init__(self, runtime):
            self.runtime = runtime

        def window_bounds(self, rectangular_required: bool):
            assert rectangular_required is True
            calls["window_bounds"] += 1

    sample = SimpleNamespace(
        key=("t0",),
        features=SimpleNamespace(values={"f": 1.0}),
        targets=SimpleNamespace(values={"y": 2.0}),
    )
    dataset = SimpleNamespace(
        group_by="1h",
        features=[SimpleNamespace(id="f")],
        targets=[SimpleNamespace(id="y")],
    )

    monkeypatch.setattr(inspect_ops, "PipelineContext", _Ctx)
    monkeypatch.setattr(
        inspect_ops,
        "build_vector_pipeline",
        lambda *args, **kwargs: iter([sample]),
    )

    def _post_process(context, vectors):
        calls["post_process"] += 1
        return vectors

    monkeypatch.setattr(inspect_ops, "post_process", _post_process)

    rows = list(
        inspect_ops._iter_merged_vectors(
            runtime=SimpleNamespace(),
            dataset=dataset,
            apply_postprocess=True,
        )
    )

    assert calls["window_bounds"] == 1
    assert calls["post_process"] == 1
    assert rows == [(("t0",), {"f": 1.0, "y": 2.0})]
