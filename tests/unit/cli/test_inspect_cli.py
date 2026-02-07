from types import SimpleNamespace

from datapipeline.cli.commands import inspect


def test_iter_merged_vectors_applies_postprocess_toggle(monkeypatch) -> None:
    calls = {"window_bounds": 0, "post_process": 0}

    class _Ctx:
        def window_bounds(self, rectangular_required: bool):
            assert rectangular_required is True
            calls["window_bounds"] += 1

    sample = SimpleNamespace(
        key=("t0",),
        features=SimpleNamespace(values={"f": 1.0}),
        targets=SimpleNamespace(values={"y": 2.0}),
    )
    dataset_ctx = SimpleNamespace(
        pipeline_context=_Ctx(),
        dataset=SimpleNamespace(group_by="1h"),
        features=[SimpleNamespace(id="f")],
        targets=[SimpleNamespace(id="y")],
    )

    monkeypatch.setattr(
        inspect,
        "build_vector_pipeline",
        lambda *args, **kwargs: iter([sample]),
    )
    monkeypatch.setattr(
        inspect,
        "_iter_with_progress",
        lambda iterable, **kwargs: iterable,
    )

    def _post_process(context, vectors):
        calls["post_process"] += 1
        return vectors

    monkeypatch.setattr(inspect, "post_process", _post_process)

    rows = list(
        inspect._iter_merged_vectors(
            dataset_ctx,
            apply_postprocess=True,
            progress_style="off",
        )
    )

    assert calls["window_bounds"] == 1
    assert calls["post_process"] == 1
    assert rows == [(("t0",), {"f": 1.0, "y": 2.0})]
