from pathlib import Path
from types import SimpleNamespace

from datapipeline.config.profiles import InspectProfile
from datapipeline.services.run_entries import RunEntry, iter_runtime_runs


def test_iter_runtime_runs_clears_split_keep_for_inspect(monkeypatch):
    profile = InspectProfile(cmd="inspect", name="coverage", target="coverage")
    entry = RunEntry(
        name="coverage",
        config=profile,
        target_id="coverage",
    )
    runtime = SimpleNamespace(split=SimpleNamespace(keep="train"), split_keep=None, run=None)

    monkeypatch.setattr(
        "datapipeline.services.run_entries.bootstrap_build_runtime",
        lambda _project_path: runtime,
    )

    runs = list(iter_runtime_runs(Path("/tmp/project.yaml"), [entry], keep_override=None))
    assert len(runs) == 1
    _, _, _, resolved_runtime = runs[0]
    assert resolved_runtime.run == profile
    assert resolved_runtime.split_keep is None
