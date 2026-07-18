import json
import shutil
from pathlib import Path

from datapipeline.artifacts.scaler import FoldedScalerArtifact, load_scaler_artifact
from tests.helpers.regression import read_jsonl, serve_dataset


def _serve(project_root: Path) -> tuple[Path, FoldedScalerArtifact]:
    request = serve_dataset(project_root)

    run_paths = request.serve_run_plans[0].paths
    run_metadata = json.loads(run_paths.metadata_path.read_text(encoding="utf-8"))
    assert run_metadata["status"] == "success"
    assert run_metadata["finished_at"] is not None
    assert (run_paths.serve_root / "latest").resolve() == run_paths.run_root.resolve()

    scaler = load_scaler_artifact(project_root / "artifacts" / "build" / "scaler.json")
    assert isinstance(scaler, FoldedScalerArtifact)
    return run_paths.dataset_dir, scaler


def _read_output(directory: Path, output_id: str) -> list[dict[str, object]]:
    return read_jsonl(directory / f"dataset.{output_id}.jsonl")


def _sample(day: int, signal: float, outcome: float) -> dict[str, object]:
    return {
        "key": [f"2024-01-{day:02d} 00:00:00+00:00"],
        "features": {"values": {"signal": signal}},
        "targets": {"values": {"outcome": outcome}},
    }


def test_walk_forward_scaling_and_routed_outputs(copy_fixture, tmp_path: Path) -> None:
    project_root = copy_fixture("walk_forward_project")
    changed_root = tmp_path / "walk_forward_changed_validation"
    shutil.copytree(project_root, changed_root)

    output_dir, scaler = _serve(project_root)

    assert {path.name for path in output_dir.iterdir()} == {
        "dataset.fold_0.train.jsonl",
        "dataset.fold_0.validation.jsonl",
        "dataset.fold_0.test.jsonl",
        "dataset.fold_1.train.jsonl",
        "dataset.fold_1.validation.jsonl",
        "dataset.fold_1.test.jsonl",
    }
    assert scaler.model_dump(mode="json") == {
        "kind": "folded_scaler",
        "version": 3,
        "folds": {
            "fold_0": {
                "kind": "standard_scaler",
                "version": 3,
                "with_mean": True,
                "with_std": True,
                "epsilon": 1e-12,
                "observations": 4,
                "statistics": {
                    "signal": {"mean": 1.0, "std": 1.0, "count": 2},
                    "outcome": {"mean": 12.0, "std": 2.0, "count": 2},
                },
            },
            "fold_1": {
                "kind": "standard_scaler",
                "version": 3,
                "with_mean": True,
                "with_std": True,
                "epsilon": 1e-12,
                "observations": 4,
                "statistics": {
                    "signal": {"mean": 12.0, "std": 2.0, "count": 2},
                    "outcome": {"mean": 104.0, "std": 4.0, "count": 2},
                },
            },
        },
    }

    assert _read_output(output_dir, "fold_0.train") == [
        _sample(1, -1.0, -1.0),
        _sample(2, 1.0, 1.0),
    ]
    assert _read_output(output_dir, "fold_0.validation") == [_sample(4, 3.0, 3.0)]
    assert _read_output(output_dir, "fold_0.test") == [_sample(5, 4.0, 4.0)]
    assert _read_output(output_dir, "fold_1.train") == [
        _sample(6, -1.0, -1.0),
        _sample(7, 1.0, 1.0),
    ]
    assert _read_output(output_dir, "fold_1.validation") == [_sample(9, 2.0, 2.0)]
    assert _read_output(output_dir, "fold_1.test") == [_sample(10, 3.0, 3.0)]

    signal_path = changed_root / "data" / "signal.csv"
    signal_text = signal_path.read_text(encoding="utf-8")
    signal_text = signal_text.replace(
        "2024-01-04T00:00:00Z,4", "2024-01-04T00:00:00Z,4000"
    ).replace("2024-01-05T00:00:00Z,5", "2024-01-05T00:00:00Z,5000")
    signal_path.write_text(signal_text, encoding="utf-8")

    outcome_path = changed_root / "data" / "outcome.csv"
    outcome_text = outcome_path.read_text(encoding="utf-8")
    outcome_text = outcome_text.replace(
        "2024-01-04T00:00:00Z,18", "2024-01-04T00:00:00Z,18000"
    ).replace("2024-01-05T00:00:00Z,20", "2024-01-05T00:00:00Z,20000")
    outcome_path.write_text(outcome_text, encoding="utf-8")

    changed_output_dir, changed_scaler = _serve(changed_root)

    assert changed_scaler.folds["fold_0"] == scaler.folds["fold_0"]
    assert _read_output(changed_output_dir, "fold_0.train") == _read_output(
        output_dir,
        "fold_0.train",
    )
    assert _read_output(changed_output_dir, "fold_0.validation") != _read_output(
        output_dir,
        "fold_0.validation",
    )
