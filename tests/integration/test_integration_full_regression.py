import json
from pathlib import Path

from tests.helpers.regression import read_jsonl, serve_dataset


def _read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _sample(
    hour: int,
    linear: float,
    sine: list[float | None],
    north: float,
    south: float,
    power: float,
) -> dict[str, object]:
    return {
        "key": [f"2024-03-01 {hour:02d}:00:00+00:00"],
        "features": {
            "values": {
                "linear_scaled": linear,
                "sine_window": sine,
                "humidity_partitioned__@location:north": north,
                "humidity_partitioned__@location:south": south,
            }
        },
        "targets": {"values": {"power_target": power}},
    }


def test_full_regression_project_through_serve(copy_fixture) -> None:
    project_root = copy_fixture("regression_project")
    request = serve_dataset(project_root)

    dataset_path = request.serve_run_plans[0].paths.dataset_dir / "dataset.jsonl"
    samples = read_jsonl(dataset_path)
    assert samples == [
        _sample(
            0,
            -1.4638501094227998,
            [0.0, 0.5],
            40.0,
            38.5,
            100.0,
        ),
        _sample(
            1,
            -0.8783100656536799,
            [1.0, 0.5],
            40.0,
            37.0,
            102.0,
        ),
        _sample(
            2,
            -0.29277002188455997,
            [0.0, None],
            41.0,
            37.75,
            101.0,
        ),
        _sample(
            3,
            0.29277002188455997,
            [-0.5, -1.0],
            42.0,
            37.75,
            105.0,
        ),
        _sample(
            4,
            0.8783100656536799,
            [-0.5, 0.0],
            41.5,
            39.0,
            105.0,
        ),
    ]

    build_root = project_root / "build" / "build"
    assert _read_json(build_root / "scaler.json") == {
        "epsilon": 1e-12,
        "kind": "standard_scaler",
        "observations": 6,
        "statistics": {
            "linear_scaled": {
                "count": 6,
                "mean": 15.0,
                "std": 3.415650255319866,
            }
        },
        "version": 3,
        "with_mean": True,
        "with_std": True,
    }
    assert _read_json(build_root / "schema.json") == {
        "features": [
            {"id": "linear_scaled", "kind": "scalar"},
            {
                "cadence": {"target": 2},
                "id": "sine_window",
                "kind": "list",
            },
            {
                "id": "humidity_partitioned__@location:north",
                "kind": "scalar",
            },
            {
                "id": "humidity_partitioned__@location:south",
                "kind": "scalar",
            },
        ],
        "schema_version": 2,
        "targets": [{"id": "power_target", "kind": "scalar"}],
    }

    manifest = _read_json(build_root / "vector_inputs" / "manifest.json")
    assert isinstance(manifest, dict)
    assert {
        "cadence": manifest["cadence"],
        "format": manifest["format"],
        "sample_keys": manifest["sample_keys"],
        "version": manifest["version"],
        "features": [
            {"id": entry["id"], "rows": entry["rows"]} for entry in manifest["features"]
        ],
        "targets": [
            {"id": entry["id"], "rows": entry["rows"]} for entry in manifest["targets"]
        ],
    } == {
        "cadence": "1h",
        "format": "jsonl.gz",
        "sample_keys": [],
        "version": 5,
        "features": [
            {"id": "linear_scaled", "rows": 6},
            {"id": "sine_window", "rows": 5},
            {"id": "humidity_partitioned", "rows": 12},
        ],
        "targets": [{"id": "power_target", "rows": 6}],
    }
