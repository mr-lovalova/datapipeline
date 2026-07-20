import json
from datetime import datetime

import pyarrow as arrow
import pyarrow.parquet as parquet

from tests.helpers.regression import read_jsonl, serve_dataset


def test_parquet_source_matches_jsonl_source_through_dataset_pipeline(
    copy_fixture,
) -> None:
    project_root = copy_fixture("regression_project")
    baseline_request = serve_dataset(project_root)
    baseline_path = (
        baseline_request.serve_run_plans[0].paths.dataset_dir / "dataset.jsonl"
    )
    expected = read_jsonl(baseline_path)

    jsonl_path = project_root / "data" / "linear_hourly.jsonl"
    rows = [
        json.loads(line) for line in jsonl_path.read_text(encoding="utf-8").splitlines()
    ]
    for row in rows:
        row["time"] = datetime.fromisoformat(row["time"])
    parquet_path = jsonl_path.with_suffix(".parquet")
    parquet.write_table(arrow.Table.from_pylist(rows), parquet_path)

    source_path = project_root / "sources" / "metrics.linear.yaml"
    source_path.write_text(
        source_path.read_text(encoding="utf-8")
        .replace("format: jsonl", "format: parquet")
        .replace("data/linear_hourly.jsonl", "data/linear_hourly.parquet"),
        encoding="utf-8",
    )

    parquet_request = serve_dataset(project_root, "FORCE")
    result_path = parquet_request.serve_run_plans[0].paths.dataset_dir / "dataset.jsonl"
    assert read_jsonl(result_path) == expected
