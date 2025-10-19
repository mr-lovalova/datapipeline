import io
import json
from contextlib import redirect_stdout
from pathlib import Path

from datapipeline.analysis.vector_analyzer import VectorStatsCollector
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.services.bootstrap import bootstrap
from datapipeline.utils.paths import ensure_parent
from datapipeline.services.bootstrap import artifacts_root
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.pipeline.stages import post_process


def report(
    project: str,
    *,
    output: str | None = None,
    threshold: float = 0.95,
    match_partition: str = "base",
    matrix: str = "none",  # one of: none|csv|html
    matrix_output: str | None = None,
    rows: int = 20,
    cols: int = 10,
    fmt: str | None = None,
    quiet: bool = False,
    write_coverage: bool = True,
    apply_postprocess: bool = True,
    include_targets: bool = False,
) -> None:
    """Compute a quality report and optionally export coverage JSON and/or a matrix.

    - Always prints a human-readable report (unless quiet=True).
    - When output is set, writes trimmed coverage summary JSON.
    - When matrix != 'none', writes an availability matrix in the requested format.
    """

    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    runtime = bootstrap(project_path)

    feature_cfgs = list(dataset.features or [])
    if include_targets:
        feature_cfgs += list(dataset.targets or [])
    expected_feature_ids = [cfg.id for cfg in feature_cfgs]

    # Resolve matrix format and path
    matrix_fmt = (fmt or matrix) if matrix in {"csv", "html"} else None
    if matrix_fmt:
        filename = "matrix.html" if matrix_fmt == "html" else "matrix.csv"
    else:
        filename = None
    base_artifacts = artifacts_root(project_path)
    matrix_path = None
    if matrix_fmt:
        matrix_path = Path(matrix_output) if matrix_output else (base_artifacts / filename)

    collector = VectorStatsCollector(
        expected_feature_ids or None,
        match_partition=match_partition,
        threshold=threshold,
        show_matrix=False,
        matrix_rows=rows,
        matrix_cols=cols,
        matrix_output=(str(matrix_path) if matrix_path else None),
        matrix_format=(matrix_fmt or "csv"),
    )

    # When applying transforms, let the global postprocess registry provide them (pass None).
    # When raw, pass an empty list to bypass registry/defaults.
    vectors = build_vector_pipeline(runtime, feature_cfgs, dataset.group_by, stage=None)
    if apply_postprocess:
        vectors = post_process(runtime, vectors)  # use global postprocess

    for group_key, vector in vectors:
        collector.update(group_key, vector.values)

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        summary = collector.print_report()
    if not quiet:
        report_text = buffer.getvalue()
        if report_text.strip():
            print(report_text, end="")

    # Optionally write coverage summary JSON to a path
    if write_coverage:
        output_path = Path(output) if output else (base_artifacts / "coverage.json")
        ensure_parent(output_path)

        feature_stats = summary.get("feature_stats", [])
        partition_stats = summary.get("partition_stats", [])

        trimmed = {
            "total_vectors": summary.get("total_vectors", collector.total_vectors),
            "empty_vectors": summary.get("empty_vectors", collector.empty_vectors),
            "threshold": threshold,
            "match_partition": match_partition,
            "features": {
                "keep": summary.get("keep_features", []),
                "below": summary.get("below_features", []),
                "coverage": {stat["id"]: stat["coverage"] for stat in feature_stats},
            },
            "partitions": {
                "keep": summary.get("keep_partitions", []),
                "below": summary.get("below_partitions", []),
                "keep_suffixes": summary.get("keep_suffixes", []),
                "below_suffixes": summary.get("below_suffixes", []),
                "coverage": {stat["id"]: stat["coverage"] for stat in partition_stats},
            },
        }

        with output_path.open("w", encoding="utf-8") as fh:
            json.dump(trimmed, fh, indent=2)
        print(f"📝 Saved coverage summary to {output_path}")


def partitions(
    project: str,
    *,
    output: str | None = None,
    include_targets: bool = False,
) -> None:
    """Discover observed partitions and write a manifest JSON.

    Produces a JSON with keys:
      - features: list of base feature ids
      - partitions: list of full partition ids (e.g., feature__suffix)
      - by_feature: mapping base id -> list of suffixes (empty when none)
    """

    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    runtime = bootstrap(project_path)

    feature_cfgs = list(dataset.features or [])
    if include_targets:
        feature_cfgs += list(dataset.targets or [])
    expected_feature_ids = [cfg.id for cfg in feature_cfgs]
    collector = VectorStatsCollector(
        expected_feature_ids or None,
        match_partition="full",
        threshold=None,
        show_matrix=False,
    )

    vectors = build_vector_pipeline(runtime, feature_cfgs, dataset.group_by, stage=None)
    vectors = post_process(runtime, vectors)  # apply global postprocess
    for group_key, vector in vectors:
        collector.update(group_key, vector.values)

    base_artifacts = artifacts_root(project_path)
    output_path = Path(output) if output else (base_artifacts / "partitions.json")
    ensure_parent(output_path)

    parts = sorted(collector.discovered_partitions)
    features = sorted({pid.split("__", 1)[0] for pid in parts})
    by_feature: dict[str, list[str]] = {}
    for pid in parts:
        if "__" in pid:
            base, suffix = pid.split("__", 1)
        else:
            base, suffix = pid, ""
        by_feature.setdefault(base, [])
        if suffix and suffix not in by_feature[base]:
            by_feature[base].append(suffix)
    for k in list(by_feature.keys()):
        by_feature[k] = sorted(by_feature[k])

    data = {
        "features": features,
        "partitions": parts,
        "by_feature": by_feature,
    }

    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    print(f"📝 Saved partitions manifest to {output_path}")


def expected(
    project: str,
    *,
    output: str | None = None,
    include_targets: bool = False,
) -> None:
    """Discover complete set of observed full feature IDs and write a list.

    Writes newline-separated ids to `<paths.artifacts>/expected.txt` by default.
    """

    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    runtime = bootstrap(project_path)

    feature_cfgs = list(dataset.features or [])
    if include_targets:
        feature_cfgs += list(dataset.targets or [])

    vectors = build_vector_pipeline(runtime, feature_cfgs, dataset.group_by, stage=None)
    ids: set[str] = set()
    for _, vector in vectors:
        ids.update(vector.values.keys())

    try:
        default_path = artifacts_root(project_path) / "expected.txt"
    except Exception as e:
        raise RuntimeError(
            f"{e}. Set `paths.artifacts` in your project.yaml to a writable directory."
        )
    output_path = Path(output) if output else default_path
    ensure_parent(output_path)
    with output_path.open("w", encoding="utf-8") as fh:
        for fid in sorted(ids):
            fh.write(f"{fid}\n")
    print(f"📝 Saved expected feature list to {output_path} ({len(ids)} ids)")
