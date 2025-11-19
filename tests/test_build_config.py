from pathlib import Path

from datapipeline.config.build import load_build_config


def _write_project(tmp_path: Path, build_ref: str) -> Path:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "paths:",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: artifacts",
                f"  build: {build_ref}",
            ]
        ),
        encoding="utf-8",
    )
    return project_yaml


def test_load_build_config_from_artifact_directory(tmp_path):
    project_yaml = _write_project(tmp_path, "build/artifacts")
    artifacts_dir = tmp_path / "build" / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    (artifacts_dir / "partitioned_ids.features.yaml").write_text(
        "kind: partitioned_ids\noutput: expected_ids.txt\ntarget: features\n",
        encoding="utf-8",
    )
    (artifacts_dir / "partitioned_ids.targets.yaml").write_text(
        "kind: partitioned_ids\noutput: expected_targets.txt\ntarget: targets\n",
        encoding="utf-8",
    )
    (artifacts_dir / "scaler.yaml").write_text(
        "kind: scaler\noutput: scaler_val.pkl\nsplit_label: val\nenabled: true\n",
        encoding="utf-8",
    )

    cfg = load_build_config(project_yaml)

    assert len(cfg.partitioned_ids) == 2
    assert cfg.partitioned_ids[0].output == "expected_ids.txt"
    assert cfg.partitioned_ids[0].target == "features"
    assert cfg.partitioned_ids[1].output == "expected_targets.txt"
    assert cfg.partitioned_ids[1].target == "targets"
    assert cfg.scaler.output == "scaler_val.pkl"
    assert cfg.scaler.split_label == "val"
