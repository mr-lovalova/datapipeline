from pathlib import Path

import pytest

from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.loaders.profiles import profile_defaults, profile_specs


def _artifact_tasks(project_yaml: Path):
    artifact_specs, _ = operation_specs(project_yaml)
    return list(artifact_specs)


def _all_tasks(project_yaml: Path):
    artifact_specs, operation_task_specs = operation_specs(project_yaml)
    return list(artifact_specs) + list(operation_task_specs)


def _serve_profiles(project_yaml: Path):
    return list(profile_specs(project_yaml, cmd="serve"))


def _build_profiles(project_yaml: Path):
    return list(profile_specs(project_yaml, cmd="build"))


def _inspect_profiles(project_yaml: Path):
    return list(profile_specs(project_yaml, cmd="inspect"))


def _serve_defaults(project_yaml: Path):
    return profile_defaults(project_yaml, cmd="serve")


def _write_project(tmp_path: Path, tasks_ref: str | None = None) -> Path:
    project_yaml = tmp_path / "project.yaml"
    lines = [
        "version: 1",
        "paths:",
        "  ingests: ./ingests",
        "  streams: streams",
        "  sources: sources",
        "  dataset: dataset.yaml",
        "  postprocess: postprocess.yaml",
        "  artifacts: artifacts",
    ]
    if tasks_ref:
        lines.append(f"  tasks: {tasks_ref}")
    project_yaml.write_text("\n".join(lines), encoding="utf-8")
    return project_yaml


def _operations_dir(project_yaml: Path) -> Path:
    path = project_yaml.parent / "tasks" / "operations"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _profiles_dir(project_yaml: Path) -> Path:
    path = project_yaml.parent / "profiles"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _profile_kind_dir(project_yaml: Path) -> Path:
    path = _profiles_dir(project_yaml)
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_artifact_tasks_load_configs(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "schema.yaml").write_text(
        "id: schema\nkind: artifact\noutput: schema.json\n", encoding="utf-8"
    )
    (tasks_dir / "scaler.yaml").write_text(
        "id: scaler\nkind: artifact\nsplit_label: all\noutput: stats.pkl\n", encoding="utf-8"
    )

    tasks = _artifact_tasks(project_yaml)

    ids = sorted(task.id for task in tasks)
    assert ids == ["scaler", "schema"]
    schema = next(task for task in tasks if task.id == "schema")
    assert schema.output == "schema.json"
    scaler = next(task for task in tasks if task.id == "scaler")
    assert scaler.split_label == "all"
    assert scaler.output == "stats.pkl"


def test_stats_task_loads_configs(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "stats.yaml").write_text(
        "id: stats\nkind: artifact\noutput: build/custom-stats.json\nmode: raw\n",
        encoding="utf-8",
    )

    tasks = _artifact_tasks(project_yaml)
    assert [task.id for task in tasks] == ["stats"]
    stats = tasks[0]
    assert stats.output == "build/custom-stats.json"
    assert stats.mode == "raw"


def test_stats_task_requires_explicit_mode(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "stats.yaml").write_text(
        "id: stats\nkind: artifact\noutput: build/custom-stats.json\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="mode|Field required"):
        _artifact_tasks(project_yaml)


def test_serve_tasks_respect_name_and_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "serve.train.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\nkeep: train\n", encoding="utf-8"
    )
    (tasks_dir / "serve.val.yaml").write_text(
        "cmd: serve\nname: val\ntarget: pipeline\nkeep: val\nenabled: false\n", encoding="utf-8"
    )

    tasks = _serve_profiles(project_yaml)

    assert [task.name for task in tasks] == ["train", "val"]
    assert [task.keep for task in tasks if task.enabled] == ["train"]


def test_serve_profiles_interpolate_project_globals(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: momentum",
                "variant: price",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: artifacts",
                "  tasks: tasks",
                "  profiles: profiles",
                "globals:",
                "  adv_window_days: 20",
                "  volatility_window_days: 63",
                "  momentum_skip_days: 21",
                "  momentum_lag_days: 189",
                "  forward_return_horizon_days: 126",
                "  dataset_version: ${project_name}_${project_variant}_adv${adv_window_days}_vol${volatility_window_days}_mom${momentum_lag_days}_${momentum_skip_days}_fwd${forward_return_horizon_days}",
            ]
        ),
        encoding="utf-8",
    )
    profiles_dir = _profile_kind_dir(project_yaml)
    (profiles_dir / "serve.processed.yaml").write_text(
        "\n".join(
            [
                "cmd: serve",
                "name: processed",
                "target: pipeline",
                "output:",
                "  transport: fs",
                "  format: jsonl",
                "  view: raw",
                "  directory: ../../data/processed/${dataset_version}",
                "  filename: ${dataset_version}",
            ]
        ),
        encoding="utf-8",
    )

    profile = _serve_profiles(project_yaml)[0]

    expected = "momentum_price_adv20_vol63_mom189_21_fwd126"
    assert profile.output.directory == Path(f"../../data/processed/{expected}")
    assert profile.output.filename == expected


def test_profile_defaults_interpolate_project_globals(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: momentum",
                "variant: price",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: artifacts",
                "  tasks: tasks",
                "  profiles: profiles",
                "globals:",
                "  dataset_version: ${project_name}_${project_variant}",
            ]
        ),
        encoding="utf-8",
    )
    profiles_dir = _profile_kind_dir(project_yaml)
    (profiles_dir / "serve.defaults.yaml").write_text(
        "\n".join(
            [
                "cmd: serve",
                "output:",
                "  transport: fs",
                "  format: jsonl",
                "  view: raw",
                "  directory: ../../data/processed/${dataset_version}",
            ]
        ),
        encoding="utf-8",
    )

    defaults = _serve_defaults(project_yaml)

    assert defaults.output.directory == Path("../../data/processed/momentum_price")


def test_profile_name_is_required(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "serve.train.yaml").write_text(
        "cmd: serve\ntarget: pipeline\nkeep: train\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="serve\\.name|Field required"):
        _serve_profiles(project_yaml)


def test_profile_version_field_is_rejected(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_dir = _profile_kind_dir(project_yaml)
    (profiles_dir / "serve.train.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\nversion: 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        _serve_profiles(project_yaml)


def test_profile_defaults_version_field_is_rejected(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_dir = _profile_kind_dir(project_yaml)
    (profiles_dir / "serve.defaults.yaml").write_text(
        "cmd: serve\nversion: 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        _serve_profiles(project_yaml)


def test_build_profiles_load_and_respect_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "build.fast.yaml").write_text(
        "cmd: build\nname: fast\nenabled: true\nmode: auto\ntarget: schema\n",
        encoding="utf-8",
    )
    (tasks_dir / "build.full.yaml").write_text(
        "cmd: build\nname: full\nenabled: false\nmode: force\ntarget: metadata\n",
        encoding="utf-8",
    )

    tasks = _build_profiles(project_yaml)
    assert [task.name for task in tasks] == ["fast", "full"]
    assert tasks[0].target == "schema"
    assert tasks[0].enabled is True
    assert tasks[1].enabled is False


def test_inspect_profiles_load_and_respect_enabled(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "inspect.default.yaml").write_text(
        "cmd: inspect\nname: default\nenabled: true\ntarget: coverage\n",
        encoding="utf-8",
    )
    (tasks_dir / "inspect.extra.yaml").write_text(
        "cmd: inspect\nname: extra\nenabled: false\ntarget: coverage\n",
        encoding="utf-8",
    )

    tasks = _inspect_profiles(project_yaml)
    assert [task.name for task in tasks] == ["default", "extra"]
    assert tasks[0].target == "coverage"
    assert tasks[0].enabled is True
    assert tasks[1].enabled is False


def test_profile_order_overrides_file_order(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_dir = _profile_kind_dir(project_yaml)
    (profiles_dir / "serve.train.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\n",
        encoding="utf-8",
    )
    (profiles_dir / "serve.val.yaml").write_text(
        "cmd: serve\nname: val\norder: 2\ntarget: pipeline\n",
        encoding="utf-8",
    )
    (profiles_dir / "serve.test.yaml").write_text(
        "cmd: serve\nname: test\norder: 1\ntarget: pipeline\n",
        encoding="utf-8",
    )

    tasks = _serve_profiles(project_yaml)
    assert [task.name for task in tasks] == ["test", "val", "train"]


def test_serve_defaults_are_loaded_but_not_executable_profiles(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_dir = _profile_kind_dir(project_yaml)
    (profiles_dir / "serve.defaults.yaml").write_text(
        (
            "cmd: serve\n"
            "observability:\n"
            "  logging:\n"
            "    outputs:\n"
            "      - transport: stdout\n"
        ),
        encoding="utf-8",
    )
    (profiles_dir / "serve.train.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\n",
        encoding="utf-8",
    )

    tasks = _serve_profiles(project_yaml)
    defaults = _serve_defaults(project_yaml)

    assert [task.name for task in tasks] == ["train"]
    assert defaults is not None
    assert defaults.cmd == "serve"
    assert defaults.observability is not None


def test_profile_defaults_reject_executable_fields(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_dir = _profile_kind_dir(project_yaml)
    (profiles_dir / "serve.defaults.yaml").write_text(
        "cmd: serve\nname: should-not-exist\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        _serve_profiles(project_yaml)


def test_duplicate_profile_defaults_per_kind_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_dir = _profile_kind_dir(project_yaml)
    (profiles_dir / "serve.defaults.yaml").write_text(
        "cmd: serve\n",
        encoding="utf-8",
    )
    (profiles_dir / "serve.defaults.yml").write_text(
        "cmd: serve\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate serve defaults"):
        _serve_profiles(project_yaml)


def test_artifact_operation_rejects_dependencies_field(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "metadata.yaml").write_text(
        "id: metadata\nkind: artifact\ndependencies:\n  - schema\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        _artifact_tasks(project_yaml)


def test_serve_operation_tasks_load(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "pipeline.yaml").write_text(
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
        encoding="utf-8",
    )

    tasks = _all_tasks(project_yaml)
    assert [task.id for task in tasks] == ["pipeline"]
    assert tasks[0].entrypoint == "core.runtime.pipeline"


def test_runtime_operation_options_load_and_normalize(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "pipeline.yaml").write_text(
        (
            "id: pipeline\n"
            "kind: runtime\n"
            "entrypoint: core.runtime.pipeline\n"
            "options:\n"
            "  sort: missing\n"
        ),
        encoding="utf-8",
    )

    tasks = _all_tasks(project_yaml)
    operation = tasks[0]
    assert operation.options == {"sort": "missing"}


def test_runtime_operation_rejects_dependencies_field(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "pipeline.yaml").write_text(
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\ndependencies:\n  - missing_artifact\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        _all_tasks(project_yaml)


def test_runtime_operation_rejects_output_formats_field(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "pipeline.yaml").write_text(
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\noutput_formats:\n  - jsonl\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        _all_tasks(project_yaml)


def test_duplicate_artifact_kinds_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "schema.a.yaml").write_text(
        "id: schema\nkind: artifact\noutput: schema-a.json\n", encoding="utf-8"
    )
    (tasks_dir / "schema.b.yaml").write_text(
        "id: schema\nkind: artifact\noutput: schema-b.json\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="Duplicate artifact task ids"):
        _artifact_tasks(project_yaml)


def test_duplicate_serve_profile_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "serve.a.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\n", encoding="utf-8"
    )
    (tasks_dir / "serve.b.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="Duplicate serve profile names"):
        _serve_profiles(project_yaml)


def test_duplicate_serve_operation_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "pipeline.a.yaml").write_text(
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
        encoding="utf-8",
    )
    (tasks_dir / "pipeline.b.yaml").write_text(
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate operation task ids"):
        _all_tasks(project_yaml)


def test_task_ids_must_be_globally_unique_across_kinds(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "schema.artifact.yaml").write_text(
        "id: schema\nkind: artifact\noutput: schema.json\n",
        encoding="utf-8",
    )
    (tasks_dir / "schema.runtime.yaml").write_text(
        "id: schema\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="globally unique"):
        _all_tasks(project_yaml)


def test_duplicate_build_profile_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "build.a.yaml").write_text(
        "cmd: build\nname: nightly\ntarget: schema\n",
        encoding="utf-8",
    )
    (tasks_dir / "build.b.yaml").write_text(
        "cmd: build\nname: nightly\ntarget: metadata\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate build profile names"):
        _build_profiles(project_yaml)


def test_duplicate_inspect_profile_names_raise(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _profile_kind_dir(project_yaml)
    (tasks_dir / "inspect.a.yaml").write_text(
        "cmd: inspect\nname: inspect\ntarget: coverage\n",
        encoding="utf-8",
    )
    (tasks_dir / "inspect.b.yaml").write_text(
        "cmd: inspect\nname: inspect\ntarget: coverage\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate inspect profile names"):
        _inspect_profiles(project_yaml)


def test_root_level_task_files_are_rejected(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_root = project_yaml.parent / "tasks"
    tasks_root.mkdir(parents=True, exist_ok=True)
    (tasks_root / "pipeline.yaml").write_text(
        "id: pipeline\nkind: runtime\nentrypoint: core.runtime.pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="tasks/operations"):
        _all_tasks(project_yaml)


def test_nested_profile_files_are_rejected(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_root = _profiles_dir(project_yaml) / "serve"
    profiles_root.mkdir(parents=True, exist_ok=True)
    (profiles_root / "train.yaml").write_text(
        "cmd: serve\nname: train\ntarget: pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="flat under profiles/"):
        _serve_profiles(project_yaml)


def test_profile_kind_directory_mismatch_is_rejected(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    build_dir = _profile_kind_dir(project_yaml)
    (build_dir / "build.oops.yaml").write_text(
        "cmd: serve\nname: oops\ntarget: pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="declares profile cmd 'serve'"):
        _build_profiles(project_yaml)


def test_profile_filename_prefix_is_required(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_root = _profiles_dir(project_yaml)
    (profiles_root / "oops.yaml").write_text(
        "cmd: serve\nname: oops\ntarget: pipeline\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must use \\{serve,build,inspect\\}"):
        _serve_profiles(project_yaml)


def test_profile_rejects_unknown_fields(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    profiles_root = _profiles_dir(project_yaml)
    (profiles_root / "build.schema.yaml").write_text(
        "cmd: build\nname: schema\ntarget: schema\noutput: should-not-exist\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        _build_profiles(project_yaml)


def test_operation_task_rejects_unknown_fields(tmp_path):
    project_yaml = _write_project(tmp_path, tasks_ref="tasks")
    tasks_dir = _operations_dir(project_yaml)
    (tasks_dir / "pipeline.yaml").write_text(
        (
            "id: pipeline\n"
            "kind: runtime\n"
            "entrypoint: core.runtime.pipeline\n"
            "runtime_kind: inspect\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        _all_tasks(project_yaml)
