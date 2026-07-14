from pathlib import Path

import pytest
from pydantic import ValidationError

from datapipeline.cli.workspace import WorkspaceContext
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.dataset.split import HashSplitConfig
from datapipeline.config.profiles import (
    BuildProfile,
    InspectProfile,
    ServeOutputConfig,
    ServeProfile,
)
from datapipeline.config.preview import PreviewStage
from datapipeline.execution.settings import LogOutputTarget
from datapipeline.profiles.runtime_profiles import resolve_runtime_profiles
from datapipeline.config.workspace import WorkspaceConfig
from datapipeline.profiles.request_builder import build_cli_output_config
from tests.unit.profiles.helpers import pipeline_definition


def _resolve(
    project_path: Path,
    profiles: list[ServeProfile | InspectProfile],
    preview: PreviewStage | None = None,
    cli_output: ServeOutputConfig | None = None,
    cli_log_outputs: list[LogOutputTarget] | None = None,
    cli_heartbeat_interval_seconds: float | None = None,
    split: HashSplitConfig | None = None,
):
    definition = pipeline_definition(
        project_path,
        dataset=FeatureDatasetConfig(
            sample=SampleConfig(cadence="1h"),
            split=split,
        ),
    )
    return resolve_runtime_profiles(
        definition=definition,
        profiles=profiles,
        preview=preview,
        limit=None,
        cli_output=cli_output,
        cli_log_level=None,
        cli_log_outputs=cli_log_outputs,
        base_log_level="INFO",
        cli_visuals=None,
        cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
    )


def test_serve_profile_accepts_splits_list():
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train", "val"],
        }
    )

    assert profile.splits == ["train", "val"]


def test_serve_profile_rejects_splits_string():
    with pytest.raises(ValidationError, match="splits must be a list"):
        ServeProfile.model_validate(
            {
                "cmd": "serve",
                "name": "splits",
                "target": "serve",
                "splits": "train",
            }
        )


def test_serve_profile_rejects_numeric_preview() -> None:
    with pytest.raises(ValidationError, match="preview"):
        ServeProfile.model_validate(
            {
                "cmd": "serve",
                "name": "preview",
                "target": "serve",
                "preview": 3,
            }
        )


@pytest.mark.parametrize(
    ("profile_type", "command"),
    [(ServeProfile, "serve"), (InspectProfile, "inspect")],
)
def test_runtime_profiles_use_flat_artifact_mode(profile_type, command):
    profile = profile_type.model_validate(
        {
            "cmd": command,
            "name": "example",
            "target": "serve",
            "artifact_mode": "force",
        }
    )

    assert profile.artifact_mode == "FORCE"


@pytest.mark.parametrize(
    ("profile_type", "command"),
    [(ServeProfile, "serve"), (InspectProfile, "inspect")],
)
def test_runtime_profiles_reject_nested_build_config(profile_type, command):
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        profile_type.model_validate(
            {
                "cmd": command,
                "name": "example",
                "target": "serve",
                "build": {"mode": "AUTO"},
            }
        )


def test_runtime_profiles_resolve_heartbeat_setting(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {"heartbeat_interval_seconds": 30},
        }
    )
    resolved = _resolve(tmp_path, [profile])[0]
    assert resolved.observability.heartbeat_interval_seconds == 30

    cli_override = _resolve(
        tmp_path,
        [profile],
        cli_heartbeat_interval_seconds=0,
    )[0]
    assert cli_override.observability.heartbeat_interval_seconds == 0


def test_run_profiles_resolve_splits_for_fs_output(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train", "val"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
            },
        }
    )
    resolved = _resolve(
        tmp_path / "project.yaml",
        [profile],
        split=HashSplitConfig(ratios={"train": 0.8, "val": 0.2}),
    )[0]

    assert resolved.name == "splits"
    assert resolved.target_id == "serve"
    assert resolved.splits == ("train", "val")
    assert not hasattr(resolved, "runtime")
    assert resolved.output.transport == "fs"


def test_run_profiles_reject_splits_without_dataset_split(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
            },
        }
    )

    with pytest.raises(ValueError, match="dataset split is not configured"):
        _resolve(tmp_path / "project.yaml", [profile])


def test_run_profiles_reject_unknown_splits(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train", "test"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
            },
        }
    )
    with pytest.raises(ValueError, match="unknown split labels: 'test'"):
        _resolve(
            tmp_path / "project.yaml",
            [profile],
            split=HashSplitConfig(ratios={"train": 0.8, "val": 0.2}),
        )


def test_run_profiles_reject_splits_for_stdout(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train"],
        }
    )
    with pytest.raises(ValueError, match="output transport is not fs"):
        _resolve(
            tmp_path / "project.yaml",
            [profile],
            split=HashSplitConfig(ratios={"train": 1.0}),
        )


def test_run_profiles_reject_splits_with_explicit_filename(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
                "filename": "vectors",
            },
        }
    )
    with pytest.raises(ValueError, match="cannot set output.filename with splits"):
        _resolve(
            tmp_path / "project.yaml",
            [profile],
            split=HashSplitConfig(ratios={"train": 1.0}),
        )


def test_run_profiles_reject_splits_with_colliding_output_filenames(
    tmp_path,
):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["north/west", "north_west"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
            },
        }
    )
    with pytest.raises(ValueError, match="same output filename"):
        _resolve(
            tmp_path / "project.yaml",
            [profile],
            split=HashSplitConfig(ratios={"north/west": 0.5, "north_west": 0.5}),
        )


def test_operation_options_rejects_preview_when_unsupported(tmp_path):
    profile = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "coverage",
            "target": "coverage",
            "artifact_mode": "AUTO",
        }
    )

    with pytest.raises(ValueError, match="does not support previews"):
        _resolve(tmp_path, [profile], preview="source")


def test_run_profiles_leave_unconfigured_throttle_unset(tmp_path):
    profile = ServeProfile.model_validate(
        {"cmd": "serve", "name": "demo", "target": "serve"}
    )

    resolved = _resolve(tmp_path, [profile])[0]

    assert resolved.throttle_ms is None


def test_run_profiles_use_builtin_visuals_defaults(tmp_path):
    profile = ServeProfile.model_validate(
        {"cmd": "serve", "name": "demo", "target": "serve"}
    )

    resolved = _resolve(tmp_path, [profile])[0]

    assert resolved.observability.visuals == "on"


def test_run_profiles_run_visuals_override_defaults(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {"visuals": "ON"},
        }
    )

    resolved = _resolve(tmp_path, [profile])[0]

    assert resolved.observability.visuals == "on"


def test_run_profiles_resolve_log_output_precedence(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {"logging": {"outputs": [{"transport": "stdout"}]}},
        }
    )

    resolved = _resolve(tmp_path / "project.yaml", [profile])[0]
    assert resolved.observability.log_output.outputs[0].transport == "stdout"
    assert resolved.observability.log_output.outputs[0].destination is None

    cli_log = tmp_path / "logs" / "cli.log"
    cli_override = _resolve(
        tmp_path / "project.yaml",
        [profile],
        cli_log_outputs=[LogOutputTarget(transport="fs", destination=cli_log)],
    )[0]
    assert cli_override.observability.log_output.outputs[0].transport == "fs"
    assert cli_override.observability.log_output.outputs[0].destination == cli_log


def test_execution_scoped_logs_can_be_resolved_for_inspect_profiles(tmp_path):
    inspect_profile = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "demo",
            "target": "coverage",
            "observability": {
                "logging": {
                    "outputs": [
                        {
                            "transport": "fs",
                            "scope": "execution",
                            "path": "logs/run.log",
                        }
                    ]
                }
            },
        }
    )
    cli_output = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        directory=tmp_path / "out",
    )

    resolved_inspect = _resolve(
        tmp_path / "project.yaml",
        [inspect_profile],
        cli_output=cli_output,
    )[0]
    inspect_log = resolved_inspect.observability.log_output.outputs[0]
    assert inspect_log.scope == "execution"
    assert inspect_log.destination == Path("logs/run.log")

    serve_profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {
                "logging": {
                    "outputs": [
                        {
                            "transport": "fs",
                            "scope": "execution",
                            "path": "logs/run.log",
                        }
                    ]
                }
            },
        }
    )
    resolved_serve = _resolve(
        tmp_path / "project.yaml",
        [serve_profile],
        cli_output=cli_output,
    )[0]
    serve_log = resolved_serve.observability.log_output.outputs[0]
    assert resolved_serve.output.run is not None
    assert serve_log.scope == "execution"
    assert serve_log.destination == Path("logs/run.log")


def test_execution_scoped_logs_default_to_task_specific_filename(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "target": "serve",
            "name": "val",
            "observability": {
                "logging": {"outputs": [{"transport": "fs", "scope": "execution"}]}
            },
        }
    )
    cli_output = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        directory=tmp_path / "out",
    )

    resolved = _resolve(
        tmp_path / "project.yaml",
        [profile],
        cli_output=cli_output,
    )[0]
    log_output = resolved.observability.log_output.outputs[0]
    assert resolved.output.run is not None
    assert log_output.scope == "execution"
    assert log_output.destination is None


def test_serve_runtime_profiles_share_run_and_namespace_splits(tmp_path):
    profiles = [
        ServeProfile.model_validate(
            {
                "cmd": "serve",
                "name": name,
                "target": "serve",
                "splits": splits,
            }
        )
        for name, splits in (
            ("first", ["train"]),
            ("second", ["train"]),
            ("train", None),
        )
    ]
    cli_output = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        directory=tmp_path / "out",
    )

    resolved = _resolve(
        tmp_path / "project.yaml",
        profiles,
        cli_output=cli_output,
        split=HashSplitConfig(ratios={"train": 1.0}),
    )

    run_ids = {
        profile.output.run.run_id
        for profile in resolved
        if profile.output.run is not None
    }
    dataset_dirs = {
        profile.output.run.dataset_dir
        for profile in resolved
        if profile.output.run is not None
    }
    assert len(run_ids) == 1
    assert len(dataset_dirs) == 1

    profiles_by_name = {profile.name: profile for profile in resolved}
    assert {profile.output.destination.name for profile in resolved} == {
        "first.jsonl",
        "second.jsonl",
        "train.jsonl",
    }
    assert {
        profiles_by_name["first"].output.for_split("train").destination.name,
        profiles_by_name["second"].output.for_split("train").destination.name,
        profiles_by_name["train"].output.destination.name,
    } == {
        "first.train.jsonl",
        "second.train.jsonl",
        "train.jsonl",
    }
    planned_run = resolved[0].output.run
    assert planned_run is not None
    assert not planned_run.serve_root.exists()
    assert not planned_run.dataset_dir.exists()
    assert not planned_run.metadata_path.exists()


def test_runtime_profiles_reject_sanitized_output_collision(tmp_path):
    profiles = [
        ServeProfile.model_validate({"cmd": "serve", "name": name, "target": "serve"})
        for name in ("daily/eu", "daily_eu")
    ]
    output_root = tmp_path / "out"

    with pytest.raises(ValueError, match="resolve to the same path"):
        _resolve(
            tmp_path / "project.yaml",
            profiles,
            cli_output=ServeOutputConfig(
                transport="fs",
                format="jsonl",
                directory=output_root,
            ),
        )

    assert not output_root.exists()


def test_shared_serve_run_rejects_mixed_preview_stages_without_writes(tmp_path):
    profiles = [
        ServeProfile.model_validate(
            {
                "cmd": "serve",
                "name": name,
                "target": "serve",
                "preview": preview,
            }
        )
        for name, preview in (("first", "source"), ("second", "mapped"))
    ]
    output_root = tmp_path / "out"

    with pytest.raises(ValueError, match="must use the same preview"):
        _resolve(
            tmp_path / "project.yaml",
            profiles,
            cli_output=ServeOutputConfig(
                transport="fs",
                format="jsonl",
                directory=output_root,
            ),
        )

    assert not output_root.exists()


def test_shared_serve_runs_reject_explicit_output_filename(tmp_path):
    profiles = [
        ServeProfile.model_validate(
            {
                "cmd": "serve",
                "name": name,
                "target": "serve",
                "output": {
                    "transport": "fs",
                    "format": "jsonl",
                    "directory": str(tmp_path / "out"),
                    "filename": "vectors",
                },
            }
        )
        for name in ("train", "val")
    ]

    with pytest.raises(ValueError, match="cannot set output.filename"):
        _resolve(tmp_path / "project.yaml", profiles)

    assert not (tmp_path / "out").exists()


def test_cli_output_directory_resolves_relative_to_workspace(tmp_path):
    workspace = WorkspaceContext(
        file_path=tmp_path / "jerry.yaml",
        config=WorkspaceConfig.model_validate({}),
    )

    config = build_cli_output_config(
        "fs",
        "jsonl",
        ".",
        workspace=workspace,
    )

    assert config is not None
    assert config.directory == tmp_path.resolve()


def test_inspect_profiles_accept_html_output_for_matrix(tmp_path):
    profile = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "matrix",
            "target": "matrix",
            "artifact_mode": "AUTO",
            "output": {
                "transport": "fs",
                "format": "html",
                "directory": str(tmp_path / "inspect"),
            },
        }
    )

    resolved = _resolve(tmp_path, [profile])[0]

    assert resolved.output.format == "html"
    assert resolved.output.transport == "fs"


def test_inspect_profile_model_allows_html_output_for_any_target(tmp_path):
    profile = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "coverage",
            "target": "coverage",
            "artifact_mode": "AUTO",
            "output": {
                "transport": "fs",
                "format": "html",
                "directory": str(tmp_path / "inspect"),
            },
        }
    )

    assert profile.output is not None
    assert profile.output.format == "html"


def test_inspect_profiles_accept_cli_html_override_for_non_matrix(tmp_path):
    profile = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "coverage",
            "target": "coverage",
            "artifact_mode": "AUTO",
        }
    )

    resolved = _resolve(
        tmp_path,
        [profile],
        cli_output=ServeOutputConfig(
            transport="fs",
            format="html",
            directory=tmp_path / "inspect",
        ),
    )[0]

    assert resolved.output.format == "html"


def test_serve_profile_model_allows_html_output(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "serve",
            "target": "serve",
            "output": {
                "transport": "fs",
                "format": "html",
                "directory": str(tmp_path / "serve"),
            },
        }
    )

    assert profile.output is not None
    assert profile.output.format == "html"


def test_serve_profiles_accept_cli_txt_override(tmp_path):
    profile = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "serve",
            "target": "serve",
            "artifact_mode": "AUTO",
        }
    )

    resolved = _resolve(
        tmp_path,
        [profile],
        cli_output=ServeOutputConfig(
            transport="fs",
            format="txt",
            directory=tmp_path / "serve",
        ),
    )[0]

    assert resolved.output.format == "txt"


def test_build_profile_rejects_runtime_output_fields():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        BuildProfile.model_validate(
            {
                "cmd": "build",
                "name": "schema",
                "target": "schema",
                "output": {
                    "transport": "fs",
                    "format": "jsonl",
                    "directory": "out",
                },
            }
        )
