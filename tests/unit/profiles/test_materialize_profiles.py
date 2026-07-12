import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.observability import (
    LoggingConfig,
    LogOutputConfig,
    ObservabilityConfig,
)
from datapipeline.config.profiles import MaterializeProfile, MaterializeProfileDefaults
from datapipeline.config.tasks import ArtifactTask, TicksTask
from datapipeline.profiles import materialize as materialize_profiles
from datapipeline.services.materialize import MaterializeResult


def _profile(
    name: str,
    stream: str,
    output: str,
    *,
    enabled: bool = True,
    overwrite: bool = False,
    observability: ObservabilityConfig | None = None,
) -> MaterializeProfile:
    return MaterializeProfile(
        cmd="materialize",
        name=name,
        stream=stream,
        output=output,
        enabled=enabled,
        overwrite=overwrite,
        observability=observability,
    )


def _prepare_run(
    monkeypatch,
    tmp_path: Path,
    profiles: list[MaterializeProfile],
    defaults: MaterializeProfileDefaults | None = None,
):
    if defaults is None:
        defaults = MaterializeProfileDefaults(cmd="materialize")
    stream_ids = {profile.stream for profile in profiles}
    runtime = SimpleNamespace(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
        registries=SimpleNamespace(
            stream_specs={stream_id: object() for stream_id in stream_ids}
        ),
        heartbeat_interval_seconds=None,
    )
    writes = []
    specs = []
    messages = []

    monkeypatch.setattr(
        materialize_profiles,
        "profile_specs_with_defaults",
        lambda project_path, command: (profiles, defaults),
    )
    monkeypatch.setattr(materialize_profiles, "bootstrap", lambda project_path: runtime)
    monkeypatch.setattr(
        materialize_profiles, "load_streams", lambda project_path: StreamsConfig()
    )
    monkeypatch.setattr(
        materialize_profiles,
        "load_dataset",
        lambda project_path, dataset_name: object(),
    )
    monkeypatch.setattr(
        materialize_profiles,
        "execution_root",
        lambda project_path: tmp_path / "execution",
    )

    def _run_execution(spec, work):
        specs.append(spec)
        return work()

    def _materialize_stream_to_path(**kwargs):
        writes.append(kwargs)
        output = kwargs["output"]
        return MaterializeResult(
            count=1,
            output=output,
            metadata=output.with_suffix(".metadata.json"),
        )

    monkeypatch.setattr(materialize_profiles, "run_execution", _run_execution)
    monkeypatch.setattr(
        materialize_profiles,
        "materialize_stream_to_path",
        _materialize_stream_to_path,
    )
    monkeypatch.setattr(
        materialize_profiles,
        "emit_file_result",
        lambda label, path, records=None: messages.append((label, path, records)),
    )
    return writes, specs, messages


def _run(
    tmp_path: Path,
    run_name: str | None = None,
    overwrite: bool | None = None,
    output: Path | None = None,
    artifact_mode: str | None = None,
):
    return materialize_profiles.run_materialize_profiles(
        project_path=tmp_path / "project.yaml",
        run_name=run_name,
        overwrite=overwrite,
        cli_output=output,
        cli_visuals=None,
        cli_heartbeat_interval_seconds=None,
        cli_artifact_mode=artifact_mode,
        cli_log_level=None,
        cli_log_outputs=[],
        base_log_level="INFO",
    )


def test_materialize_runs_all_enabled_profiles_in_order(monkeypatch, tmp_path) -> None:
    profiles = [
        _profile("adv-20", "adv.20", "adv-20.jsonl"),
        _profile(
            "disabled",
            "adv.63",
            "disabled.jsonl",
            enabled=False,
        ),
        _profile(
            "adv-126",
            "adv.126",
            "adv-126.jsonl",
            overwrite=True,
        ),
    ]
    writes, specs, messages = _prepare_run(monkeypatch, tmp_path, profiles)
    profile_messages: list[tuple[str, int]] = []
    monkeypatch.setattr(
        materialize_profiles,
        "emit_execution_message",
        lambda message, level: profile_messages.append((message, level)),
    )

    results = _run(tmp_path)

    assert [call["stream_id"] for call in writes] == ["adv.20", "adv.126"]
    assert [call["overwrite"] for call in writes] == [False, True]
    assert len(specs) == 2
    assert [result.output.name for result in results] == [
        "adv-20.jsonl",
        "adv-126.jsonl",
    ]
    assert not (tmp_path / "execution").exists()
    assert profile_messages == [
        (
            f"Profile: materialize adv-20 (1/2) stream=adv.20 "
            f"output={tmp_path / 'adv-20.jsonl'} overwrite=false",
            logging.DEBUG,
        ),
        (
            f"Profile: materialize adv-126 (2/2) stream=adv.126 "
            f"output={tmp_path / 'adv-126.jsonl'} overwrite=true",
            logging.DEBUG,
        ),
    ]
    assert messages == [
        ("Output", tmp_path / "adv-20.jsonl", None),
        ("Metadata", tmp_path / "adv-20.metadata.json", None),
        ("Output", tmp_path / "adv-126.jsonl", None),
        ("Metadata", tmp_path / "adv-126.metadata.json", None),
    ]


def test_materialize_uses_command_execution_defaults(monkeypatch, tmp_path) -> None:
    defaults = MaterializeProfileDefaults(
        cmd="materialize",
        execution=ExecutionConfig(sort_batch_records=32),
    )
    writes, _, _ = _prepare_run(
        monkeypatch,
        tmp_path,
        [_profile("adv-20", "adv.20", "adv-20.jsonl")],
        defaults,
    )

    _run(tmp_path)

    assert writes[0]["runtime"].execution == defaults.execution


@pytest.mark.parametrize(
    ("default_mode", "cli_mode", "expected_mode"),
    [
        (None, None, "AUTO"),
        ("FORCE", None, "FORCE"),
        ("FORCE", "OFF", "OFF"),
    ],
)
def test_materialize_prepares_required_artifacts_once(
    monkeypatch,
    tmp_path,
    default_mode,
    cli_mode,
    expected_mode,
) -> None:
    default_observability = ObservabilityConfig(
        visuals="off",
        heartbeat_interval_seconds=12,
        logging=LoggingConfig(
            level="warning",
            outputs=[LogOutputConfig(transport="fs", scope="execution")],
        ),
    )
    profile_observability = ObservabilityConfig(
        visuals="on",
        heartbeat_interval_seconds=99,
        logging=LoggingConfig(level="debug"),
    )
    defaults = MaterializeProfileDefaults(
        cmd="materialize",
        artifact_mode=default_mode,
        observability=default_observability,
    )
    profiles = [
        _profile(name, stream, f"{name}.jsonl", observability=profile_observability)
        for name, stream in (("adv-20", "adv.20"), ("adv-63", "adv.63"))
    ]
    writes, _, _ = _prepare_run(monkeypatch, tmp_path, profiles, defaults)
    monkeypatch.setattr(
        materialize_profiles,
        "stream_cadence_artifacts",
        lambda stream, streams: {
            "adv.20": {"ticks_20"},
            "adv.63": {"ticks_63"},
        }[stream],
    )
    monkeypatch.setattr(
        materialize_profiles,
        "operation_specs",
        lambda project_path: (
            [
                TicksTask(id="ticks_20", stream="raw.20", output="ticks-20.jsonl"),
                TicksTask(id="ticks_63", stream="raw.63", output="ticks-63.jsonl"),
            ],
            [],
        ),
    )
    build_calls = []
    execution_specs = []
    inside_artifact_execution = False

    def run_execution(spec, work):
        nonlocal inside_artifact_execution
        execution_specs.append(spec)
        inside_artifact_execution = True
        try:
            return work()
        finally:
            inside_artifact_execution = False

    def run_build(project_path, **kwargs):
        assert inside_artifact_execution
        assert writes == []
        build_calls.append(kwargs)

    monkeypatch.setattr(materialize_profiles, "run_execution", run_execution)
    monkeypatch.setattr(
        materialize_profiles,
        "run_build_if_needed",
        run_build,
    )

    _run(tmp_path, artifact_mode=cli_mode)

    assert len(execution_specs) == 3
    artifact_spec, *profile_specs = execution_specs
    assert len(build_calls) == 1
    assert build_calls[0]["required_artifacts"] == {"ticks_20", "ticks_63"}
    assert build_calls[0]["mode"] == expected_mode
    assert build_calls[0]["heartbeat_interval_seconds"] == 12
    assert build_calls[0]["runtime"] is writes[0]["runtime"]
    assert artifact_spec.visuals == "off"
    assert artifact_spec.log_decision.name == "WARNING"
    artifact_log = artifact_spec.log_output.outputs[0].destination
    assert artifact_log is not None
    assert artifact_log.name == "materialize.artifacts.log"
    assert [spec.visuals for spec in profile_specs] == ["on", "on"]
    assert [spec.log_decision.name for spec in profile_specs] == ["DEBUG", "DEBUG"]
    assert {
        spec.log_output.outputs[0].destination.name
        for spec in profile_specs
        if spec.log_output.outputs[0].destination is not None
    } == {"materialize.adv-20.log", "materialize.adv-63.log"}


@pytest.mark.parametrize(
    ("artifact_tasks", "message"),
    [
        ([], "requires a declared ticks task"),
        (
            [
                ArtifactTask(
                    id="market_ticks",
                    entrypoint="plugin.snapshot",
                    output="snapshot.json",
                )
            ],
            "not a ticks task",
        ),
    ],
)
def test_materialize_rejects_invalid_tick_artifact_producer(
    monkeypatch,
    tmp_path,
    artifact_tasks,
    message,
) -> None:
    writes, _, _ = _prepare_run(
        monkeypatch,
        tmp_path,
        [_profile("adv-20", "adv.20", "adv-20.jsonl")],
    )
    monkeypatch.setattr(
        materialize_profiles,
        "stream_cadence_artifacts",
        lambda stream, streams: {"market_ticks"},
    )
    monkeypatch.setattr(
        materialize_profiles,
        "operation_specs",
        lambda project_path: (artifact_tasks, []),
    )

    with pytest.raises(materialize_profiles.MaterializeProfileError, match=message):
        _run(tmp_path)

    assert writes == []


def test_materialize_run_selects_one_profile_even_when_disabled(
    monkeypatch,
    tmp_path,
) -> None:
    profiles = [
        _profile("adv-20", "adv.20", "adv-20.jsonl"),
        _profile(
            "adv-63",
            "adv.63",
            "adv-63.jsonl",
            enabled=False,
        ),
    ]
    writes, specs, _ = _prepare_run(monkeypatch, tmp_path, profiles)

    _run(tmp_path, run_name="adv-63", overwrite=True)

    assert [call["stream_id"] for call in writes] == ["adv.63"]
    assert writes[0]["overwrite"] is True
    assert len(specs) == 1


def test_materialize_cli_overrides_selected_profile_output(
    monkeypatch, tmp_path
) -> None:
    profiles = [_profile("adv-20", "adv.20", "configured.jsonl")]
    writes, _, _ = _prepare_run(monkeypatch, tmp_path, profiles)
    output = tmp_path / "overridden.jsonl"

    _run(
        tmp_path,
        run_name="adv-20",
        output=output,
    )

    assert writes[0]["output"] == output


def test_materialize_rejects_empty_run_name(monkeypatch, tmp_path) -> None:
    profiles = [_profile("adv-20", "adv.20", "adv-20.jsonl")]
    writes, _, _ = _prepare_run(monkeypatch, tmp_path, profiles)

    with pytest.raises(
        materialize_profiles.MaterializeProfileError,
        match="profile name must not be empty",
    ):
        _run(tmp_path, run_name="")

    assert writes == []
    assert not (tmp_path / "execution").exists()


def test_materialize_preflight_rejects_duplicate_outputs_before_writing(
    monkeypatch,
    tmp_path,
) -> None:
    profiles = [
        _profile("adv-20", "adv.20", "same.jsonl"),
        _profile("adv-63", "adv.63", "same.jsonl"),
    ]
    writes, _, _ = _prepare_run(monkeypatch, tmp_path, profiles)

    with pytest.raises(ValueError, match="write the same path"):
        _run(tmp_path)

    assert writes == []
    assert not (tmp_path / "execution").exists()


def test_materialize_preflight_checks_every_output_before_writing(
    monkeypatch,
    tmp_path,
) -> None:
    existing = tmp_path / "second.jsonl"
    existing.write_text("already here\n", encoding="utf-8")
    profiles = [
        _profile("first", "adv.20", "first.jsonl"),
        _profile("second", "adv.63", "second.jsonl"),
    ]
    writes, _, _ = _prepare_run(monkeypatch, tmp_path, profiles)
    monkeypatch.setattr(
        materialize_profiles,
        "load_streams",
        lambda project_path: pytest.fail(
            "artifact planning ran before output preflight"
        ),
    )

    with pytest.raises(FileExistsError, match="--overwrite"):
        _run(tmp_path)

    assert writes == []
    assert not (tmp_path / "execution").exists()


def test_materialize_rejects_outputs_inside_managed_artifacts(
    monkeypatch,
    tmp_path,
) -> None:
    profiles = [
        _profile(
            "ticks",
            "adv.20",
            "artifacts/ticks.jsonl",
            overwrite=True,
        )
    ]
    writes, _, _ = _prepare_run(monkeypatch, tmp_path, profiles)
    monkeypatch.setattr(
        materialize_profiles,
        "load_streams",
        lambda project_path: pytest.fail("artifact preparation ran before preflight"),
    )

    with pytest.raises(
        materialize_profiles.MaterializeProfileError,
        match="inside the managed artifacts root",
    ):
        _run(tmp_path)

    assert writes == []


def test_materialize_reports_success_before_a_later_profile_fails(
    monkeypatch,
    tmp_path,
) -> None:
    profiles = [
        _profile("first", "adv.20", "first.jsonl"),
        _profile("second", "adv.63", "second.jsonl"),
    ]
    _, _, messages = _prepare_run(monkeypatch, tmp_path, profiles)

    def materialize(**kwargs):
        if kwargs["stream_id"] == "adv.63":
            raise RuntimeError("second profile failed")
        output = kwargs["output"]
        return MaterializeResult(
            count=1,
            output=output,
            metadata=output.with_suffix(".metadata.json"),
        )

    monkeypatch.setattr(
        materialize_profiles,
        "materialize_stream_to_path",
        materialize,
    )

    with pytest.raises(RuntimeError, match="second profile failed"):
        _run(tmp_path)

    assert messages == [
        ("Output", tmp_path / "first.jsonl", None),
        ("Metadata", tmp_path / "first.metadata.json", None),
    ]
