from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.config.profiles import MaterializeProfile
from datapipeline.cli.visuals.execution_context import current_execution_scope
from datapipeline.profiles import materialize as materialize_profiles
from datapipeline.services.materialize import MaterializeResult


def _profile(
    name: str,
    stream: str,
    output: str,
    *,
    enabled: bool = True,
    overwrite: bool = False,
) -> MaterializeProfile:
    return MaterializeProfile(
        cmd="materialize",
        name=name,
        stream=stream,
        output=output,
        enabled=enabled,
        overwrite=overwrite,
    )


def _prepare_run(monkeypatch, tmp_path: Path, profiles: list[MaterializeProfile]):
    stream_ids = {profile.stream for profile in profiles}
    runtime = SimpleNamespace(
        project_yaml=tmp_path / "project.yaml",
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
        lambda project_path, command: (profiles, None),
    )
    monkeypatch.setattr(materialize_profiles, "bootstrap", lambda project_path: runtime)
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

    def _run_profile(spec, work):
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

    monkeypatch.setattr(materialize_profiles, "run_profile", _run_profile)
    monkeypatch.setattr(
        materialize_profiles,
        "materialize_stream_to_path",
        _materialize_stream_to_path,
    )
    monkeypatch.setattr(
        materialize_profiles,
        "emit_execution_message",
        lambda message: messages.append((current_execution_scope(), message)),
    )
    return writes, specs, messages


def _run(
    tmp_path: Path,
    run_name: str | None = None,
    overwrite: bool | None = None,
    output: Path | None = None,
):
    return materialize_profiles.run_materialize_profiles(
        project_path=tmp_path / "project.yaml",
        run_name=run_name,
        overwrite=overwrite,
        cli_output=output,
        cli_visuals=None,
        cli_heartbeat_interval_seconds=None,
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

    results = _run(tmp_path)

    assert [call["stream_id"] for call in writes] == ["adv.20", "adv.126"]
    assert [call["overwrite"] for call in writes] == [False, True]
    assert [(spec.name, spec.index, spec.total) for spec in specs] == [
        ("adv-20", 1, 2),
        ("adv-126", 2, 2),
    ]
    assert [result.output.name for result in results] == [
        "adv-20.jsonl",
        "adv-126.jsonl",
    ]
    assert not (tmp_path / "execution").exists()
    assert [scope["profile_name"] for scope, _ in messages] == [
        "adv-20",
        "adv-20",
        "adv-20",
        "adv-126",
        "adv-126",
        "adv-126",
    ]
    assert [message for _, message in messages] == [
        "Result: 1 records",
        f"Output: {tmp_path / 'adv-20.jsonl'}",
        f"Metadata: {tmp_path / 'adv-20.metadata.json'}",
        "Result: 1 records",
        f"Output: {tmp_path / 'adv-126.jsonl'}",
        f"Metadata: {tmp_path / 'adv-126.metadata.json'}",
    ]


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
    assert [(spec.name, spec.index, spec.total) for spec in specs] == [("adv-63", 1, 1)]


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

    with pytest.raises(FileExistsError, match="--overwrite"):
        _run(tmp_path)

    assert writes == []
    assert not (tmp_path / "execution").exists()


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

    assert [scope["profile_name"] for scope, _ in messages] == [
        "first",
        "first",
        "first",
    ]
    assert [message for _, message in messages] == [
        "Result: 1 records",
        f"Output: {tmp_path / 'first.jsonl'}",
        f"Metadata: {tmp_path / 'first.metadata.json'}",
    ]
