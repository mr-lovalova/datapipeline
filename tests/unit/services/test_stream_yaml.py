from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from datapipeline.config.streams import AlignedStreamConfig, SourceStreamConfig
from datapipeline.services.scaffold.paths import ensure_project_scaffold
from datapipeline.services.scaffold.stream_yaml import (
    write_aligned_stream,
    write_source_stream,
)


@pytest.mark.parametrize(
    "stream_id",
    ["../outside", "/absolute", "nested/name", "a..b"],
)
def test_source_stream_rejects_unsafe_id_before_mutating_project(
    tmp_path: Path,
    stream_id: str,
) -> None:
    project_yaml = tmp_path / "project.yaml"

    with pytest.raises(ValidationError):
        write_source_stream(
            project_yaml,
            stream_id,
            "provider.prices",
            "identity",
        )

    assert not project_yaml.exists()
    assert not (tmp_path / "streams").exists()


def test_aligned_stream_rejects_invalid_inputs_before_mutating_project(
    tmp_path: Path,
) -> None:
    project_yaml = tmp_path / "project.yaml"

    with pytest.raises(ValidationError):
        write_aligned_stream(
            project_yaml,
            "prices.spread",
            ["prices.bid", "prices.bid"],
            "combine_spread",
        )

    assert not project_yaml.exists()
    assert not (tmp_path / "streams").exists()


def test_source_stream_refuses_to_replace_existing_config(tmp_path: Path) -> None:
    project_yaml = tmp_path / "project.yaml"
    ensure_project_scaffold(project_yaml)
    path = tmp_path / "streams" / "prices.daily.yaml"
    path.write_text("existing\n", encoding="utf-8")

    with pytest.raises(FileExistsError):
        write_source_stream(
            project_yaml,
            "prices.daily",
            "provider.prices",
            "identity",
        )

    assert path.read_text(encoding="utf-8") == "existing\n"


def test_aligned_stream_refuses_to_replace_existing_config(tmp_path: Path) -> None:
    project_yaml = tmp_path / "project.yaml"
    ensure_project_scaffold(project_yaml)
    path = tmp_path / "streams" / "prices.spread.yaml"
    path.write_text("existing\n", encoding="utf-8")

    with pytest.raises(FileExistsError):
        write_aligned_stream(
            project_yaml,
            "prices.spread",
            ["prices.bid", "prices.ask"],
            "combine_spread",
        )

    assert path.read_text(encoding="utf-8") == "existing\n"


def test_stream_scaffolds_render_valid_configs(tmp_path: Path) -> None:
    project_yaml = tmp_path / "project.yaml"

    source_path = write_source_stream(
        project_yaml,
        "prices.daily-us",
        "provider.prices",
        "identity",
    )
    aligned_path = write_aligned_stream(
        project_yaml,
        "prices.spread",
        ["prices.bid", "prices.ask"],
        "combine_spread",
    )

    source = SourceStreamConfig.model_validate(
        yaml.safe_load(source_path.read_text(encoding="utf-8"))
    )
    aligned = AlignedStreamConfig.model_validate(
        yaml.safe_load(aligned_path.read_text(encoding="utf-8"))
    )
    assert source.id == "prices.daily-us"
    assert aligned.from_.align == ("prices.bid", "prices.ask")
