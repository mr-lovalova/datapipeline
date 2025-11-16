from pathlib import Path

from datapipeline.config.run import OutputConfig
from datapipeline.io.output import resolve_output_target


def test_resolve_output_target_uses_directory_and_run_name(tmp_path):
    default_dir = tmp_path / "outputs"
    default_dir.mkdir()
    cfg = OutputConfig(transport="fs", format="json-lines", directory=default_dir)

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.transport == "fs"
    assert target.format == "json-lines"
    assert target.destination == (default_dir / "train" / "train.jsonl").resolve()


def test_resolve_output_target_honors_custom_filename(tmp_path):
    base_dir = tmp_path / "outputs"
    base_dir.mkdir()
    cfg = OutputConfig(transport="fs", format="json-lines", directory=base_dir, filename="custom")

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.destination == (base_dir / "train" / "custom.jsonl").resolve()
