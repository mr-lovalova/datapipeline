from pathlib import Path

from datapipeline.config.tasks import ServeOutputConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.io.serializers import json_line_serializer
from datapipeline.io.output import resolve_output_target


def test_resolve_output_target_uses_directory_and_run_name(tmp_path):
    default_dir = tmp_path / "outputs"
    default_dir.mkdir()
    cfg = ServeOutputConfig(transport="fs", format="jsonl", directory=default_dir)

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.transport == "fs"
    assert target.format == "jsonl"
    assert target.view == "raw"
    assert target.encoding == "utf-8"
    assert target.destination == (default_dir / "train" / "train.jsonl").resolve()


def test_resolve_output_target_honors_custom_filename(tmp_path):
    base_dir = tmp_path / "outputs"
    base_dir.mkdir()
    cfg = ServeOutputConfig(transport="fs", format="jsonl", directory=base_dir, filename="custom")

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.destination == (base_dir / "train" / "custom.jsonl").resolve()
    assert target.encoding == "utf-8"


def test_resolve_output_target_honors_view(tmp_path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    cfg = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        view="numeric",
        directory=out_dir,
    )

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.view == "numeric"


def test_resolve_output_target_honors_encoding(tmp_path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    cfg = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        encoding="utf-8-sig",
        directory=out_dir,
    )

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.encoding == "utf-8-sig"


def test_json_serializer_includes_sample_key_and_vectors():
    serializer = json_line_serializer()
    sample = Sample(
        key=("2024-01-01T00:00:00Z",),
        features=Vector(values={"a": 1.0}),
        targets=Vector(values={"t": 2.0}),
    )

    line = serializer(sample)
    assert '"kind": "Sample"' in line
    assert '"key": ["2024-01-01T00:00:00Z"]' in line
    assert '"fields": {"features.a": 1.0, "targets.t": 2.0}' in line


def test_stdout_print_defaults_to_raw_view():
    target = resolve_output_target(
        cli_output=ServeOutputConfig(transport="stdout", format="print"),
        config_output=None,
        default=None,
        base_path=Path("."),
        run_name="train",
    )
    assert target.view == "raw"


def test_stdout_jsonl_defaults_to_raw_view():
    target = resolve_output_target(
        cli_output=ServeOutputConfig(transport="stdout", format="jsonl"),
        config_output=None,
        default=None,
        base_path=Path("."),
        run_name="train",
    )
    assert target.view == "raw"
