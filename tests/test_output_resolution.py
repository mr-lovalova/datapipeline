from pathlib import Path

from datapipeline.config.tasks import ServeOutputConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.io.serializers import json_line_serializer
from datapipeline.io.output import resolve_output_target


def test_resolve_output_target_uses_directory_and_run_name(tmp_path):
    default_dir = tmp_path / "outputs"
    default_dir.mkdir()
    cfg = ServeOutputConfig(transport="fs", format="json-lines", directory=default_dir)

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
    assert target.payload == "sample"


def test_resolve_output_target_honors_custom_filename(tmp_path):
    base_dir = tmp_path / "outputs"
    base_dir.mkdir()
    cfg = ServeOutputConfig(transport="fs", format="json-lines", directory=base_dir, filename="custom")

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.destination == (base_dir / "train" / "custom.jsonl").resolve()


def test_resolve_output_target_preserves_payload_setting(tmp_path):
    base_dir = tmp_path / "vectors_out"
    base_dir.mkdir()
    cfg = ServeOutputConfig(
        transport="fs", format="json-lines", directory=base_dir, payload="vector"
    )

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="eval",
    )

    assert target.payload == "vector"


def test_resolve_output_target_respects_payload_override(tmp_path):
    cfg = ServeOutputConfig(transport="stdout", format="print", payload="sample")

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=tmp_path,
        run_name=None,
        payload_override="vector",
    )

    assert target.payload == "vector"


def test_json_serializer_vector_payload_excludes_key():
    serializer = json_line_serializer("vector")
    sample = Sample(
        key=("2024-01-01T00:00:00Z",),
        features=Vector(values={"a": 1.0}),
        targets=Vector(values={"t": 2.0}),
    )

    line = serializer(sample)
    assert '"features": [1.0]' in line
    assert '"targets": [2.0]' in line
    assert "2024" not in line  # key omitted
