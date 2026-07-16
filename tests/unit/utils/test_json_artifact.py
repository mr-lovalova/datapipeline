from pathlib import Path

import pytest

import datapipeline.utils.json_artifact as json_artifact
from datapipeline.utils.json_artifact import write_json_artifact


def test_json_artifact_serialization_failure_preserves_previous_file(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "artifact.json"
    destination.write_text('{"previous": true}\n', encoding="utf-8")

    with pytest.raises(TypeError):
        write_json_artifact(destination, {"value": object()})

    assert destination.read_text(encoding="utf-8") == '{"previous": true}\n'
    assert list(tmp_path.iterdir()) == [destination]


def test_json_artifact_interrupt_preserves_previous_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "artifact.json"
    destination.write_text('{"previous": true}\n', encoding="utf-8")

    def interrupt_dump(payload, handle, indent, sort_keys):
        handle.write('{"partial":')
        raise KeyboardInterrupt

    monkeypatch.setattr(json_artifact.json, "dump", interrupt_dump)

    with pytest.raises(KeyboardInterrupt):
        write_json_artifact(destination, {"value": 1})

    assert destination.read_text(encoding="utf-8") == '{"previous": true}\n'
    assert list(tmp_path.iterdir()) == [destination]
