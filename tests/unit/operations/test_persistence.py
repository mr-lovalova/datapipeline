import logging

import pytest

from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import RuntimeOutput, persist_runtime_result


def test_failed_runtime_write_preserves_existing_file(tmp_path) -> None:
    destination = tmp_path / "out.jsonl"
    destination.write_text("previous\n", encoding="utf-8")

    def rows():
        yield {"value": 1}
        raise RuntimeError("boom")

    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=destination,
    )

    with pytest.raises(RuntimeError, match="boom"):
        persist_runtime_result(
            RuntimeOutput(rows=rows()),
            target=target,
            visuals="off",
            logger=logging.getLogger(__name__),
            emit_message=lambda *args, **kwargs: None,
        )

    assert destination.read_text(encoding="utf-8") == "previous\n"
