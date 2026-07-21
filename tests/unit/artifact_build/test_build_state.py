from pathlib import Path

import pytest

from datapipeline.build.state import BuildState, save_build_state


def test_build_state_rejects_symlink_escape(tmp_path: Path) -> None:
    artifacts_root = tmp_path / "artifacts"
    build_dir = artifacts_root / "_system/build"
    build_dir.mkdir(parents=True)
    outside = tmp_path / "outside-state.json"
    outside.write_text("keep", encoding="utf-8")
    (build_dir / "state.json").symlink_to(outside)

    with pytest.raises(ValueError, match="must stay under artifacts root"):
        save_build_state(BuildState(), artifacts_root)

    assert outside.read_text(encoding="utf-8") == "keep"
