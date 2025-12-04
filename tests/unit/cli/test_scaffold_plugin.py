from pathlib import Path

import pytest

from datapipeline.services.scaffold.plugin import scaffold_plugin


def test_scaffold_plugin_normalizes_hyphenated_name(tmp_path: Path) -> None:
    scaffold_plugin("test-datapipeline", tmp_path)

    plugin_root = tmp_path / "test-datapipeline"
    assert (plugin_root / "src" / "test_datapipeline").is_dir()

    pyproject = (plugin_root / "pyproject.toml").read_text()
    assert 'name = "test-datapipeline"' in pyproject

    readme = (plugin_root / "README.md").read_text()
    assert "jerry plugin init test-datapipeline" in readme


@pytest.mark.parametrize("name", ["data pipeline", "datapipeline"])
def test_scaffold_plugin_rejects_disallowed_names(name: str, tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        scaffold_plugin(name, tmp_path)

    assert exc.value.code == 1
