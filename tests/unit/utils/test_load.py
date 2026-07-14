from pathlib import Path

import pytest

from datapipeline.utils import load


@pytest.mark.parametrize(
    "yaml_text",
    [
        "value: first\nvalue: second\n",
        "outer:\n  value: first\n  value: second\n",
    ],
)
def test_load_yaml_rejects_duplicate_mapping_keys(
    tmp_path: Path,
    yaml_text: str,
) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(yaml_text, encoding="utf-8")

    with pytest.raises(ValueError, match="found duplicate key 'value'"):
        load.load_yaml(path)


def test_load_yaml_allows_explicit_override_of_merged_defaults(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "defaults: &defaults\n  value: default\nconfig:\n  <<: *defaults\n  value: explicit\n",
        encoding="utf-8",
    )

    assert load.load_yaml(path)["config"] == {"value": "explicit"}


def test_load_yaml_rejects_repeated_merge_keys(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "first: &first {a: 1}\nsecond: &second {b: 2}\n"
        "config:\n  <<: *first\n  <<: *second\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="found duplicate key '<<'"):
        load.load_yaml(path)


def test_load_ep_rejects_noncallable_target(monkeypatch: pytest.MonkeyPatch) -> None:
    entrypoints = load.md.EntryPoints(
        [
            load.md.EntryPoint(
                name="invalid",
                value="builtins:Ellipsis",
                group="test.group",
            )
        ]
    )
    monkeypatch.setattr(load.md, "entry_points", lambda: entrypoints)
    load.load_ep.cache_clear()

    with pytest.raises(TypeError, match="Entry point 'invalid'.*must be callable"):
        load.load_ep("test.group", "invalid")

    load.load_ep.cache_clear()
