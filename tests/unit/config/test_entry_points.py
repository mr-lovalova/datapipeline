import tomllib
from importlib import import_module


def _load_entry_point(target: str):
    module_name, _, attr_name = target.partition(":")
    module = import_module(module_name)
    return getattr(module, attr_name)


def test_stream_transform_entry_points_import() -> None:
    with open("pyproject.toml", "rb") as fh:
        pyproject = tomllib.load(fh)

    stream_entries = pyproject["project"]["entry-points"][
        "datapipeline.transforms.stream"
    ]

    assert "lint" not in stream_entries
    for target in stream_entries.values():
        assert _load_entry_point(target) is not None
