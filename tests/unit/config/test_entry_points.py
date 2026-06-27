import tomllib
from importlib import import_module


def _load_entry_point(target: str):
    module_name, _, attr_name = target.partition(":")
    module = import_module(module_name)
    return getattr(module, attr_name)


def test_transform_entry_points_import() -> None:
    with open("pyproject.toml", "rb") as fh:
        pyproject = tomllib.load(fh)

    transform_entries = pyproject["project"]["entry-points"]
    for group in (
        "datapipeline.transforms.record",
        "datapipeline.transforms.stream",
        "datapipeline.transforms.vector",
    ):
        entries = transform_entries[group]
        assert "lint" not in entries
        for target in entries.values():
            assert _load_entry_point(target) is not None
