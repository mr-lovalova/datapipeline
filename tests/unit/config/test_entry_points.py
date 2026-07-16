from importlib import import_module
import tomllib


def _load_entry_point(target: str):
    module_name, _, attr_name = target.partition(":")
    module = import_module(module_name)
    return getattr(module, attr_name)


def test_declared_entry_points_import() -> None:
    with open("pyproject.toml", "rb") as fh:
        pyproject = tomllib.load(fh)

    entry_points = pyproject["project"]["entry-points"]
    for entries in entry_points.values():
        for target in entries.values():
            assert _load_entry_point(target) is not None
