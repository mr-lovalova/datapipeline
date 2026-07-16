import keyword
from pathlib import Path


def ensure_pkg_dir(base: Path, name: str) -> Path:
    path = base / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "__init__.py").touch(exist_ok=True)
    return path


def is_python_identifier(name: str) -> bool:
    return bool(name and name.isidentifier() and not keyword.iskeyword(name))


def write_new_file(path: Path, text: str) -> None:
    with path.open("x", encoding="utf-8") as file:
        file.write(text)
