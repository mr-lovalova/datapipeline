from pathlib import Path
from typing import Callable, Sequence, TypeVar

from datapipeline.utils.load import load_yaml

T = TypeVar("T")


def spec_files(root: Path) -> Sequence[Path]:
    if not root.exists():
        return []
    if root.is_file():
        return [root]
    return sorted(path for path in root.rglob("*.y*ml") if path.is_file())


def load_specs(path: Path, loader: Callable[[dict], T]) -> list[T]:
    doc = load_yaml(path)
    entries = doc if isinstance(doc, list) else [doc]
    specs: list[T] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise TypeError(f"{path} must define mapping tasks.")
        spec = loader(entry)
        setattr(spec, "source_path", path)
        specs.append(spec)
    return specs


def ensure_unique_specs(
    specs: Sequence[T],
    error_template: str,
    key_fn,
) -> None:
    by_key: dict[str, list[T]] = {}
    for spec in specs:
        by_key.setdefault(str(key_fn(spec)), []).append(spec)
    duplicates = {key: items for key, items in by_key.items() if len(items) > 1}
    if not duplicates:
        return
    lines: list[str] = []
    for key, items in sorted(duplicates.items()):
        paths = ", ".join(
            str(path) if path is not None else "<generated>"
            for path in (getattr(item, "source_path", None) for item in items)
        )
        lines.append(f"{key} ({paths})")
    raise ValueError(error_template.format(details="; ".join(lines)))


__all__ = ["spec_files", "load_specs", "ensure_unique_specs"]
