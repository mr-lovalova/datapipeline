from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple


def sections_from_path(root: Optional[Path], target: Optional[Path]) -> Tuple[str, ...]:
    if root is None:
        return tuple()
    root_name = (root.name or root.as_posix())
    sections: list[str] = [root_name]
    if target is not None:
        try:
            target_rel = target.relative_to(root)
        except Exception:
            target_rel = target
        for part in target_rel.parts:
            if part and part not in {".", ""}:
                sections.append(part)
    return tuple(sections)
