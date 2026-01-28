from pathlib import Path
import sys
import logging

from datapipeline.services.scaffold.layout import to_snake, slugify, ep_key_from_name

_LOGGER = logging.getLogger("datapipeline.cli")


def ensure_pkg_dir(base: Path, name: str) -> Path:
    path = base / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "__init__.py").touch(exist_ok=True)
    return path


__all__ = [
    "ensure_pkg_dir",
    "to_snake",
    "slugify",
    "ep_key_from_name",
    "validate_identifier",
    "write_if_missing",
    "prompt_required",
    "prompt_optional",
    "choose_name",
    "status",
    "info",
    "error_exit",
    "pick_from_list",
    "pick_from_menu",
    "choose_existing_or_create",
]


def validate_identifier(name: str, label: str) -> None:
    if not name or not name.isidentifier():
        error_exit(f"{label} must be a valid Python identifier")


def write_if_missing(path: Path, text: str, *, label: str | None = None) -> bool:
    if path.exists():
        status("skip", f"{label or 'File'} already exists: {path}")
        return False
    path.write_text(text)
    status("new", str(path))
    return True


def prompt_required(prompt: str) -> str:
    value = input(f"{prompt}: ").strip()
    if not value:
        error_exit(f"{prompt} is required")
    return value


def prompt_optional(prompt: str) -> str | None:
    value = input(f"{prompt}: ").strip()
    return value or None


def choose_name(label: str, *, default: str | None = None) -> str:
    if not default:
        return prompt_required(label)
    info(f"{label}:")
    info(f"  [1] {default} (default)")
    info("  [2] Custom name")
    while True:
        sel = input("> ").strip()
        if sel == "":
            return default
        if sel == "1":
            return default
        if sel == "2":
            return prompt_required(label)
        info("Please enter a number from the list.")


def info(message: str) -> None:
    _LOGGER.info(message)


def status(tag: str, message: str) -> None:
    _LOGGER.info("[%s] %s", tag, message)


def error_exit(message: str, code: int = 2) -> None:
    _LOGGER.error(message)
    raise SystemExit(code)


def pick_from_list(prompt: str, options: list[str]) -> str:
    info(prompt)
    for i, opt in enumerate(options, 1):
        info(f"  [{i}] {opt}")
    while True:
        sel = input("> ").strip()
        if sel.isdigit():
            idx = int(sel)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        info("Please enter a number from the list.")


def pick_from_menu(prompt: str, options: list[tuple[str, str]], *, allow_default: bool = True) -> str:
    info(prompt)
    for i, (_, label) in enumerate(options, 1):
        info(f"  [{i}] {label}")
    while True:
        sel = input("> ").strip()
        if sel == "" and allow_default:
            return options[0][0]
        if sel.isdigit():
            idx = int(sel)
            if 1 <= idx <= len(options):
                return options[idx - 1][0]
        info("Please enter a number from the list.")


def pick_multiple_from_list(prompt: str, options: list[str]) -> list[str]:
    info(prompt)
    for i, opt in enumerate(options, 1):
        info(f"  [{i}] {opt}")
    sel = input("> ").strip()
    try:
        idxs = [int(x) for x in sel.split(",") if x.strip()]
    except ValueError:
        error_exit("Invalid selection.")
    picked = [options[i - 1] for i in idxs if 1 <= i <= len(options)]
    if not picked:
        error_exit("No inputs selected.")
    return picked


def choose_existing_or_create(
    *,
    label: str,
    existing: list[str],
    create_label: str,
    create_fn,
    prompt_new: str,
    root: Path | None,
    default_new: str | None = None,
) -> str:
    info(f"{label}:")
    info(f"  [1] {create_label} (default)")
    info(f"  [2] Select existing {label}")
    while True:
        sel = input("> ").strip()
        if sel == "":
            sel = "1"
        if sel == "1":
            name = choose_name(prompt_new, default=default_new)
            create_fn(name=name, root=root)
            return name
        if sel == "2":
            if not existing:
                error_exit(f"No existing {label} found.")
            return pick_from_list(f"Select {label}:", existing)
        info("Please enter a number from the list.")


def choose_existing_or_create_name(
    *,
    label: str,
    existing: list[str],
    create_label: str,
    prompt_new: str,
    default_new: str | None = None,
) -> tuple[str, bool]:
    """Return (name, created) without side effects."""
    info(f"{label}:")
    info(f"  [1] {create_label} (default)")
    info(f"  [2] Select existing {label}")
    while True:
        sel = input("> ").strip()
        if sel == "":
            sel = "1"
        if sel == "1":
            name = choose_name(prompt_new, default=default_new)
            return name, True
        if sel == "2":
            if not existing:
                error_exit(f"No existing {label} found.")
            name = pick_from_list(f"Select {label}:", existing)
            return name, False
        info("Please enter a number from the list.")
