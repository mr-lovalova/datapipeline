from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field
from pydantic.type_adapter import TypeAdapter

from datapipeline.config.profiles import BuildProfile, InspectProfile, Profile, ServeProfile
from datapipeline.services.project_paths import profiles_dir

from .common import ensure_unique_specs, load_specs, spec_files

ProfileModel = Annotated[
    ServeProfile | BuildProfile | InspectProfile,
    Field(discriminator="cmd"),
]

PROFILE_ADAPTER = TypeAdapter(ProfileModel)
PROFILE_KIND_PREFIXES = {"serve", "build", "inspect"}
ProfileCmd = Literal["serve", "build", "inspect"]


def _load_profile_entry(entry: dict) -> Profile:
    return PROFILE_ADAPTER.validate_python(entry)


def _profile_kind_from_filename(path: Path) -> str | None:
    stem = path.stem.strip().lower()
    if not stem or "." not in stem:
        return None
    prefix, _ = stem.split(".", 1)
    return prefix if prefix in PROFILE_KIND_PREFIXES else None


def _validate_profile_layout(root: Path) -> None:
    if not root.exists() or root.is_file():
        return
    nested_files = sorted(
        path for path in root.rglob("*.y*ml") if path.is_file() and path.parent != root
    )
    if nested_files:
        listed = ", ".join(str(path.relative_to(root)) for path in nested_files)
        raise ValueError(
            "Profile files must be flat under profiles/ using "
            "{serve,build,inspect}.<name>.yaml naming; "
            f"found nested profile files: {listed}"
        )

    invalid = sorted(
        path for path in spec_files(root) if _profile_kind_from_filename(path) is None
    )
    if not invalid:
        return
    listed = ", ".join(str(path.relative_to(root)) for path in invalid)
    raise ValueError(
        "Profile files must use {serve,build,inspect}.<name>.yaml naming under profiles/; "
        f"found invalid profile locations: {listed}"
    )


def _load_profile_specs(project_yaml: Path) -> list[Profile]:
    root = profiles_dir(project_yaml)
    _validate_profile_layout(root)
    specs: list[Profile] = []
    for path in sorted(root.glob("*.y*ml")):
        expected_kind = _profile_kind_from_filename(path)
        if expected_kind is None:
            continue
        loaded = load_specs(path, _load_profile_entry)
        for spec in loaded:
            if spec.cmd != expected_kind:
                raise ValueError(
                    f"{path} declares profile cmd '{spec.cmd}' but is located under "
                    f"profiles/{expected_kind}.<name>.yaml."
                )
            specs.append(spec)
    return specs


def profile_specs(
    project_yaml: Path,
    cmd: ProfileCmd | None = None,
) -> list[Profile]:
    specs = _load_profile_specs(project_yaml)
    grouped: dict[str, list[Profile]] = {"serve": [], "build": [], "inspect": []}
    for spec in specs:
        grouped[spec.cmd].append(spec)

    for kind, kind_specs in grouped.items():
        grouped[kind] = _ordered_profiles(kind_specs)
        ensure_unique_specs(
            grouped[kind],
            error_template=f"Duplicate {kind} profile names are not allowed: {{details}}",
            key_fn=lambda spec: spec.name,
        )

    if cmd is not None:
        return list(grouped[cmd])
    return grouped["serve"] + grouped["build"] + grouped["inspect"]


def _ordered_profiles(specs: list[Profile]) -> list[Profile]:
    ordered = [spec for spec in specs if spec.order is not None]
    unordered = [spec for spec in specs if spec.order is None]
    ordered.sort(key=lambda spec: (spec.order, spec.name))
    return ordered + unordered


__all__ = ["profile_specs"]
