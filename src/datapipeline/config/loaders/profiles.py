from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field
from pydantic.type_adapter import TypeAdapter

from datapipeline.config.profiles import (
    BuildProfile,
    BuildProfileDefaults,
    InspectProfile,
    InspectProfileDefaults,
    Profile,
    ProfileDefaults,
    ServeProfile,
    ServeProfileDefaults,
)
from datapipeline.services.project_paths import profiles_dir
from datapipeline.utils.load import load_yaml

from .common import ensure_unique_specs, load_specs, spec_files

ProfileModel = Annotated[
    ServeProfile | BuildProfile | InspectProfile,
    Field(discriminator="cmd"),
]

PROFILE_ADAPTER = TypeAdapter(ProfileModel)
PROFILE_KIND_PREFIXES = {"serve", "build", "inspect"}
ProfileCmd = Literal["serve", "build", "inspect"]
ProfileDefaultsModel = Annotated[
    ServeProfileDefaults | BuildProfileDefaults | InspectProfileDefaults,
    Field(discriminator="cmd"),
]
PROFILE_DEFAULTS_ADAPTER = TypeAdapter(ProfileDefaultsModel)


def _load_profile_entry(entry: dict) -> Profile:
    return PROFILE_ADAPTER.validate_python(entry)


def _profile_kind_from_filename(path: Path) -> str | None:
    stem = path.stem.strip().lower()
    if not stem or "." not in stem:
        return None
    prefix, _ = stem.split(".", 1)
    return prefix if prefix in PROFILE_KIND_PREFIXES else None


def _is_defaults_profile(path: Path) -> bool:
    stem = path.stem.strip().lower()
    if "." not in stem:
        return False
    _, suffix = stem.split(".", 1)
    return suffix == "defaults"


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
            "{serve,build,inspect}.<name|defaults>.yaml naming; "
            f"found nested profile files: {listed}"
        )

    invalid = sorted(
        path for path in spec_files(root) if _profile_kind_from_filename(path) is None
    )
    if not invalid:
        return
    listed = ", ".join(str(path.relative_to(root)) for path in invalid)
    raise ValueError(
        "Profile files must use {serve,build,inspect}.<name|defaults>.yaml naming under profiles/; "
        f"found invalid profile locations: {listed}"
    )


def _load_profile_specs(
    project_yaml: Path,
) -> tuple[list[Profile], dict[str, ProfileDefaults]]:
    root = profiles_dir(project_yaml)
    _validate_profile_layout(root)
    specs: list[Profile] = []
    defaults_by_kind: dict[str, ProfileDefaults] = {}
    for path in sorted(root.glob("*.y*ml")):
        expected_kind = _profile_kind_from_filename(path)
        if expected_kind is None:
            continue
        if _is_defaults_profile(path):
            doc = load_yaml(path)
            if not isinstance(doc, dict):
                raise TypeError(f"{path} must define a mapping profile defaults.")
            defaults = PROFILE_DEFAULTS_ADAPTER.validate_python(doc)
            if defaults.cmd != expected_kind:
                raise ValueError(
                    f"{path} declares profile cmd '{defaults.cmd}' but is located under "
                    f"profiles/{expected_kind}.defaults.yaml."
                )
            existing = defaults_by_kind.get(expected_kind)
            if existing is not None:
                first_path = getattr(existing, "source_path", None)
                raise ValueError(
                    f"Duplicate {expected_kind} defaults are not allowed: "
                    f"{first_path or '<generated>'}, {path}"
                )
            setattr(defaults, "source_path", path)
            defaults_by_kind[expected_kind] = defaults
            continue
        loaded = load_specs(path, _load_profile_entry)
        for spec in loaded:
            if spec.cmd != expected_kind:
                raise ValueError(
                    f"{path} declares profile cmd '{spec.cmd}' but is located under "
                    f"profiles/{expected_kind}.<name>.yaml."
                )
            specs.append(spec)
    return specs, defaults_by_kind


def profile_specs(
    project_yaml: Path,
    cmd: ProfileCmd | None = None,
) -> list[Profile]:
    if cmd is not None:
        profiles, _ = profile_specs_with_defaults(project_yaml, cmd=cmd)
        return profiles

    specs, _ = _load_profile_specs(project_yaml)
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


def profile_specs_with_defaults(
    project_yaml: Path,
    cmd: ProfileCmd,
) -> tuple[list[Profile], ProfileDefaults | None]:
    specs, defaults_by_kind = _load_profile_specs(project_yaml)
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
    return list(grouped[cmd]), defaults_by_kind.get(cmd)


def profile_defaults(
    project_yaml: Path,
    cmd: ProfileCmd,
) -> ProfileDefaults | None:
    _, defaults = profile_specs_with_defaults(project_yaml, cmd=cmd)
    return defaults


def apply_profile_defaults(
    profile: Profile,
    defaults: ProfileDefaults | None,
) -> Profile:
    if defaults is None:
        return profile
    if profile.cmd != defaults.cmd:
        raise ValueError(
            f"Cannot apply {defaults.cmd} defaults to {profile.cmd} profile '{profile.name}'."
        )

    defaults_payload = defaults.model_dump(
        exclude_unset=True,
        exclude_none=True,
        exclude={"source_path"},
    )
    profile_payload = profile.model_dump(
        exclude_unset=True,
        exclude={"source_path"},
    )
    merged_payload = _deep_merge_dicts(
        defaults_payload,
        profile_payload,
        replace_keys={"output"},
    )
    merged = PROFILE_ADAPTER.validate_python(merged_payload)
    setattr(merged, "source_path", getattr(profile, "source_path", None))
    return merged


def _deep_merge_dicts(
    base: dict,
    override: dict,
    *,
    replace_keys: set[str],
) -> dict:
    merged = dict(base)
    for key, value in override.items():
        existing = merged.get(key)
        if key in replace_keys:
            merged[key] = value
        elif isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dicts(
                existing,
                value,
                replace_keys=replace_keys,
            )
        else:
            merged[key] = value
    return merged


def _ordered_profiles(specs: list[Profile]) -> list[Profile]:
    ordered = [spec for spec in specs if spec.order is not None]
    unordered = [spec for spec in specs if spec.order is None]
    ordered.sort(key=lambda spec: (spec.order, spec.name))
    return ordered + unordered


__all__ = [
    "apply_profile_defaults",
    "profile_defaults",
    "profile_specs",
    "profile_specs_with_defaults",
]
