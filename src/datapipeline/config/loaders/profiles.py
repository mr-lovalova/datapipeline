from pathlib import Path
from typing import Annotated

from pydantic import Field
from pydantic.type_adapter import TypeAdapter

from datapipeline.config.profiles import (
    BuildProfile,
    BuildProfileDefaults,
    InspectProfile,
    InspectProfileDefaults,
    MaterializeProfile,
    MaterializeProfileDefaults,
    Profile,
    ProfileCommand,
    ProfileDefaults,
    ServeProfile,
    ServeProfileDefaults,
)
from datapipeline.services.config_refs import (
    interpolate_config_vars,
    project_vars_from_data,
    resolve_config_refs,
)
from datapipeline.services.project_paths import profiles_dir
from datapipeline.utils.load import load_yaml

from .common import ensure_unique_specs, spec_files

ProfileModel = Annotated[
    ServeProfile | BuildProfile | InspectProfile | MaterializeProfile,
    Field(discriminator="cmd"),
]

PROFILE_KINDS: tuple[ProfileCommand, ...] = (
    "serve",
    "build",
    "inspect",
    "materialize",
)
PROFILE_ADAPTER: TypeAdapter[ProfileModel] = TypeAdapter(ProfileModel)
ProfileDefaultsModel = Annotated[
    ServeProfileDefaults
    | BuildProfileDefaults
    | InspectProfileDefaults
    | MaterializeProfileDefaults,
    Field(discriminator="cmd"),
]
PROFILE_DEFAULTS_ADAPTER: TypeAdapter[ProfileDefaultsModel] = TypeAdapter(
    ProfileDefaultsModel
)


def _project_vars(project_yaml: Path) -> dict:
    data = resolve_config_refs(load_yaml(project_yaml), project_yaml=project_yaml)
    return project_vars_from_data(data)


def _load_profile_doc(path: Path, project_yaml: Path, vars_: dict):
    doc = resolve_config_refs(
        load_yaml(path, require_mapping=False),
        project_yaml=project_yaml,
    )
    return interpolate_config_vars(doc, vars_)


def _profile_identity_from_filename(
    path: Path,
) -> tuple[ProfileCommand, str] | None:
    stem = path.stem.strip()
    if not stem or "." not in stem:
        return None
    prefix, name = stem.split(".", 1)
    command = prefix.lower()
    name = name.strip()
    if not name:
        return None
    for profile_command in PROFILE_KINDS:
        if command == profile_command:
            return profile_command, name
    return None


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
            "{serve,build,inspect,materialize}.<name|defaults>.yaml naming; "
            f"found nested profile files: {listed}"
        )

    invalid = sorted(
        path
        for path in spec_files(root)
        if _profile_identity_from_filename(path) is None
    )
    if not invalid:
        return
    listed = ", ".join(str(path.relative_to(root)) for path in invalid)
    raise ValueError(
        "Profile files must use {serve,build,inspect,materialize}.<name|defaults>.yaml "
        "naming under profiles/; "
        f"found invalid profile locations: {listed}"
    )


def _load_profile_specs(
    project_yaml: Path,
    command: ProfileCommand | None = None,
) -> tuple[list[Profile], dict[str, ProfileDefaults]]:
    root = profiles_dir(project_yaml)
    _validate_profile_layout(root)
    vars_ = _project_vars(project_yaml)
    specs: list[Profile] = []
    defaults_by_kind: dict[str, ProfileDefaults] = {}
    for path in sorted(root.glob("*.y*ml")):
        identity = _profile_identity_from_filename(path)
        if identity is None:
            continue
        expected_kind, profile_name = identity
        if command is not None and expected_kind != command:
            continue
        if profile_name.lower() == "defaults":
            doc = _load_profile_doc(path, project_yaml, vars_)
            if not isinstance(doc, dict):
                raise TypeError(f"{path} must define a mapping profile defaults.")
            if "cmd" in doc:
                raise ValueError(
                    "Profile command comes from the defaults filename; "
                    "remove the 'cmd' key."
                )
            defaults = PROFILE_DEFAULTS_ADAPTER.validate_python(
                {"cmd": expected_kind, **doc}
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
        doc = _load_profile_doc(path, project_yaml, vars_)
        if not isinstance(doc, dict):
            raise TypeError(f"{path} must define one mapping profile.")
        if "cmd" in doc or "name" in doc:
            raise ValueError(
                "Profile command and name come from the filename; "
                "remove the 'cmd' and 'name' keys."
            )
        spec = PROFILE_ADAPTER.validate_python(
            {"cmd": expected_kind, "name": profile_name, **doc}
        )
        setattr(spec, "source_path", path)
        specs.append(spec)
    return specs, defaults_by_kind


def profile_specs(
    project_yaml: Path,
    cmd: ProfileCommand | None = None,
) -> list[Profile]:
    if cmd is not None:
        profiles, _ = profile_specs_with_defaults(project_yaml, cmd=cmd)
        return profiles

    specs, _ = _load_profile_specs(project_yaml)
    grouped = _group_profiles(specs)
    return [profile for kind in PROFILE_KINDS for profile in grouped[kind]]


def profile_specs_with_defaults(
    project_yaml: Path,
    cmd: ProfileCommand,
) -> tuple[list[Profile], ProfileDefaults]:
    specs, defaults_by_kind = _load_profile_specs(project_yaml, command=cmd)
    grouped = _group_profiles(specs)
    defaults = defaults_by_kind.get(cmd)
    if defaults is None:
        defaults = PROFILE_DEFAULTS_ADAPTER.validate_python({"cmd": cmd})
    return list(grouped[cmd]), defaults


def apply_profile_defaults(
    profile: Profile,
    defaults: ProfileDefaults,
) -> Profile:
    if profile.cmd != defaults.cmd:
        raise ValueError(
            f"Cannot apply {defaults.cmd} defaults to {profile.cmd} profile '{profile.name}'."
        )

    non_profile_fields = {"execution", "source_path"}
    if isinstance(defaults, MaterializeProfileDefaults):
        non_profile_fields.add("artifact_mode")
    defaults_payload = defaults.model_dump(
        exclude_unset=True,
        exclude_none=True,
        exclude=non_profile_fields,
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


def _group_profiles(specs: list[Profile]) -> dict[str, list[Profile]]:
    grouped: dict[str, list[Profile]] = {kind: [] for kind in PROFILE_KINDS}
    for spec in specs:
        grouped[spec.cmd].append(spec)
    for kind, kind_specs in grouped.items():
        grouped[kind] = _ordered_profiles(kind_specs)
        ensure_unique_specs(
            grouped[kind],
            error_template=f"Duplicate {kind} profile names are not allowed: {{details}}",
            key_fn=lambda spec: spec.name,
        )
    return grouped


__all__ = [
    "apply_profile_defaults",
    "profile_specs",
    "profile_specs_with_defaults",
]
