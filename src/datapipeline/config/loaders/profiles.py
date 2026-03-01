from pathlib import Path
from typing import Annotated

from pydantic import Field
from pydantic.type_adapter import TypeAdapter

from datapipeline.config.profiles import BuildProfile, InspectProfile, Profile, ServeProfile
from datapipeline.services.project_paths import profiles_dir

from .common import ensure_unique_specs, load_specs, spec_files

ProfileModel = Annotated[
    ServeProfile | BuildProfile | InspectProfile,
    Field(discriminator="type"),
]

PROFILE_ADAPTER = TypeAdapter(ProfileModel)
PROFILE_KIND_PREFIXES = {"serve", "build", "inspect"}


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
            if spec.type != expected_kind:
                raise ValueError(
                    f"{path} declares profile type '{spec.type}' but is located under "
                    f"profiles/{expected_kind}/."
                )
            specs.append(spec)
    return specs


def profile_specs(
    project_yaml: Path,
) -> tuple[list[ServeProfile], list[BuildProfile], list[InspectProfile]]:
    specs = _load_profile_specs(project_yaml)
    serve_specs: list[ServeProfile] = [spec for spec in specs if isinstance(spec, ServeProfile)]
    build_specs: list[BuildProfile] = [spec for spec in specs if isinstance(spec, BuildProfile)]
    inspect_specs: list[InspectProfile] = [
        spec for spec in specs if isinstance(spec, InspectProfile)
    ]

    for kind, kind_specs in (
        ("serve", serve_specs),
        ("build", build_specs),
        ("inspect", inspect_specs),
    ):
        ensure_unique_specs(
            kind_specs,
            error_template=f"Duplicate {kind} profile names are not allowed: {{details}}",
            key_fn=lambda spec: spec.name,
        )

    return serve_specs, build_specs, inspect_specs


__all__ = ["profile_specs"]
