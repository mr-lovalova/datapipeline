from typing import Iterable, Protocol, TypeVar


class NamedProfile(Protocol):
    name: str
    enabled: bool


P = TypeVar("P", bound=NamedProfile)


def select_profiles(
    profiles: Iterable[P],
    run_name: str | None,
    kind: str,
) -> list[P]:
    selected = list(profiles)
    if run_name is not None:
        normalized_name = run_name.strip()
        if not normalized_name:
            raise ValueError(f"{kind.capitalize()} profile name must not be empty.")
        selected = [profile for profile in selected if profile.name == normalized_name]
        if not selected:
            raise ValueError(f"Unknown {kind} profile '{normalized_name}'")
        return selected
    return [profile for profile in selected if profile.enabled]
