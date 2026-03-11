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
    if run_name:
        selected = [profile for profile in selected if profile.name == run_name]
        if not selected:
            raise ValueError(f"Unknown {kind} profile '{run_name}'")
        return selected
    return [profile for profile in selected if profile.enabled]


__all__ = ["select_profiles"]
