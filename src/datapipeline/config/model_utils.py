from typing import Sequence


def normalize_string_list(
    value,
    field_name: str,
    lower: bool = False,
) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, Sequence):
        raise ValueError(f"{field_name} must be a list")
    normalized: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item).strip()
        if lower:
            text = text.lower()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def normalize_required_text(
    value,
    field_name: str,
    lower: bool = False,
) -> str:
    text = str(value).strip() if value is not None else ""
    if lower:
        text = text.lower()
    if not text:
        raise ValueError(f"{field_name} must be set")
    return text


__all__ = ["normalize_string_list", "normalize_required_text"]
