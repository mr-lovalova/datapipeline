from math import isfinite
from urllib.parse import quote, unquote


FEATURE_ID_SEPARATOR = "__"
FEATURE_ID_COMPONENT_SEP = "|"


def base_id(feature_id: str) -> str:
    base, separator, suffix = feature_id.partition(FEATURE_ID_SEPARATOR)
    if not separator:
        return feature_id
    if not base or not suffix:
        raise ValueError(f"Invalid partitioned feature id {feature_id!r}")
    return base


def partition_suffix(feature_id: str) -> str:
    _, separator, suffix = feature_id.partition(FEATURE_ID_SEPARATOR)
    if not separator:
        return ""
    if not suffix:
        raise ValueError(f"Invalid partitioned feature id {feature_id!r}")
    return suffix


def is_partitioned(feature_id: str) -> bool:
    return FEATURE_ID_SEPARATOR in feature_id


def make_partition_id(base: str, suffix: str) -> str:
    if FEATURE_ID_SEPARATOR in base:
        raise ValueError(
            "Feature base id must not contain reserved separator "
            f"{FEATURE_ID_SEPARATOR!r}"
        )
    return f"{base}{FEATURE_ID_SEPARATOR}{suffix}" if suffix else base


def encode_feature_id_component(field: str, value: object) -> str:
    if not field:
        raise ValueError("feature_id_by fields must not be empty")
    encoded_field = quote(field, safe="")
    if value is None:
        encoded_value = "!n"
    elif type(value) is str:
        encoded_value = quote(value, safe="")
    elif type(value) is bool:
        encoded_value = f"!b:{int(value)}"
    elif type(value) is int:
        encoded_value = f"!i:{value}"
    elif type(value) is float:
        if not isfinite(value):
            raise ValueError(
                f"Feature identity field {field!r} must contain a finite float."
            )
        encoded_value = f"!f:{value.hex()}"
    else:
        raise TypeError(
            f"Feature identity field {field!r} must contain a string, integer, "
            f"float, boolean, or null; got {type(value).__name__}."
        )
    return f"@{encoded_field}:{encoded_value}"


def feature_id_components(feature_id: str) -> tuple[tuple[str, object], ...]:
    suffix = partition_suffix(feature_id)
    if not suffix:
        return ()

    components: list[tuple[str, object]] = []
    for encoded_component in suffix.split(FEATURE_ID_COMPONENT_SEP):
        if not encoded_component.startswith("@"):
            raise ValueError(
                f"Invalid feature identity component {encoded_component!r}"
            )
        encoded_field, separator, encoded_value = encoded_component[1:].partition(":")
        if not separator or not encoded_field:
            raise ValueError(
                f"Invalid feature identity component {encoded_component!r}"
            )

        field = unquote(encoded_field)
        if encoded_value == "!n":
            value: object = None
        elif encoded_value.startswith("!b:"):
            payload = encoded_value[3:]
            if payload not in {"0", "1"}:
                raise ValueError(
                    f"Invalid boolean feature identity value {encoded_value!r}"
                )
            value = payload == "1"
        elif encoded_value.startswith("!i:"):
            value = int(encoded_value[3:])
        elif encoded_value.startswith("!f:"):
            value = float.fromhex(encoded_value[3:])
        elif encoded_value.startswith("!"):
            raise ValueError(f"Invalid feature identity value {encoded_value!r}")
        else:
            value = unquote(encoded_value)
        components.append((field, value))
    return tuple(components)
