from math import isfinite
from urllib.parse import quote, unquote


VARIABLE_ID_SEPARATOR = "__"
VARIABLE_ID_COMPONENT_SEPARATOR = "|"


def base_id(variable_id: str) -> str:
    base, separator, suffix = variable_id.partition(VARIABLE_ID_SEPARATOR)
    if not separator:
        return variable_id
    if not base or not suffix:
        raise ValueError(f"Invalid partitioned variable id {variable_id!r}")
    return base


def partition_suffix(variable_id: str) -> str:
    _, separator, suffix = variable_id.partition(VARIABLE_ID_SEPARATOR)
    if not separator:
        return ""
    if not suffix:
        raise ValueError(f"Invalid partitioned variable id {variable_id!r}")
    return suffix


def make_partitioned_variable_id(base: str, suffix: str) -> str:
    if VARIABLE_ID_SEPARATOR in base:
        raise ValueError(
            "Variable base id must not contain reserved separator "
            f"{VARIABLE_ID_SEPARATOR!r}"
        )
    return f"{base}{VARIABLE_ID_SEPARATOR}{suffix}" if suffix else base


def encode_variable_id_component(field: str, value: object) -> str:
    if not field:
        raise ValueError("variable identity fields must not be empty")
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
                f"Variable identity field {field!r} must contain a finite float."
            )
        encoded_value = f"!f:{value.hex()}"
    else:
        raise TypeError(
            f"Variable identity field {field!r} must contain a string, integer, "
            f"float, boolean, or null; got {type(value).__name__}."
        )
    return f"@{encoded_field}:{encoded_value}"


def variable_id_components(variable_id: str) -> tuple[tuple[str, object], ...]:
    suffix = partition_suffix(variable_id)
    if not suffix:
        return ()

    components: list[tuple[str, object]] = []
    for encoded_component in suffix.split(VARIABLE_ID_COMPONENT_SEPARATOR):
        if not encoded_component.startswith("@"):
            raise ValueError(
                f"Invalid variable identity component {encoded_component!r}"
            )
        encoded_field, separator, encoded_value = encoded_component[1:].partition(":")
        if not separator or not encoded_field:
            raise ValueError(
                f"Invalid variable identity component {encoded_component!r}"
            )

        field = unquote(encoded_field)
        if encoded_value == "!n":
            value: object = None
        elif encoded_value.startswith("!b:"):
            payload = encoded_value[3:]
            if payload not in {"0", "1"}:
                raise ValueError(
                    f"Invalid boolean variable identity value {encoded_value!r}"
                )
            value = payload == "1"
        elif encoded_value.startswith("!i:"):
            value = int(encoded_value[3:])
        elif encoded_value.startswith("!f:"):
            value = float.fromhex(encoded_value[3:])
        elif encoded_value.startswith("!"):
            raise ValueError(f"Invalid variable identity value {encoded_value!r}")
        else:
            value = unquote(encoded_value)
        components.append((field, value))
    return tuple(components)
