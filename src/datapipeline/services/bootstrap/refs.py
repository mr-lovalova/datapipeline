import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Protocol


_CONFIG_REF_RE = re.compile(r"\$\{([A-Za-z_][\w-]*):([^}]+)\}")


class ConfigRefProvider(Protocol):
    def resolve(self, key: str, *, context: "ConfigRefContext") -> Any: ...


@dataclass(frozen=True, slots=True)
class ConfigRefContext:
    project_yaml: Path
    env: Mapping[str, str]


class ConfigRefError(ValueError):
    """Raised when an external config reference cannot be resolved."""


class EnvConfigRefProvider:
    def resolve(self, key: str, *, context: ConfigRefContext) -> str:
        name = key.strip()
        if not name:
            raise ConfigRefError("Config reference '${env:...}' must include a variable name.")
        value = context.env.get(name)
        if value is None:
            raise ConfigRefError(
                f"Missing environment variable '{name}' referenced from "
                f"{context.project_yaml.parent / '.env'} or process environment."
            )
        return value


def resolve_config_refs(
    obj: Any,
    *,
    project_yaml: Path,
    providers: Mapping[str, ConfigRefProvider] | None = None,
) -> Any:
    resolved_project_yaml = project_yaml.resolve()
    context = ConfigRefContext(
        project_yaml=resolved_project_yaml,
        env=_merged_env(resolved_project_yaml),
    )
    registry = dict(providers or default_config_ref_providers())
    return _resolve_config_refs(obj, context=context, providers=registry)


def default_config_ref_providers() -> Mapping[str, ConfigRefProvider]:
    return {"env": EnvConfigRefProvider()}


def _resolve_config_refs(
    obj: Any,
    *,
    context: ConfigRefContext,
    providers: Mapping[str, ConfigRefProvider],
) -> Any:
    if isinstance(obj, dict):
        return {
            key: _resolve_config_refs(value, context=context, providers=providers)
            for key, value in obj.items()
        }
    if isinstance(obj, list):
        return [
            _resolve_config_refs(value, context=context, providers=providers)
            for value in obj
        ]
    if isinstance(obj, str):
        return _resolve_string_refs(obj, context=context, providers=providers)
    return obj


def _resolve_string_refs(
    text: str,
    *,
    context: ConfigRefContext,
    providers: Mapping[str, ConfigRefProvider],
) -> Any:
    match = _CONFIG_REF_RE.fullmatch(text)
    if match:
        return _resolve_match(match, context=context, providers=providers)

    def repl(match: re.Match[str]) -> str:
        value = _resolve_match(match, context=context, providers=providers)
        return str(value)

    return _CONFIG_REF_RE.sub(repl, text)


def _resolve_match(
    match: re.Match[str],
    *,
    context: ConfigRefContext,
    providers: Mapping[str, ConfigRefProvider],
) -> Any:
    scheme = match.group(1).strip().lower()
    key = match.group(2).strip()
    provider = providers.get(scheme)
    if provider is None:
        raise ConfigRefError(
            f"Unsupported config reference scheme '{scheme}' in '{match.group(0)}'."
        )
    return provider.resolve(key, context=context)


def _merged_env(project_yaml: Path) -> dict[str, str]:
    env = dict(_load_dotenv(project_yaml.parent / ".env"))
    env.update(os.environ)
    return env


@lru_cache(maxsize=None)
def _load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_dotenv_line(raw_line)
        if parsed is None:
            continue
        key, value = parsed
        values[key] = value
    return values


def _parse_dotenv_line(line: str) -> tuple[str, str] | None:
    text = line.strip()
    if not text or text.startswith("#"):
        return None
    if text.startswith("export "):
        text = text[len("export ") :].lstrip()
    if "=" not in text:
        return None
    key, raw_value = text.split("=", 1)
    key = key.strip()
    if not key:
        return None

    value = raw_value.strip()
    if not value:
        return key, ""

    quote = value[0]
    if quote in {"'", '"'} and value.endswith(quote):
        inner = value[1:-1]
        if quote == '"':
            inner = (
                inner.replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t")
                .replace('\\"', '"')
                .replace("\\\\", "\\")
            )
        return key, inner

    comment_index = value.find(" #")
    if comment_index != -1:
        value = value[:comment_index].rstrip()
    return key, value


__all__ = [
    "ConfigRefContext",
    "ConfigRefError",
    "ConfigRefProvider",
    "EnvConfigRefProvider",
    "default_config_ref_providers",
    "resolve_config_refs",
]
