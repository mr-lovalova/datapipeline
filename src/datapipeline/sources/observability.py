from dataclasses import dataclass
from pathlib import Path
from typing import Any

from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.models.loader import SyntheticLoader


@dataclass(frozen=True)
class LoaderObservability:
    transport: Any | None
    current_label: str | None
    composed_inputs: list[str] | None
    info_lines: list[str] | None = None
    debug_lines: list[str] | None = None


def describe_loader(loader: Any) -> LoaderObservability:
    if isinstance(loader, ForeachLoader):
        return _describe_foreach_loader(loader)
    if isinstance(loader, SyntheticLoader):
        return _describe_synthetic_loader(loader)

    return LoaderObservability(
        transport=getattr(loader, "transport", None),
        current_label=None,
        composed_inputs=_coerce_inputs(
            getattr(getattr(loader, "_spec", None), "inputs", None)
        ),
        info_lines=_loader_lines(loader, "info_lines"),
        debug_lines=_loader_lines(loader, "debug_lines"),
    )


def _loader_lines(loader: Any, attr: str) -> list[str] | None:
    producer = getattr(loader, attr, None)
    if not callable(producer):
        return None
    try:
        values = list(producer() or [])
    except Exception:
        return None
    lines = [str(value).strip() for value in values if str(value).strip()]
    return lines or None


def _describe_foreach_loader(loader: ForeachLoader) -> LoaderObservability:
    value = getattr(loader, "_current_value", None)
    values = getattr(loader, "_values", None)
    entrypoint = ""
    spec_args = None
    spec = getattr(loader, "_loader_spec", None)
    if isinstance(spec, dict):
        entrypoint = str(spec.get("entrypoint", ""))
        maybe_args = spec.get("args")
        if isinstance(maybe_args, dict):
            spec_args = maybe_args
    args = getattr(loader, "_current_args", None)
    action = foreach_action(entrypoint, args, spec_args)

    if value is None:
        current_label = None
    else:
        value_label = foreach_value_label(value)
        current_label = join_action_value(action, value_label)

    info_lines = foreach_info_lines(entrypoint, args, spec_args, values)
    return LoaderObservability(
        transport=getattr(loader, "_current_transport", None),
        current_label=current_label,
        composed_inputs=None,
        info_lines=info_lines,
    )


def _describe_synthetic_loader(loader: SyntheticLoader) -> LoaderObservability:
    generator = getattr(loader, "generator", None)
    generator_name = type(generator).__name__ if generator is not None else "generator"
    info_lines = []
    debug_lines = []
    if generator is not None:
        try:
            info_lines = list(getattr(generator, "info_lines", lambda: [])() or [])
        except Exception:
            info_lines = []
        try:
            debug_lines = list(getattr(generator, "debug_lines", lambda: [])() or [])
        except Exception:
            debug_lines = []
    if not info_lines:
        info_lines = [f"synthetic.generate: {generator_name}"]
    return LoaderObservability(
        transport=getattr(loader, "transport", None),
        current_label=None,
        composed_inputs=None,
        info_lines=info_lines,
        debug_lines=debug_lines or None,
    )


def _coerce_inputs(raw: Any) -> list[str] | None:
    if not isinstance(raw, (list, tuple)):
        return None
    values = [str(item) for item in raw if str(item)]
    return values or None


def foreach_transport_name(args: Any, spec_args: Any) -> str | None:
    if isinstance(args, dict):
        raw = args.get("transport")
        if raw is not None:
            return str(raw).strip().lower() or None
    if isinstance(spec_args, dict):
        raw = spec_args.get("transport")
        if raw is not None:
            return str(raw).strip().lower() or None
    return None


def foreach_action(entrypoint: str, args: Any, spec_args: Any) -> str | None:
    if entrypoint == "core.io":
        transport_name = foreach_transport_name(args, spec_args)
        if transport_name == "http":
            return "Downloading"
        if transport_name == "fs":
            return "Loading"
        return None
    if entrypoint:
        return f"via {entrypoint}"
    return None


def foreach_value_label(value: Any) -> str:
    text = str(value)
    name = Path(text).name
    return name or text


def join_action_value(action: str | None, value_label: str) -> str:
    if action:
        return f'{action} "{value_label}"'
    return f'"{value_label}"'


def foreach_info_lines(
    entrypoint: str,
    args: Any,
    spec_args: Any,
    values: Any,
) -> list[str] | None:
    if entrypoint != "core.io":
        return None
    if foreach_transport_name(args, spec_args) != "fs":
        return None
    if not isinstance(values, list):
        return None
    if not values:
        return ["fs.glob: 0 files"]

    labels = [foreach_value_label(value) for value in values]
    total = len(labels)
    if total == 1:
        return [f"fs.glob: 1 file (file={labels[0]})"]
    return [f"fs.glob: {total} files (first={labels[0]}, last={labels[-1]})"]
