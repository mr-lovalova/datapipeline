from dataclasses import dataclass
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
    )


def _describe_foreach_loader(loader: ForeachLoader) -> LoaderObservability:
    value = getattr(loader, "_current_value", None)
    idx = getattr(loader, "_current_index", None)
    values = getattr(loader, "_values", None)
    total = len(values) if isinstance(values, list) else None
    entrypoint = ""
    spec = getattr(loader, "_loader_spec", None)
    if isinstance(spec, dict):
        entrypoint = str(spec.get("entrypoint", ""))
    args = getattr(loader, "_current_args", None)
    action = None
    if entrypoint == "core.io" and isinstance(args, dict):
        transport_name = args.get("transport")
        if transport_name == "http":
            action = "Downloading"
        elif transport_name == "fs":
            action = "Loading"
    elif entrypoint:
        action = f"via {entrypoint}"

    if value is None:
        current_label = None
    else:
        parts: list[str] = []
        if action:
            parts.append(action)
        parts.append(f"\"{value}\"")
        if isinstance(idx, int) and isinstance(total, int) and total > 0:
            parts.append(f"({idx}/{total})")
        current_label = " ".join(parts)

    return LoaderObservability(
        transport=getattr(loader, "_current_transport", None),
        current_label=current_label,
        composed_inputs=None,
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
