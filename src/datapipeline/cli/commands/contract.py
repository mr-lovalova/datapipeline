import sys
from pathlib import Path

from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.entrypoints import read_group_entries, inject_ep
from datapipeline.services.constants import FILTERS_GROUP, MAPPERS_GROUP
from datapipeline.services.project_paths import (
    sources_dir as resolve_sources_dir,
    streams_dir as resolve_streams_dir,
    ensure_project_scaffold,
    resolve_project_yaml_path,
)
from datapipeline.services.scaffold.mappers import attach_source_to_domain
import re


def _pick_from_list(prompt: str, options: list[str]) -> str:
    print(prompt, file=sys.stderr)
    for i, opt in enumerate(options, 1):
        print(f"  [{i}] {opt}", file=sys.stderr)
    while True:
        sel = input("> ").strip()
        if sel.isdigit():
            idx = int(sel)
            if 1 <= idx <= len(options):
                return options[idx - 1]
        print("Please enter a number from the list.", file=sys.stderr)


def handle(
    *,
    plugin_root: Path | None = None,
    use_identity: bool = False,
) -> None:
    root_dir, name, pyproject = pkg_root(plugin_root)
    # Select contract type: Ingest (source->stream) or Composed (streams->stream)
    print("Select contract type:", file=sys.stderr)
    print("  [1] Ingest (source → stream)", file=sys.stderr)
    print("  [2] Composed (streams → stream)", file=sys.stderr)
    sel = input("> ").strip()
    if sel == "2":
        if use_identity:
            print("[error] --identity is only supported for ingest contracts.", file=sys.stderr)
            raise SystemExit(2)
        # Defer to composed scaffolder (fully interactive)
        scaffold_conflux(
            stream_id=None,
            inputs=None,
            mapper_path=None,
            with_mapper_stub=True,
            plugin_root=plugin_root,
        )
        return

    # Discover sources by scanning sources_dir YAMLs
    # Default to dataset-scoped project config
    proj_path = resolve_project_yaml_path(root_dir)
    # Ensure a minimal project scaffold so we can resolve dirs interactively
    ensure_project_scaffold(proj_path)
    sources_dir = resolve_sources_dir(proj_path)
    source_options: list[str] = []
    if sources_dir.exists():
        # Recursively scan YAMLs and read declared source id (alias)
        from datapipeline.utils.load import load_yaml
        from datapipeline.services.constants import PARSER_KEY, LOADER_KEY, SOURCE_ID_KEY
        for p in sorted(sources_dir.rglob("*.y*ml")):
            try:
                data = load_yaml(p)
            except Exception:
                continue
            if isinstance(data, dict) and isinstance(data.get(PARSER_KEY), dict) and isinstance(data.get(LOADER_KEY), dict):
                alias = data.get(SOURCE_ID_KEY)
                if isinstance(alias, str):
                    source_options.append(alias)
        source_options = sorted(set(source_options))
    if not source_options:
        print("[error] No sources found. Create one first (jerry source add ...)")
        raise SystemExit(2)

    src_key = _pick_from_list(
        "Select a source for the contract:", source_options)
    # Expect aliases as 'provider.dataset' (from source file's id)
    parts = src_key.split(".", 1)
    if len(parts) != 2:
        print("[error] Source alias must be 'provider.dataset' (from source file's id)", file=sys.stderr)
        raise SystemExit(2)
    provider, dataset = parts[0], parts[1]

    # Discover domains by scanning the package, fallback to EPs if needed
    base = resolve_base_pkg_dir(root_dir, name)
    domain_options = []
    for dirname in ("domains",):
        dom_dir = base / dirname
        if dom_dir.exists():
            domain_options.extend(
                [p.name for p in dom_dir.iterdir() if p.is_dir()
                 and (p / "model.py").exists()]
            )
    domain_options = sorted(set(domain_options))
    if not domain_options:
        domain_options = sorted(
            read_group_entries(pyproject, FILTERS_GROUP).keys())
    if not domain_options:
        print("[error] No domains found. Create one first (jerry domain add ...)")
        raise SystemExit(2)

    dom_name = _pick_from_list(
        "Select a domain to contract with:", domain_options)

    def _slug(s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"[^a-z0-9]+", "_", s)
        return s.strip("_")

    if use_identity:
        mapper_ep = "identity"
        print("[ok] Using built-in mapper entry point 'identity'.")
    else:
        # create mapper + EP (domain.origin)
        attach_source_to_domain(
            domain=dom_name,
            provider=provider,
            dataset=dataset,
            root=plugin_root,
        )
        ep_key = f"{_slug(dom_name)}.{_slug(dataset)}"
        print(f"[ok] Registered mapper entry point as '{ep_key}'.")
        mapper_ep = ep_key

    # Derive canonical stream id as domain.dataset[.variant]
    print("Optional variant suffix (press Enter to skip):", file=sys.stderr)
    variant = input("> ").strip()
    if variant:
        canonical_alias = f"{_slug(dom_name)}.{_slug(dataset)}.{_slug(variant)}"
    else:
        canonical_alias = f"{_slug(dom_name)}.{_slug(dataset)}"

    # Inject per-file canonical stream into streams directory
    streams_path = resolve_streams_dir(proj_path)

    # canonical_alias and mapper_ep defined above
    # Write a single-file canonical spec into streams directory, matching
    # ContractConfig schema with helpful commented placeholders per stage.
    try:
        # Ensure streams_path is a directory path
        streams_dir = streams_path if streams_path.is_dir() else streams_path.parent
        streams_dir.mkdir(parents=True, exist_ok=True)
        cfile = streams_dir / f"{canonical_alias}.yaml"
        # Build a richer scaffold as YAML text to preserve comments
        scaffold = f"""
kind: ingest
source: {src_key}
id: {canonical_alias}  # format: domain.dataset.(variant)

mapper:
  entrypoint: {mapper_ep}
  args: {{}}

# partition_by: <field or [fields]> 
# sort_batch_size: 100000              # in-memory sort chunk size

record:                              # record-level transforms
  - filter: {{ operator: ge, field: time, comparand: "${{start_time}}" }}
  - filter: {{ operator: le, field: time, comparand: "${{end_time}}" }}
#   - floor_time: {{ resolution: 10m }}
#   - lag: {{ lag: 10m }}

# stream:                              # per-feature transforms (input sorted by id,time)
#   - ensure_ticks: {{ tick: 10m }}
#   - granularity: {{ mode: first }}
#   - fill: {{ statistic: median, window: 6, min_samples: 1 }}

# debug:                               # optional validation-only checks
#   - lint: {{ mode: warn, tick: 10m }}
"""
        with cfile.open("w", encoding="utf-8") as f:
            f.write(scaffold)
        print(f"[new] canonical spec: {cfile}")
    except Exception as e:
        print(f"[error] Failed to write canonical spec: {e}", file=sys.stderr)


def scaffold_conflux(
    *,
    stream_id: str | None,
    inputs: str | None,
    mapper_path: str | None,
    with_mapper_stub: bool,
    plugin_root: Path | None,
) -> None:
    """Scaffold a composed (multi-input) contract and optional mapper stub.

    inputs: comma-separated list of "[alias=]ref[@stage]" strings.
    mapper_path default: <pkg>.domains.<domain>:mapper where domain = stream_id.split('.')[0]
    """
    root_dir, name, _ = pkg_root(plugin_root)
    # Resolve default project path early for interactive selections
    proj_path = resolve_project_yaml_path(root_dir)
    ensure_project_scaffold(proj_path)
    # Defer target domain selection until after choosing inputs

    # We will write the contract after selecting inputs and target domain
    # Build inputs string first: interactive select, then target domain
    if not inputs:
        # Interactive selection of canonical streams (scan recursively, read ids)
        streams: list[str] = []
        sdir = resolve_streams_dir(proj_path)
        if sdir.exists():
            from datapipeline.utils.load import load_yaml
            from datapipeline.services.constants import STREAM_ID_KEY
            for p in sorted(sdir.rglob("*.y*ml")):
                try:
                    data = load_yaml(p)
                except Exception:
                    continue
                if isinstance(data, dict) and data.get("kind") in {"ingest", "composed"}:
                    sid = data.get(STREAM_ID_KEY)
                    if isinstance(sid, str) and sid:
                        streams.append(sid)
        streams = sorted(set(streams))
        if not streams:
            print(
                "[error] No canonical streams found. Create them first via 'jerry contract' (ingest).", file=sys.stderr)
            raise SystemExit(2)
        print(
            "Select one or more input streams (comma-separated numbers):", file=sys.stderr)
        for i, sid in enumerate(streams, 1):
            print(f"  [{i}] {sid}", file=sys.stderr)
        sel = input("> ").strip()
        try:
            idxs = [int(x) for x in sel.split(',') if x.strip()]
        except ValueError:
            print("[error] Invalid selection.", file=sys.stderr)
            raise SystemExit(2)
        picked = []
        for i in idxs:
            if 1 <= i <= len(streams):
                picked.append(streams[i-1])
        if not picked:
            print("[error] No inputs selected.", file=sys.stderr)
            raise SystemExit(2)
        # Build default aliases using domain+variant to avoid collisions.
        # Stream id format: domain.dataset.variant (variant optional)
        built = []
        for ref in picked:
            parts = ref.split(".")
            if len(parts) >= 3:
                domain, variant = parts[0], parts[-1]
                alias = f"{domain}_{variant}"
            elif len(parts) == 2:
                # No explicit variant -> use domain as alias
                alias = parts[0]
            else:
                # Fallback to full ref if unexpected
                alias = ref
            built.append(f"{alias}={ref}")
        inputs = ",".join(built)

    # YAML list items do not need commas; avoid embedding commas in item text
    inputs_list = "\n  - ".join(
        s.strip() for s in inputs.split(",") if s.strip()
    )

    # If no stream_id, select target domain now and derive stream id (mirror ingest flow)
    if not stream_id:
        base = resolve_base_pkg_dir(root_dir, name)
        domain_options: list[str] = []
        dom_dir = base / "domains"
        if dom_dir.exists():
            domain_options.extend(
                [p.name for p in dom_dir.iterdir() if p.is_dir()
                 and (p / "model.py").exists()]
            )
        domain_options = sorted(set(domain_options))
        if not domain_options:
            print("[error] No domains found. Create one first (jerry domain add ...)")
            raise SystemExit(2)
        print("Select a target domain for the composed stream:", file=sys.stderr)
        for i, opt in enumerate(domain_options, 1):
            print(f"  [{i}] {opt}", file=sys.stderr)
        sel = input("> ").strip()
        try:
            idx = int(sel)
            if idx < 1 or idx > len(domain_options):
                raise ValueError
        except Exception:
            print("[error] Invalid selection.", file=sys.stderr)
            raise SystemExit(2)
        domain = domain_options[idx - 1]
        stream_id = f"{domain}.processed"
        # Default mapper path uses import-safe package dir, not project name
        pkg_base = resolve_base_pkg_dir(root_dir, name).name
        mapper_path = mapper_path or f"{pkg_base}.mappers.{domain}:mapper"
    else:
        domain = stream_id.split('.')[0]
        pkg_base = resolve_base_pkg_dir(root_dir, name).name
        mapper_path = mapper_path or f"{pkg_base}.mappers.{domain}:mapper"

    # Optional mapper stub under mappers/
    if with_mapper_stub:
        base = resolve_base_pkg_dir(root_dir, name)
        map_pkg_dir = base / "mappers"
        map_pkg_dir.mkdir(parents=True, exist_ok=True)
        (map_pkg_dir / "__init__.py").touch(exist_ok=True)
        mapper_file = map_pkg_dir / f"{domain}.py"
        if not mapper_file.exists():
            mapper_file.write_text(
                """
from typing import Iterator, Mapping
from datapipeline.domain.record import TemporalRecord


def mapper(
    inputs: Mapping[str, Iterator[TemporalRecord]],
    *, driver: str | None = None, aux: Mapping[str, Iterator[TemporalRecord]] | None = None, context=None, **params
) -> Iterator[TemporalRecord]:
    # TODO: implement domain math; inputs are ordered/regularized; aux is raw
    key = driver or next(iter(inputs.keys()))
    for rec in inputs[key]:
        yield rec  # replace with your dataclass and computation
""".lstrip()
            )
            print(f"[new] {mapper_file}")
        # Register mapper entry point under datapipeline.mappers
        # Choose EP name equal to stream_id for clarity/reuse
        ep_key = stream_id
        # If mapper_path looks like a dotted target (module:attr), use it; otherwise build default target
        package_name = base.name  # filesystem package dir is import-safe (underscored)
        default_target = f"{package_name}.mappers.{domain}:mapper"
        ep_target = mapper_path if (
            mapper_path and ":" in mapper_path) else default_target
        pyproj_path = root_dir / "pyproject.toml"
        try:
            toml_text = pyproj_path.read_text()
            updated = inject_ep(toml_text, MAPPERS_GROUP, ep_key, ep_target)
            if updated != toml_text:
                pyproj_path.write_text(updated)
                print(
                    f"[ok] Registered mapper entry point '{ep_key}' -> {ep_target}")
        except FileNotFoundError:
            print(
                "[info] pyproject.toml not found; skipping entry point registration", file=sys.stderr)
        # From here on, reference the EP name in the YAML
        mapper_path = ep_key
    # Contract file path (now that stream_id is known)
    ensure_project_scaffold(proj_path)
    streams_path = resolve_streams_dir(proj_path)
    streams_dir = streams_path if streams_path.is_dir() else streams_path.parent
    streams_dir.mkdir(parents=True, exist_ok=True)
    cfile = streams_dir / f"{stream_id}.yaml"
    if cfile.exists():
        print(f"[info] Contract already exists, skipping: {cfile}")
        return

    yaml_text = f"""
kind: composed
id: {stream_id}  # format: domain.dataset.(variant)
# partition_by: <field or [fields]>
inputs:
  - {inputs_list}

mapper:
  entrypoint: {mapper_path}
  args: {{ driver: {(inputs.split(',')[0].split('=')[0].strip() if '=' in inputs.split(',')[0] else inputs.split(',')[0].strip())} }}
"""
    cfile.write_text(yaml_text.strip() + "\n", encoding="utf-8")
    print(f"[new] composed contract: {cfile}")
