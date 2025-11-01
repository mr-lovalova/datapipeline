import sys
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.entrypoints import read_group_entries
from datapipeline.services.constants import FILTERS_GROUP
from datapipeline.services.project_paths import (
    sources_dir as resolve_sources_dir,
    streams_dir as resolve_streams_dir,
    ensure_project_scaffold,
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


def handle() -> None:
    root_dir, name, pyproject = pkg_root(None)

    # Discover sources by scanning sources_dir YAMLs
    # Default to dataset-scoped project config
    proj_path = root_dir / "config" / "datasets" / "default" / "project.yaml"
    # Ensure a minimal project scaffold so we can resolve dirs interactively
    ensure_project_scaffold(proj_path)
    sources_dir = resolve_sources_dir(proj_path)
    source_options = []
    if sources_dir.exists():
        source_options = sorted(p.stem for p in sources_dir.glob("*.y*ml"))
    if not source_options:
        print("[error] No sources found. Create one first (jerry source add ...)")
        raise SystemExit(2)

    src_key = _pick_from_list("Select a source to link:", source_options)
    # Expect aliases from sources_dir filenames: provider_dataset.yaml
    parts = src_key.split("_", 1)
    if len(parts) != 2:
        print("[error] Source alias must be 'provider_dataset' (from sources/<alias>.yaml)", file=sys.stderr)
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

    dom_name = _pick_from_list("Select a domain to link to:", domain_options)

    # create mapper + EP (domain.origin)
    attach_source_to_domain(
        domain=dom_name,
        provider=provider,
        dataset=dataset,
        root=None,
    )

    def _slug(s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"[^a-z0-9]+", "_", s)
        return s.strip("_")
    ep_key = f"{_slug(dom_name)}.{_slug(provider)}"
    print(f"[ok] Registered mapper entry point as '{ep_key}'.")

    # Inject per-file canonical stream into streams directory
    streams_path = resolve_streams_dir(proj_path)

    canonical_alias = ep_key
    mapper_ep = ep_key
    # Write a single-file canonical spec into streams directory, matching
    # ContractConfig schema with helpful commented placeholders per stage.
    try:
        # Ensure streams_path is a directory path
        streams_dir = streams_path if streams_path.is_dir() else streams_path.parent
        streams_dir.mkdir(parents=True, exist_ok=True)
        cfile = streams_dir / f"{canonical_alias}.yaml"
        # Build a richer scaffold as YAML text to preserve comments
        scaffold = f"""
source_id: {src_key}
stream_id: {canonical_alias}

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
        print(f"[new] Created canonical spec: {cfile}")
    except Exception as e:
        print(f"[error] Failed to write canonical spec: {e}", file=sys.stderr)
