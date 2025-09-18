import sys
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.entrypoints import read_group_entries
import yaml
from datapipeline.services.constants import FILTERS_GROUP, MAPPER_KEY, ENTRYPOINT_KEY, ARGS_KEY, SOURCE_KEY
from datapipeline.services.project_paths import sources_dir as resolve_sources_dir, streams_dir as resolve_streams_dir
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


def handle(time_aware: bool) -> None:
    root_dir, name, pyproject = pkg_root(None)

    # Discover sources by scanning sources_dir YAMLs
    proj_path = root_dir / "config" / "project.yaml"
    sources_dir = resolve_sources_dir(proj_path)
    source_options = []
    if sources_dir.exists():
        source_options = sorted(p.stem for p in sources_dir.glob("*.y*ml"))
    if not source_options:
        print("❗ No sources found. Create one first (jerry distillery add ...)")
        raise SystemExit(2)

    src_key = _pick_from_list("Select a source to link:", source_options)
    # Expect aliases from sources_dir filenames: provider_dataset.yaml
    parts = src_key.split("_", 1)
    if len(parts) != 2:
        print("❗ Source alias must be 'provider_dataset' (from sources/<alias>.yaml)", file=sys.stderr)
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
        print("❗ No domains found. Create one first (jerry spirit add ...)")
        raise SystemExit(2)

    dom_name = _pick_from_list("Select a domain to link to:", domain_options)

    # create mapper + EP (domain.origin)
    attach_source_to_domain(domain=dom_name, provider=provider,
                            dataset=dataset, time_aware=time_aware, root=None)

    def _slug(s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"[^a-z0-9]+", "_", s)
        return s.strip("_")
    ep_key = f"{_slug(dom_name)}.{_slug(provider)}"
    print(f"✅ Registered mapper entry point as '{ep_key}'.")

    # Inject per-file canonical stream into streams directory
    streams_path = resolve_streams_dir(proj_path)

    canonical_alias = src_key  # default canonical stream alias
    mapper_ep = ep_key
    # Write a single-file canonical spec into streams directory
    try:
        # Ensure streams_path is a directory path
        streams_dir = streams_path if streams_path.is_dir() else streams_path.parent
        streams_dir.mkdir(parents=True, exist_ok=True)
        cfile = streams_dir / f"{canonical_alias}.yaml"
        data = {
            SOURCE_KEY: src_key,
            MAPPER_KEY: {ENTRYPOINT_KEY: mapper_ep, ARGS_KEY: {}},
        }
        with cfile.open("w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False, default_flow_style=False)
        print(f"✨ Created canonical spec: {cfile}")
    except Exception as e:
        print(f"❗ Failed to write canonical spec: {e}", file=sys.stderr)
