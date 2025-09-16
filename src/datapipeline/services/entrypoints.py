import re
from pathlib import Path

EP_PREFIX = "datapipeline"


def inject_ep(toml: str, comp: str, key: str, target: str) -> str:
    """Idempotently inject or update an entry-point in pyproject.toml.

    Robust approach:
    - Locate the [project.entry-points."datapipeline.<comp>"] block
    - Parse all existing lines (quoted or unquoted keys)
    - Update/insert the specified key → target
    - Reconstruct the block with unique, quoted keys (stable, deduped)
    - Create the block if it doesn't exist
    """
    header = f'[project.entry-points."{EP_PREFIX}.{comp}"]'
    block_re = rf"{re.escape(header)}\n([\s\S]*?)(?=\n\[|$)"

    def parse_block(body: str) -> dict[str, str]:
        entries: dict[str, str] = {}
        for line in body.splitlines():
            s = line.strip()
            if not s or s.startswith('#'):
                continue
            m = re.match(r'^(?:\"([^\"]+)\"|([^\s=]+))\s*=\s*\"([^\"]+)\"\s*$', s)
            if m:
                k = m.group(1) or m.group(2)
                v = m.group(3)
                entries[k] = v
        return entries

    def render_block(entries: dict[str, str]) -> str:
        lines = [header]
        for k, v in entries.items():
            lines.append(f'"{k}" = "{v}"')
        return "\n".join(lines) + "\n"

    m = re.search(block_re, toml)
    if m:
        body = m.group(1)
        entries = parse_block(body)
        entries[key] = target
        new_block = render_block(entries)
        # Replace the entire block (header + body) with normalized block
        full_block_re = rf"{re.escape(header)}\n[\s\S]*?(?=\n\[|$)"
        return re.sub(full_block_re, new_block.rstrip(), toml) + ("\n" if not toml.endswith("\n") else "")
    else:
        # Create new block
        return toml.rstrip() + "\n\n" + render_block({key: target})


def read_group_entries(pyproject: Path, comp: str) -> dict[str, str]:
    header = f'[project.entry-points."{EP_PREFIX}.{comp}"]'
    m = re.search(
        rf"{re.escape(header)}\n([\s\S]*?)(?=\n\[|$)", pyproject.read_text())
    if not m:
        return {}
    block = m.group(1)
    out: dict[str, str] = {}
    for line in block.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m2 = re.match(
            r'(?:(?:\"(?P<qkey>[^\"]+)\")|(?P<ukey>[A-Za-z0-9_.-]+))\s*=\s*\"(?P<target>[^\"]+)\"', line)
        if m2:
            out[m2.group('qkey') or m2.group('ukey')] = m2.group('target')
    return out
