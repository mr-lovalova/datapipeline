from pathlib import Path

from datapipeline.services.scaffold.domain import create_domain


def handle(subcmd: str, domain: str | None, *, plugin_root: Path | None = None) -> None:
    if subcmd in {"create", "add"}:
        if not domain:
            print(
                "[error] Domain name is required. Use 'jerry domain add <name>' "
                "or pass -n/--name."
            )
            raise SystemExit(2)
        create_domain(domain=domain, root=plugin_root)
