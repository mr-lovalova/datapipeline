from pathlib import Path

from datapipeline.services.scaffold.domain import create_domain
from datapipeline.services.scaffold.utils import error_exit


def handle(subcmd: str, domain: str | None, *, plugin_root: Path | None = None) -> None:
    if subcmd == "create":
        if not domain:
            error_exit(
                "Domain name is required. Use 'jerry domain create <name>' "
                "or pass -n/--name."
            )
        create_domain(domain=domain, root=plugin_root)
