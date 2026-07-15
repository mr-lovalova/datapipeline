import logging
from pathlib import Path

from datapipeline.services.scaffold.domain import create_domain

logger = logging.getLogger(__name__)


def handle(subcmd: str, domain: str | None, *, plugin_root: Path | None = None) -> None:
    if subcmd != "create":
        raise SystemExit(f"Unknown domain subcommand: {subcmd}")
    if not domain:
        raise SystemExit(
            "Domain name is required. Use 'jerry domain create <name>' "
            "or pass -n/--name."
        )
    try:
        path = create_domain(domain, plugin_root)
    except (FileExistsError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
    logger.info("Domain: %s", path)
