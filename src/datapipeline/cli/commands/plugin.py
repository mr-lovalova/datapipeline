import logging
from pathlib import Path
from datapipeline.services.scaffold.plugin import scaffold_plugin


logger = logging.getLogger(__name__)


def bar(subcmd: str, name: str | None, out: str) -> None:
    if subcmd == "init":
        if not name:
            logger.error("Plugin name is required. Use 'jerry plugin init <name>' or pass -n/--name.")
            raise SystemExit(2)
        scaffold_plugin(name, Path(out))
