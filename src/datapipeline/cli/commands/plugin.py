from pathlib import Path
from datapipeline.services.scaffold.plugin import scaffold_plugin


def handle(subcmd: str, name: str | None, out: str) -> None:
    if subcmd == "init":
        if not name:
            print("❗ --name is required for plugin init")
            raise SystemExit(2)
        scaffold_plugin(name, Path(out))
