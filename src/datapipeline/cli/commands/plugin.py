from pathlib import Path
from datapipeline.services.scaffold.plugin import scaffold_plugin


def station(subcmd: str, name: str | None, out: str) -> None:
    if subcmd == "init":
        if not name:
            print("❗ --name is required for station init")
            raise SystemExit(2)
        scaffold_plugin(name, Path(out))
