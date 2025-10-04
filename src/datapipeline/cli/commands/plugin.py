from pathlib import Path
from datapipeline.services.scaffold.plugin import scaffold_plugin


def bar(subcmd: str, name: str | None, out: str) -> None:
    if subcmd == "init":
        if not name:
            print("❗ --name is required for bar init")
            raise SystemExit(2)
        scaffold_plugin(name, Path(out))
