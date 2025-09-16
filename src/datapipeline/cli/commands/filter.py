from datapipeline.services.scaffold.filter import create_filter


def handle(subcmd: str, name: str | None) -> None:
    if subcmd == "create":
        if not name:
            print("❗ --name is required for filter create")
            raise SystemExit(2)
        create_filter(name=name, root=None)

