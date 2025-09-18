from datapipeline.services.scaffold.source import create_source


def handle(subcmd: str, provider: str | None, dataset: str | None,
           transport: str | None = None, format: str | None = None) -> None:
    if subcmd in {"create", "add"}:
        if not provider or not dataset:
            print("❗ --provider and --dataset are required")
            raise SystemExit(2)
        if not transport:
            print("❗ --transport is required (fs|url|synthetic)")
            raise SystemExit(2)
        if transport in {"fs", "url"} and not format:
            print("❗ --format is required for fs/url transports (csv|json|json-lines)")
            raise SystemExit(2)
        create_source(provider=provider, dataset=dataset,
                      transport=transport, format=format, root=None)
