from pathlib import Path

from datapipeline.services.scaffold.source import create_source


def handle(
    subcmd: str,
    provider: str | None,
    dataset: str | None,
    transport: str | None = None,
    format: str | None = None,
    *,
    identity: bool = False,
    alias: str | None = None,
    plugin_root: Path | None = None,
) -> None:
    if subcmd in {"create", "add"}:
        # Allow: positional provider dataset, --provider/--dataset, --alias, or provider as 'prov.ds'
        if (not provider or not dataset):
            # Try alias flag first
            if alias:
                parts = alias.split(".", 1)
                if len(parts) == 2 and all(parts):
                    provider, dataset = parts[0], parts[1]
                else:
                    print("[error] Alias must be 'provider.dataset'")
                    raise SystemExit(2)
            # Try provider passed as 'prov.ds' positional/flag
            elif provider and ("." in provider) and not dataset:
                parts = provider.split(".", 1)
                if len(parts) == 2 and all(parts):
                    provider, dataset = parts[0], parts[1]
                else:
                    print("[error] Source must be specified as '<provider> <dataset>' or '<provider>.<dataset>'")
                    raise SystemExit(2)

        if not provider or not dataset:
            print("[error] Source requires '<provider> <dataset>' (or -a/--alias provider.dataset)")
            raise SystemExit(2)
        if not transport:
            print("[error] --transport is required (fs|http|synthetic)")
            raise SystemExit(2)
        if transport in {"fs", "http"} and not format:
            print("[error] --format is required for fs/http transports (fs: csv|json|json-lines|pickle, http: csv|json|json-lines)")
            raise SystemExit(2)
        create_source(
            provider=provider,
            dataset=dataset,
            transport=transport,
            format=format,
            root=plugin_root,
            identity=identity,
        )
