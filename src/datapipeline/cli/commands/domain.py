from datapipeline.services.scaffold.domain import create_domain


def handle(subcmd: str, domain: str | None) -> None:
    if subcmd in {"create", "add"}:
        if not domain:
            print("[error] --domain is required")
            raise SystemExit(2)
        create_domain(domain=domain, root=None)
