from datapipeline.services.scaffold.domain import create_domain


def handle(subcmd: str, domain: str | None, time_aware: bool) -> None:
    if subcmd == "create":
        if not domain:
            print("❗ --domain is required")
            raise SystemExit(2)
        create_domain(domain=domain, time_aware=time_aware, root=None)
