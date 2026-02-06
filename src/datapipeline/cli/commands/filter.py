from datapipeline.services.scaffold.filter import create_filter
from datapipeline.services.scaffold.utils import error_exit


def handle(subcmd: str, name: str | None) -> None:
    if subcmd == "create":
        if not name:
            error_exit("--name is required for filter create")
        create_filter(name=name, root=None)
