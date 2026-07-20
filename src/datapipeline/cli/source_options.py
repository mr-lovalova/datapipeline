SOURCE_TRANSPORTS = ("fs", "http", "synthetic")
SOURCE_FORMATS = ("csv", "json", "jsonl")


def source_formats_for(transport: str) -> tuple[str, ...]:
    if transport in {"fs", "http"}:
        return SOURCE_FORMATS
    raise ValueError(f"Transport {transport!r} does not use a source format.")
