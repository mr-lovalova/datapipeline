SOURCE_TRANSPORTS = ("fs", "http", "synthetic")
SOURCE_HTTP_FORMATS = ("csv", "json", "jsonl")
SOURCE_FS_FORMATS = (*SOURCE_HTTP_FORMATS, "pickle")


def source_formats_for(transport: str) -> tuple[str, ...]:
    if transport == "fs":
        return SOURCE_FS_FORMATS
    if transport == "http":
        return SOURCE_HTTP_FORMATS
    raise ValueError(f"Transport {transport!r} does not use a source format.")
