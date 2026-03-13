"""Shared CLI/config option sets.

Keep these centralized so CLI prompts, parser choices, and config validation
stay in sync.
"""

OUTPUT_TRANSPORTS = ("stdout", "fs")
OUTPUT_FORMATS = ("jsonl", "csv", "pickle")
OUTPUT_INSPECT_FORMATS = ("jsonl", "csv", "pickle", "txt", "html")
OUTPUT_STDOUT_FORMATS = ("jsonl", "txt")
OUTPUT_VIEWS = ("raw", "flat")

SOURCE_TRANSPORTS = ("fs", "http", "synthetic")
SOURCE_FS_HTTP_FORMATS = ("csv", "json", "jsonl")
SOURCE_FS_FORMATS = (*SOURCE_FS_HTTP_FORMATS, "pickle")

VISUAL_CHOICES = ("on", "off")
LOG_TRANSPORT_CHOICES = ("stderr", "stdout", "fs")
LOG_SCOPE_CHOICES = ("global", "run")


def source_formats_for(transport: str) -> tuple[str, ...]:
    return SOURCE_FS_FORMATS if (transport or "").lower() == "fs" else SOURCE_FS_HTTP_FORMATS
