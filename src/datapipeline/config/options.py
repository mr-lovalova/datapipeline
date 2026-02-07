"""Shared CLI/config option sets.

Keep these centralized so CLI prompts, parser choices, and config validation
stay in sync.
"""

OUTPUT_TRANSPORTS = ("stdout", "fs")
OUTPUT_FORMATS = ("print", "jsonl", "json", "csv", "pickle")
OUTPUT_STDOUT_FORMATS = ("print", "jsonl", "json")

SOURCE_TRANSPORTS = ("fs", "http", "synthetic")
SOURCE_FS_HTTP_FORMATS = ("csv", "json", "jsonl")
SOURCE_FS_FORMATS = (*SOURCE_FS_HTTP_FORMATS, "pickle")

VISUAL_CHOICES = ("auto", "tqdm", "rich", "off")
PROGRESS_CHOICES = ("auto", "spinner", "bars", "off")


def source_formats_for(transport: str) -> tuple[str, ...]:
    return SOURCE_FS_FORMATS if (transport or "").lower() == "fs" else SOURCE_FS_HTTP_FORMATS
