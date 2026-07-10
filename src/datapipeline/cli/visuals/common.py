import logging
from collections.abc import Sequence

logger = logging.getLogger(__name__)


def log_combined_stream(
    stream_id: str,
    details: Sequence[str] | str | None,
    indent: str = "",
) -> None:
    """Emit descriptive logs for virtual multi-input sources."""

    entries: list[str] = []
    if isinstance(details, str):
        entries = [part.strip() for part in details.split(",") if part.strip()]
    elif isinstance(details, Sequence):
        entries = [str(part).strip() for part in details if str(part).strip()]
    elif details is not None:
        item = str(details).strip()
        if item:
            entries = [item]

    if entries:
        logger.info("%s[%s] Feature engineering from:", indent, stream_id)
        for entry in entries:
            left, sep, right = entry.partition("=")
            if sep:
                mapping = f"{left.strip()} -> {right.strip()}"
            else:
                mapping = entry
            logger.info("%s[%s]   - %s", indent, stream_id, mapping)
    else:
        logger.info(
            "%s[%s] Feature engineering from upstream inputs", indent, stream_id
        )
