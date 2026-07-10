import logging
from dataclasses import asdict, is_dataclass
from typing import Iterator, Any

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import PartitionedStreamTransformBase

logger = logging.getLogger(__name__)


class IdentityGuardTransform(PartitionedStreamTransformBase):
    """Validate that per-stream identity fields remain constant.

    Parameters
    - mode: 'warn' (default) logs warnings; 'error' raises on first violation
    - fields: optional explicit list of attribute names to compare. When omitted,
      the transform attempts to derive identity from dataclass fields on the
      underlying record, excluding 'time'.
    """

    def __init__(
        self,
        *,
        mode: str = "warn",
        fields: list[str] | None = None,
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(partition_by=partition_by)
        self.mode = mode
        self.fields = fields

    def _violation(self, msg: str) -> None:
        if self.mode == "error":
            raise ValueError(msg)
        logger.warning(msg)

    def _identity_map(self, rec: Any) -> dict:
        # Prefer explicit fields if provided
        if self.fields:
            out = {}
            for f in self.fields:
                try:
                    out[f] = getattr(rec, f)
                except Exception:
                    out[f] = None
            return out
        # Fall back to partition_by when available
        if self.partition_by:
            fields = (
                [self.partition_by]
                if isinstance(self.partition_by, str)
                else list(self.partition_by)
            )
            out = {}
            for f in fields:
                try:
                    out[f] = getattr(rec, f)
                except Exception:
                    out[f] = None
            return out
        # Try domain-provided hook first
        if hasattr(rec, "identity_fields") and callable(
            getattr(rec, "identity_fields")
        ):
            try:
                return rec.identity_fields()  # type: ignore[attr-defined]
            except Exception:
                pass
        # Fallback: dataclass fields minus time
        if is_dataclass(rec):
            data = asdict(rec)
            data.pop("time", None)
            return data
        return {}

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        current_key: tuple | None = None
        baseline: dict | None = None
        for rec in stream:
            key = self.partition_key(rec)
            ident = self._identity_map(rec)
            if key != current_key:
                current_key = key
                baseline = ident
            else:
                if ident != baseline:
                    self._violation(
                        "identity drift in record stream key=%s: expected=%s observed=%s"
                        % (key, baseline, ident)
                    )
            yield rec
