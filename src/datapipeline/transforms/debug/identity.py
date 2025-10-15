from __future__ import annotations

import logging
from dataclasses import asdict, is_dataclass
from typing import Iterator, Any

from datapipeline.domain.feature import FeatureRecord

logger = logging.getLogger(__name__)


class IdentityGuardTransform:
    """Validate that per-stream identity fields remain constant.

    Parameters
    - mode: 'warn' (default) logs warnings; 'error' raises on first violation
    - fields: optional explicit list of attribute names to compare. When omitted,
      the transform attempts to derive identity from dataclass fields on the
      underlying record, excluding 'time' and 'value'.
    """

    def __init__(self, *, mode: str = "warn", fields: list[str] | None = None) -> None:
        self.mode = mode
        self.fields = fields

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return self.apply(stream)

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
        # Try domain-provided hook first
        if hasattr(rec, "identity_fields") and callable(getattr(rec, "identity_fields")):
            try:
                return rec.identity_fields()  # type: ignore[attr-defined]
            except Exception:
                pass
        # Fallback: dataclass fields minus time/value
        if is_dataclass(rec):
            data = asdict(rec)
            data.pop("time", None)
            data.pop("value", None)
            return data
        return {}

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        current_key = None
        baseline: dict | None = None
        for fr in stream:
            key = fr.id
            rec = fr.record
            ident = self._identity_map(rec)
            if key != current_key:
                current_key = key
                baseline = ident
            else:
                if ident != baseline:
                    self._violation(
                        "identity drift in feature stream id=%s: expected=%s observed=%s"
                        % (fr.id, baseline, ident)
                    )
            yield fr
