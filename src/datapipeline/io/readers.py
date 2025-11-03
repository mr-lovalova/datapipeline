from pathlib import Path
from typing import Iterator, Any, Optional
import json
import gzip
import csv
import pickle


def read_manifest_for(data_path: Path) -> Optional[dict]:
    """Return manifest dict if a sidecar '<file>.manifest.json' exists."""
    m = data_path.with_suffix(data_path.suffix + ".manifest.json")
    if m.exists():
        return json.loads(m.read_text(encoding="utf-8"))
    return None


def iter_jsonl(path: Path) -> Iterator[dict]:
    """Yield JSON objects per line. Skips optional metadata headers when present."""
    with path.open("r", encoding="utf-8") as f:
        # peek first line
        first = f.readline()
        if not first:
            return
        try:
            rec = json.loads(first)
        except json.JSONDecodeError:
            # treat as raw; yield and continue
            yield json.loads(first)
        else:
            if not (isinstance(rec, dict) and "__checkpoint__" in rec):
                yield rec
        for line in f:
            if line.strip():
                yield json.loads(line)


def iter_jsonl_gz(path: Path) -> Iterator[dict]:
    """Yield JSON objects per line from .jsonl.gz. Skips optional metadata headers when present."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        first = f.readline()
        if not first:
            return
        rec = json.loads(first)
        if not (isinstance(rec, dict) and "__checkpoint__" in rec):
            yield rec
        for line in f:
            if line.strip():
                yield json.loads(line)


def iter_csv(path: Path) -> Iterator[dict]:
    """
    Yield dict rows from CSV. Assumes first row is header.
    NOTE: We do NOT inject metadata headers into CSV files; use the sidecar manifest when available.
    """
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def iter_pickle(path: Path) -> Iterator[Any]:
    """
    Yield objects from a pickle stream.
    NOTE: We do NOT inject metadata headers into pickle files; use the sidecar manifest when available.
    """
    with path.open("rb") as f:
        while True:
            try:
                yield pickle.load(f)
            except EOFError:
                break
