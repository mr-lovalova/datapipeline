from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Iterator, Dict, Any, Optional
import csv
import json
from io import StringIO


class Decoder(ABC):
    @abstractmethod
    def decode(self, lines: Iterable[str]) -> Iterator[Dict[str, Any]]:
        pass

    def count(self, lines: Iterable[str]) -> Optional[int]:
        """Optional fast count of rows for the given stream.

        Default returns None. Subclasses may override for better visuals.
        Note: This will consume the provided iterable.
        """
        return None


class CsvDecoder(Decoder):
    def __init__(self, *, delimiter: str = ";"):
        self.delimiter = delimiter

    def decode(self, lines: Iterable[str]) -> Iterator[Dict[str, Any]]:
        # Stream directly from the line iterator; no buffering
        reader = csv.DictReader(lines, delimiter=self.delimiter)
        for row in reader:
            yield row

    def count(self, lines: Iterable[str]) -> Optional[int]:
        return sum(1 for _ in csv.DictReader(lines, delimiter=self.delimiter))


class JsonDecoder(Decoder):
    def decode(self, lines: Iterable[str]) -> Iterator[Dict[str, Any]]:
        text = "\n".join(lines)
        data = json.loads(text)
        if isinstance(data, list):
            for item in data:
                yield item
        else:
            # Yield a single object as one row
            yield data

    def count(self, lines: Iterable[str]) -> Optional[int]:
        text = "\n".join(lines)
        data = json.loads(text)
        return len(data) if isinstance(data, list) else 1


class JsonLinesDecoder(Decoder):
    def decode(self, lines: Iterable[str]) -> Iterator[Dict[str, Any]]:
        for line in lines:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)

    def count(self, lines: Iterable[str]) -> Optional[int]:
        return sum(1 for s in lines if s.strip())
