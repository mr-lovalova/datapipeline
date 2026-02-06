from abc import ABC, abstractmethod
from typing import Iterable, Iterator, Any, Optional, Sequence
import codecs
import csv
import io
import json
import pickle
import itertools


class Decoder(ABC):
    @abstractmethod
    def decode(self, chunks: Iterable[bytes]) -> Iterator[Any]:
        pass

    def count(self, chunks: Iterable[bytes]) -> Optional[int]:
        """Optional fast count of rows for the given stream.

        Default returns None. Subclasses may override for better visuals.
        Note: This will consume the provided iterable.
        """
        return None


def _iter_text_lines(chunks: Iterable[bytes], encoding: str) -> Iterator[str]:
    decoder = codecs.getincrementaldecoder(encoding)()
    buffer = ""
    for chunk in chunks:
        buffer += decoder.decode(chunk)
        while True:
            idx = buffer.find("\n")
            if idx == -1:
                break
            line, buffer = buffer[:idx], buffer[idx + 1:]
            if line.endswith("\r"):
                line = line[:-1]
            yield line
    buffer += decoder.decode(b"", final=True)
    if buffer:
        if buffer.endswith("\r"):
            buffer = buffer[:-1]
        yield buffer


def _read_all_text(chunks: Iterable[bytes], encoding: str) -> str:
    decoder = codecs.getincrementaldecoder(encoding)()
    parts: list[str] = []
    for chunk in chunks:
        parts.append(decoder.decode(chunk))
    parts.append(decoder.decode(b"", final=True))
    return "".join(parts)


class CsvDecoder(Decoder):
    def __init__(
        self,
        *,
        delimiter: str = ";",
        encoding: str = "utf-8",
        error_prefixes: Optional[Sequence[str]] = None,
    ):
        self.delimiter = delimiter
        self.encoding = encoding
        self._error_prefixes = [p.lower() for p in (error_prefixes or [])]

    def _iter_lines(self, chunks: Iterable[bytes]) -> Iterator[str]:
        lines = _iter_text_lines(chunks, self.encoding)
        try:
            first = next(lines)
        except StopIteration:
            return iter(())
        if self._error_prefixes:
            lowered = first.lstrip().lower()
            if any(lowered.startswith(p) for p in self._error_prefixes):
                raise ValueError(
                    f"csv response looks like error text: {first[:120]}")
        return itertools.chain([first], lines)

    def decode(self, chunks: Iterable[bytes]) -> Iterator[dict]:
        reader = csv.DictReader(self._iter_lines(
            chunks), delimiter=self.delimiter)
        for row in reader:
            yield row

    def count(self, chunks: Iterable[bytes]) -> Optional[int]:
        return sum(1 for _ in csv.DictReader(self._iter_lines(chunks), delimiter=self.delimiter))


class JsonDecoder(Decoder):
    def __init__(self, *, encoding: str = "utf-8", array_field: Optional[str] = None):
        self.encoding = encoding
        self.array_field = array_field

    def decode(self, chunks: Iterable[bytes]) -> Iterator[Any]:
        text = _read_all_text(chunks, self.encoding)
        data = json.loads(text)
        if self.array_field:
            if not isinstance(data, dict):
                raise ValueError(
                    "json array_field requires a top-level object")
            if self.array_field not in data:
                raise ValueError(
                    f"json array_field missing: {self.array_field}")
            data = data[self.array_field]
            if data is None:
                return  # TODO MAYBE we NEED DO DO SOMETHING ABOUT THIS so we dont silence it
        if isinstance(data, list):
            for item in data:
                yield item
        else:
            # Yield a single object as one row
            yield data

    def count(self, chunks: Iterable[bytes]) -> Optional[int]:
        text = _read_all_text(chunks, self.encoding)
        data = json.loads(text)
        if self.array_field:
            if not isinstance(data, dict):
                raise ValueError(
                    "json array_field requires a top-level object")
            if self.array_field not in data:
                raise ValueError(
                    f"json array_field missing: {self.array_field}")
            data = data[self.array_field]
        return len(data) if isinstance(data, list) else 1


class JsonLinesDecoder(Decoder):
    def __init__(self, *, encoding: str = "utf-8"):
        self.encoding = encoding

    def decode(self, chunks: Iterable[bytes]) -> Iterator[dict]:
        for line in _iter_text_lines(chunks, self.encoding):
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)

    def count(self, chunks: Iterable[bytes]) -> Optional[int]:
        return sum(1 for s in _iter_text_lines(chunks, self.encoding) if s.strip())


class PickleDecoder(Decoder):
    def decode(self, chunks: Iterable[bytes]) -> Iterator[Any]:
        buffer = io.BytesIO()
        for chunk in chunks:
            buffer.write(chunk)
        buffer.seek(0)
        unpickler = pickle.Unpickler(buffer)
        try:
            while True:
                yield unpickler.load()
        except EOFError:
            return

    def count(self, chunks: Iterable[bytes]) -> Optional[int]:
        buffer = io.BytesIO()
        for chunk in chunks:
            buffer.write(chunk)
        buffer.seek(0)
        unpickler = pickle.Unpickler(buffer)
        total = 0
        try:
            while True:
                unpickler.load()
                total += 1
        except EOFError:
            return total
