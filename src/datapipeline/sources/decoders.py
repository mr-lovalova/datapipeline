from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Iterator, Any, Optional
import codecs
import csv
import io
import json
import pickle


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
            line, buffer = buffer[:idx], buffer[idx + 1 :]
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
    def __init__(self, *, delimiter: str = ";", encoding: str = "utf-8"):
        self.delimiter = delimiter
        self.encoding = encoding

    def decode(self, chunks: Iterable[bytes]) -> Iterator[dict]:
        reader = csv.DictReader(_iter_text_lines(chunks, self.encoding), delimiter=self.delimiter)
        for row in reader:
            yield row

    def count(self, chunks: Iterable[bytes]) -> Optional[int]:
        return sum(1 for _ in csv.DictReader(_iter_text_lines(chunks, self.encoding), delimiter=self.delimiter))


class JsonDecoder(Decoder):
    def __init__(self, *, encoding: str = "utf-8"):
        self.encoding = encoding

    def decode(self, chunks: Iterable[bytes]) -> Iterator[Any]:
        text = _read_all_text(chunks, self.encoding)
        data = json.loads(text)
        if isinstance(data, list):
            for item in data:
                yield item
        else:
            # Yield a single object as one row
            yield data

    def count(self, chunks: Iterable[bytes]) -> Optional[int]:
        text = _read_all_text(chunks, self.encoding)
        data = json.loads(text)
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
