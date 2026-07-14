import codecs
import csv
import io
import itertools
import json
import pickle
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from typing import Any


class Decoder(ABC):
    @abstractmethod
    def decode(self, chunks: Iterable[bytes]) -> Iterator[Any]: ...


def _iter_text_lines(chunks: Iterable[bytes], encoding: str) -> Iterator[str]:
    """Yield LF/CRLF physical lines without rewriting their terminators."""

    decoder = codecs.getincrementaldecoder(encoding)()
    fragments: list[str] = []
    for chunk in chunks:
        text = decoder.decode(chunk)
        start = 0
        while (newline := text.find("\n", start)) >= 0:
            line = text[start : newline + 1]
            if fragments:
                fragments.append(line)
                yield "".join(fragments)
                fragments.clear()
            else:
                yield line
            start = newline + 1
        if start < len(text):
            fragments.append(text[start:])

    tail = decoder.decode(b"", final=True)
    if tail:
        fragments.append(tail)
    if fragments:
        yield "".join(fragments)


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
        error_prefixes: Sequence[str] | None = None,
    ) -> None:
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
                    f"csv response looks like error text: {first[:120].rstrip()}"
                )
        return itertools.chain((first,), lines)

    def decode(self, chunks: Iterable[bytes]) -> Iterator[dict]:
        reader = csv.DictReader(self._iter_lines(chunks), delimiter=self.delimiter)
        for row in reader:
            yield row


class JsonDecoder(Decoder):
    def __init__(
        self,
        encoding: str = "utf-8",
        array_field: str | None = None,
    ) -> None:
        self.encoding = encoding
        self.array_field = array_field

    def _load_payload(self, chunks: Iterable[bytes]) -> Any:
        text = _read_all_text(chunks, self.encoding)
        data = json.loads(text)
        if self.array_field is not None:
            if not isinstance(data, dict):
                raise ValueError("json array_field requires a top-level object")
            if self.array_field not in data:
                raise ValueError(f"json array_field missing: {self.array_field}")
            data = data[self.array_field]
        return data

    def decode(self, chunks: Iterable[bytes]) -> Iterator[Any]:
        data = self._load_payload(chunks)
        if data is None and self.array_field is not None:
            return
        if isinstance(data, list):
            yield from data
            return
        yield data


class JsonLinesDecoder(Decoder):
    def __init__(self, encoding: str = "utf-8") -> None:
        self.encoding = encoding

    def decode(self, chunks: Iterable[bytes]) -> Iterator[dict]:
        for line in _iter_text_lines(chunks, self.encoding):
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


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
