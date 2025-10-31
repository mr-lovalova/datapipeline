from typing import Protocol, Callable, Optional
from pathlib import Path
import sys
import json
import pickle
import tempfile
import os
import csv
import gzip


class Writer(Protocol):
    def write(self, rec: dict) -> None: ...
    def close(self) -> None: ...


class TextLineWriter:
    def __init__(self, formatter: Callable[[dict], str], stream=None):
        self.formatter = formatter
        self.stream = stream or sys.stdout

    def write(self, rec: dict) -> None:
        print(self.formatter(rec), file=self.stream)

    def close(self) -> None:
        self.stream.flush()


def JsonLinesWriter():
    return TextLineWriter(lambda rec: json.dumps(rec, default=str))


def PrintWriter():
    return TextLineWriter(lambda rec: f"group={rec['key']}: {rec['values']}")


class PickleWriter:
    def __init__(self, destination: Path, protocol: int = pickle.HIGHEST_PROTOCOL):
        self.dest = destination
        self.protocol = protocol
        self.tmp_path: Optional[Path] = None
        self._fh = None
        self._pickler = None
        self._open_tmp()

    def _open_tmp(self):
        self.dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(
            dir=str(self.dest.parent), delete=False)
        self.tmp_path = Path(tmp.name)
        self._fh = tmp
        self._pickler = pickle.Pickler(self._fh, protocol=self.protocol)

    def write(self, rec: dict) -> None:
        self._pickler.dump((rec["key"], rec["values"]))

    def close(self) -> None:
        self._fh.close()
        os.replace(self.tmp_path, self.dest)


class CSVWriter:
    def __init__(self, destination: Path):
        self.dest = destination
        self.tmp_path: Optional[Path] = None
        self._fh = None
        self._writer = None
        self._open_tmp()

    def _open_tmp(self):
        self.dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(
            dir=str(self.dest.parent), delete=False, mode="w", newline="")
        self.tmp_path = Path(tmp.name)
        self._fh = tmp
        self._writer = csv.writer(self._fh)
        self._writer.writerow(["key", "values"])  # header

    def _format_field(self, value):
        if value is None:
            return ""
        if isinstance(value, (int, float, bool)):
            return value
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            return value
        return str(value)

    def write(self, rec: dict) -> None:
        key = rec["key"]
        values = rec["values"]
        self._writer.writerow(
            [self._format_field(key), self._format_field(values)])

    def close(self) -> None:
        self._fh.close()
        os.replace(self.tmp_path, self.dest)


class GzipJsonLinesWriter:
    def __init__(self, destination: Path):
        self.dest = destination
        self.tmp_path: Optional[Path] = None
        self._fh = None
        self._open_tmp()

    def _open_tmp(self):
        self.dest.parent.mkdir(parents=True, exist_ok=True)
        # binary write, text wrapper for newline handling
        tmp = tempfile.NamedTemporaryFile(
            dir=str(self.dest.parent), delete=False)
        self.tmp_path = Path(tmp.name)
        self._fh = gzip.GzipFile(filename="", mode="wb", fileobj=tmp)

    def write(self, rec: dict) -> None:
        line = json.dumps(rec, default=str).encode("utf-8") + b"\n"
        self._fh.write(line)

    def close(self) -> None:
        self._fh.close()
        os.replace(self.tmp_path, self.dest)


def writer_factory(output: Optional[str]) -> Writer:
    if output and output.lower().endswith(".pt"):
        return PickleWriter(Path(output))
    if output and output.lower().endswith(".csv"):
        return CSVWriter(Path(output))
    if output and (output.lower().endswith(".jsonl.gz") or output.lower().endswith(".gz")):
        return GzipJsonLinesWriter(Path(output))
    mode = (output or "print").lower()
    if mode == "print":
        return PrintWriter()
    if mode == "stream":
        return JsonLinesWriter()
    print("Error: unsupported output format. Use 'print', 'stream', '.csv', '.jsonl.gz', or a .pt file path.", file=sys.stderr)
    raise SystemExit(2)
