from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Type

from datapipeline.domain.sample import Sample


class BaseSerializer:
    payload_mode = "sample"

    def serialize_payload(self, sample: Sample) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError


class BaseJsonLineSerializer(BaseSerializer):
    def __call__(self, sample: Sample) -> str:
        data = self.serialize_payload(sample)
        return json.dumps(data, ensure_ascii=False, default=str) + "\n"


class SampleJsonLineSerializer(BaseJsonLineSerializer):
    payload_mode = "sample"

    def serialize_payload(self, sample: Sample) -> Any:
        return sample.as_full_payload()


class VectorJsonLineSerializer(BaseJsonLineSerializer):
    payload_mode = "vector"

    def serialize_payload(self, sample: Sample) -> Any:
        return sample.as_vector_payload()


class BasePrintSerializer(BaseSerializer):
    def __call__(self, sample: Sample) -> str:
        value = self.serialize_payload(sample)
        return f"{value}\n"


class SamplePrintSerializer(BasePrintSerializer):
    payload_mode = "sample"

    def serialize_payload(self, sample: Sample) -> Any:
        return sample.as_full_payload()


class VectorPrintSerializer(BasePrintSerializer):
    payload_mode = "vector"

    def serialize_payload(self, sample: Sample) -> Any:
        return sample.as_vector_payload()


class BaseCsvRowSerializer(BaseSerializer):
    def __call__(self, sample: Sample) -> list[str | Any]:
        key_value = sample.key
        if isinstance(key_value, tuple):
            key_struct = list(key_value)
        else:
            key_struct = key_value
        if isinstance(key_struct, (list, dict)):
            key_text = json.dumps(key_struct, ensure_ascii=False, default=str)
        else:
            key_text = "" if key_struct is None else str(key_struct)

        payload_data = self.serialize_payload(sample)
        if isinstance(payload_data, dict):
            payload_data.pop("key", None)
        payload_text = json.dumps(payload_data, ensure_ascii=False, default=str)
        return [key_text, payload_text]


class SampleCsvRowSerializer(BaseCsvRowSerializer):
    payload_mode = "sample"

    def serialize_payload(self, sample: Sample) -> Any:
        return sample.as_full_payload()


class VectorCsvRowSerializer(BaseCsvRowSerializer):
    payload_mode = "vector"

    def serialize_payload(self, sample: Sample) -> Any:
        return sample.as_vector_payload()


class BasePickleSerializer(BaseSerializer):
    def __call__(self, sample: Sample) -> Any:
        return self.serialize_payload(sample)


class SamplePickleSerializer(BasePickleSerializer):
    payload_mode = "sample"

    def serialize_payload(self, sample: Sample) -> Any:
        return sample


class VectorPickleSerializer(BasePickleSerializer):
    payload_mode = "vector"

    def serialize_payload(self, sample: Sample) -> Any:
        return sample.as_vector_payload()


def _record_payload(value: Any) -> Any:
    if value is None:
        return None
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return value
    attrs = getattr(value, "__dict__", None)
    if attrs:
        return {
            k: v
            for k, v in attrs.items()
            if not k.startswith("_")
        }
    return value


def _record_key(value: Any) -> Any:
    direct = getattr(value, "time", None)
    if direct is not None:
        return direct
    record = getattr(value, "record", None)
    if record is not None:
        return getattr(record, "time", None)
    return None


class RecordJsonLineSerializer:
    def __call__(self, record: Any) -> str:
        payload = _record_payload(record)
        return json.dumps(payload, ensure_ascii=False, default=str) + "\n"


class RecordPrintSerializer:
    def __call__(self, record: Any) -> str:
        return f"{_record_payload(record)}\n"


class RecordCsvRowSerializer:
    def __call__(self, record: Any) -> list[str | Any]:
        key_value = _record_key(record)
        key_text = "" if key_value is None else str(key_value)
        payload = json.dumps(_record_payload(record), ensure_ascii=False, default=str)
        return [key_text, payload]


class RecordPickleSerializer:
    def __call__(self, record: Any) -> Any:
        return record


def _serializer_factory(
    registry: Dict[str, Type[BaseSerializer]],
    payload: str,
    default_cls: Type[BaseSerializer],
) -> BaseSerializer:
    cls = registry.get(payload, default_cls)
    return cls()


JSON_SERIALIZERS: Dict[str, Type[BaseJsonLineSerializer]] = {
    SampleJsonLineSerializer.payload_mode: SampleJsonLineSerializer,
    VectorJsonLineSerializer.payload_mode: VectorJsonLineSerializer,
}

PRINT_SERIALIZERS: Dict[str, Type[BasePrintSerializer]] = {
    SamplePrintSerializer.payload_mode: SamplePrintSerializer,
    VectorPrintSerializer.payload_mode: VectorPrintSerializer,
}

CSV_SERIALIZERS: Dict[str, Type[BaseCsvRowSerializer]] = {
    SampleCsvRowSerializer.payload_mode: SampleCsvRowSerializer,
    VectorCsvRowSerializer.payload_mode: VectorCsvRowSerializer,
}

PICKLE_SERIALIZERS: Dict[str, Type[BasePickleSerializer]] = {
    SamplePickleSerializer.payload_mode: SamplePickleSerializer,
    VectorPickleSerializer.payload_mode: VectorPickleSerializer,
}


def json_line_serializer(payload: str) -> BaseJsonLineSerializer:
    return _serializer_factory(JSON_SERIALIZERS, payload, SampleJsonLineSerializer)


def print_serializer(payload: str) -> BasePrintSerializer:
    return _serializer_factory(PRINT_SERIALIZERS, payload, SamplePrintSerializer)


def csv_row_serializer(payload: str) -> BaseCsvRowSerializer:
    return _serializer_factory(CSV_SERIALIZERS, payload, SampleCsvRowSerializer)


def pickle_serializer(payload: str) -> BasePickleSerializer:
    return _serializer_factory(PICKLE_SERIALIZERS, payload, SamplePickleSerializer)


def record_json_line_serializer() -> RecordJsonLineSerializer:
    return RecordJsonLineSerializer()


def record_print_serializer() -> RecordPrintSerializer:
    return RecordPrintSerializer()


def record_csv_row_serializer() -> RecordCsvRowSerializer:
    return RecordCsvRowSerializer()


def record_pickle_serializer() -> RecordPickleSerializer:
    return RecordPickleSerializer()
