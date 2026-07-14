import pytest

from datapipeline.sources.decoders import CsvDecoder, JsonDecoder, JsonLinesDecoder


def test_json_lines_preserve_records_across_byte_and_character_boundaries() -> None:
    chunks = [
        b'{"name":"And',
        b"ers \xc3",
        b'\xa5"}\r\n\n{"name":"Maja"}\n',
        b'{"name":"Nils"}',
    ]
    decoder = JsonLinesDecoder()

    assert list(decoder.decode(chunks)) == [
        {"name": "Anders \u00e5"},
        {"name": "Maja"},
        {"name": "Nils"},
    ]


def test_json_top_level_null_is_one_value() -> None:
    decoder = JsonDecoder()

    assert list(decoder.decode([b"null"])) == [None]


def test_json_null_array_field_is_empty() -> None:
    decoder = JsonDecoder(array_field="items")

    assert list(decoder.decode([b'{"items": null}'])) == []


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_csv_preserves_newlines_inside_quoted_fields(newline: str) -> None:
    text = f'name;note{newline}Anders \u00e5;"first line{newline}second line"{newline}'
    payload = text.encode("utf-8")
    chunks = [payload[index : index + 1] for index in range(len(payload))]
    decoder = CsvDecoder()

    assert list(decoder.decode(chunks)) == [
        {"name": "Anders \u00e5", "note": f"first line{newline}second line"}
    ]


def test_csv_handles_rows_split_between_chunks_without_a_final_newline() -> None:
    chunks = [b"name;value\r\nAnd", b"ers;1\r\nMaja;2"]
    decoder = CsvDecoder()

    assert list(decoder.decode(chunks)) == [
        {"name": "Anders", "value": "1"},
        {"name": "Maja", "value": "2"},
    ]


def test_csv_error_prefix_is_checked_before_parsing_rows() -> None:
    decoder = CsvDecoder(error_prefixes=("service unavailable",))
    chunks = [b"Service un", b"available\ntry later"]

    with pytest.raises(
        ValueError,
        match="csv response looks like error text: Service unavailable",
    ):
        list(decoder.decode(chunks))


def test_csv_yields_complete_rows_without_reading_a_later_chunk() -> None:
    def chunks():
        yield b"name;value\n"
        yield b"Anders;1\n"
        raise AssertionError("decoder read beyond the requested row")

    rows = CsvDecoder().decode(chunks())

    assert next(rows) == {"name": "Anders", "value": "1"}
    rows.close()
