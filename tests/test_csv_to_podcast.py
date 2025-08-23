import sys
import pathlib
import pytest
from unittest.mock import patch, MagicMock

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from tools.csv_to_podcast import build_item


def _mock_response(headers):
    resp = MagicMock()
    resp.headers = headers
    resp.raise_for_status = lambda: None
    return resp


@patch("tools.csv_to_podcast.requests.head")
def test_build_item_includes_content_length(mock_head):
    headers = {
        "Content-Type": "audio/mpeg",
        "Content-Length": "321"
    }
    mock_head.return_value = _mock_response(headers)
    row = {"Book_Title": "T", "FullBook_MP3_URL": "http://example.com/a.mp3"}
    item = build_item(row, "Wed, 01 Jan 2024 00:00:00 +0000")
    assert 'enclosure url="http://example.com/a.mp3" length="321" type="audio/mpeg"' in item


@patch("tools.csv_to_podcast.requests.head")
def test_accept_ranges_not_bytes_is_ignored(mock_head):
    headers = {
        "Content-Type": "audio/mpeg",
        "Accept-Ranges": "none",
        "Content-Length": "77",
    }
    mock_head.return_value = _mock_response(headers)
    row = {"Book_Title": "T", "FullBook_MP3_URL": "http://example.com/a.mp3"}
    item = build_item(row, "Wed, 01 Jan 2024 00:00:00 +0000")
    assert 'length="77"' in item


@patch("tools.csv_to_podcast.requests.head")
def test_missing_content_length_is_skipped(mock_head, capsys):
    headers = {"Content-Type": "audio/mpeg"}  # missing Content-Length
    mock_head.return_value = _mock_response(headers)
    row = {"Book_Title": "T", "FullBook_MP3_URL": "http://example.com/a.mp3"}
    item = build_item(row, "Wed, 01 Jan 2024 00:00:00 +0000")
    assert item is None
    out = capsys.readouterr().out
    assert "Missing Content-Length" in out


@patch("tools.csv_to_podcast.requests.head")
def test_build_item_contains_translated_fields(mock_head):
    headers = {"Content-Type": "audio/mpeg", "Content-Length": "1"}
    mock_head.return_value = _mock_response(headers)
    row = {
        "Book_Title": "عنوانی",
        "Book_Description": "توضیحی",
        "Book_Detail": "جزئیاتی",
        "FullBook_MP3_URL": "http://example.com/a.mp3",
    }
    item = build_item(row, "Wed, 01 Jan 2024 00:00:00 +0000")
    assert "عنوان کتاب: عنوانی" in item
    assert "توضیحات کتاب: توضیحی" in item
    assert "جزئیات/متن کامل معرفی: جزئیاتی" in item


@patch("tools.csv_to_podcast.requests.head")
def test_long_description_trims_optional_fields(mock_head):
    headers = {"Content-Type": "audio/mpeg", "Content-Length": "1"}
    mock_head.return_value = _mock_response(headers)
    import tools.csv_to_podcast as mod
    original_limit = mod.MAX_DESC_LENGTH
    mod.MAX_DESC_LENGTH = 80
    row = {
        "Book_Title": "T",
        "Book_Description": "D",
        "Book_Detail": "DETAIL",
        "Book_Language": "FA",
        "Book_Country": "IR",
        "FullBook_MP3_URL": "http://example.com/a.mp3",
    }
    try:
        item = build_item(row, "Wed, 01 Jan 2024 00:00:00 +0000")
    finally:
        mod.MAX_DESC_LENGTH = original_limit
    assert "زبان کتاب" not in item
    assert "کشور (منشأ یا مخاطب)" not in item


@patch("tools.csv_to_podcast.requests.head")
def test_invalid_content_type_is_skipped(mock_head, capsys):
    headers = {"Content-Type": "text/html", "Content-Length": "123"}
    mock_head.return_value = _mock_response(headers)
    row = {"Book_Title": "T", "FullBook_MP3_URL": "http://example.com/a.mp3"}
    item = build_item(row, "Wed, 01 Jan 2024 00:00:00 +0000")
    assert item is None
    out = capsys.readouterr().out
    assert "Invalid Content-Type" in out
