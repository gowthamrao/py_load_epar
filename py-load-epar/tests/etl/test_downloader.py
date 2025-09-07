import hashlib
from pathlib import Path
from unittest.mock import patch

import pytest
import requests_mock

from py_load_epar.etl.downloader import (
    _download_file_to_stream,
    download_document_and_hash,
    download_excel_file,
)


def test_download_excel_file(requests_mock, tmp_path: Path):
    """Test that the Excel file is downloaded correctly."""
    url = "https://fake-ema-url.com/data.xlsx"
    mock_content = b"excel_file_content"
    requests_mock.get(url, content=mock_content)

    download_path = download_excel_file(url, destination_folder=tmp_path)

    assert download_path.exists()
    assert download_path.read_bytes() == mock_content
    assert download_path.name == "ema_data.xlsx"


def test_download_document_and_hash(requests_mock, tmp_path: Path):
    """Test that a document is downloaded and its hash is calculated correctly."""
    url = "https://fake-ema-url.com/document.pdf"
    mock_content = b"some_pdf_content"
    requests_mock.get(url, content=mock_content)

    expected_hash = hashlib.sha256(mock_content).hexdigest()

    storage_path, file_hash = download_document_and_hash(url, tmp_path)

    assert storage_path.exists()
    assert storage_path.read_bytes() == mock_content
    assert file_hash == expected_hash
    assert storage_path.name == "document.pdf"


def test_download_raises_exception_on_http_error(requests_mock):
    """Test that the downloader raises an exception for a 404 error."""
    url = "https://fake-ema-url.com/not_found"
    requests_mock.get(url, status_code=404)

    with pytest.raises(Exception):
        # Use a dummy file stream for the test
        from io import BytesIO

        dummy_stream = BytesIO()
        _download_file_to_stream(url, dummy_stream)
