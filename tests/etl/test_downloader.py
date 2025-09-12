import hashlib
from unittest.mock import MagicMock

import pytest

from py_load_epar.etl.downloader import (
    _download_file_to_stream,
    download_document_and_hash,
)
from py_load_epar.storage.interfaces import IStorage


def test_download_document_and_hash_with_mock_storage(requests_mock):
    """
    Test that a document is downloaded, hashed, and saved via a mock storage adapter.
    """
    url = "https://fake-ema-url.com/document.pdf"
    mock_content = b"some_pdf_content_for_testing"
    requests_mock.get(url, content=mock_content)

    # Create a mock storage object that adheres to the IStorage interface
    mock_storage = MagicMock(spec=IStorage)
    expected_uri = "mock://storage/documents/document.pdf"
    mock_storage.save.return_value = expected_uri

    # Calculate the expected hash
    expected_hash = hashlib.sha256(mock_content).hexdigest()

    # Call the function with the mock storage
    storage_uri, file_hash = download_document_and_hash(url, mock_storage)

    # --- Assertions ---
    # 1. Check the return values are correct
    assert storage_uri == expected_uri
    assert file_hash == expected_hash

    # 2. Check that the 'save' method on the mock storage was called correctly
    mock_storage.save.assert_called_once()

    # 3. Inspect the arguments passed to the 'save' method
    call_args = mock_storage.save.call_args
    saved_stream = call_args.kwargs['data_stream']
    saved_object_name = call_args.kwargs['object_name']

    assert saved_stream.read() == mock_content
    assert saved_object_name == "documents/document.pdf"


import requests


def test_download_raises_exception_on_http_error(requests_mock):
    """Test that the downloader raises an exception for a 404 error."""
    url = "https://fake-ema-url.com/not_found"
    requests_mock.get(url, status_code=404)

    with pytest.raises(Exception):
        # Use a dummy file stream for the test
        from io import BytesIO

        dummy_stream = BytesIO()
        _download_file_to_stream(url, dummy_stream)


def test_download_retries_on_transient_error(requests_mock):
    """
    Test that the downloader retries on a 503 error and eventually succeeds.
    """
    url = "https://fake-ema-url.com/transient_error"
    mock_content = b"successful_content"
    # Simulate a 503 error, then a success
    requests_mock.get(
        url,
        [
            {"status_code": 503, "text": "Service Unavailable"},
            {"status_code": 200, "content": mock_content},
        ],
    )
    from io import BytesIO

    dummy_stream = BytesIO()
    _download_file_to_stream(url, dummy_stream)
    dummy_stream.seek(0)
    assert dummy_stream.read() == mock_content
    assert requests_mock.call_count == 2


def test_download_handles_timeout(requests_mock):
    """
    Test that the downloader handles a requests.Timeout correctly.
    """
    url = "https://fake-ema-url.com/timeout"
    requests_mock.get(url, exc=requests.exceptions.Timeout)

    with pytest.raises(requests.exceptions.Timeout):
        from io import BytesIO

        dummy_stream = BytesIO()
        _download_file_to_stream(url, dummy_stream)


def test_download_with_no_filename_in_url(requests_mock):
    """
    Test that a default filename is used when the URL has no path.
    """
    url = "http://fake-ema-url.com/"
    mock_content = b"some_content"
    requests_mock.get(url, content=mock_content)

    mock_storage = MagicMock(spec=IStorage)
    mock_storage.save.return_value = "mock://storage/documents/downloaded_document"

    download_document_and_hash(url, mock_storage)

    mock_storage.save.assert_called_once()
    call_args = mock_storage.save.call_args
    saved_object_name = call_args.kwargs["object_name"]
    assert saved_object_name == "documents/downloaded_document"
