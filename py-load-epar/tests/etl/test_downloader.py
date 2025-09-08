import hashlib
from unittest.mock import MagicMock, patch

import pytest
import requests_mock

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


def test_download_raises_exception_on_http_error(requests_mock):
    """Test that the downloader raises an exception for a 404 error."""
    url = "https://fake-ema-url.com/not_found"
    requests_mock.get(url, status_code=404)

    with pytest.raises(Exception):
        # Use a dummy file stream for the test
        from io import BytesIO

        dummy_stream = BytesIO()
        _download_file_to_stream(url, dummy_stream)
