import datetime
import uuid
from pathlib import Path
from unittest.mock import MagicMock, call

import requests

from py_load_epar.etl.orchestrator import _process_documents
from py_load_epar.models import EparDocument, EparIndex
from py_load_epar.storage.interfaces import IStorage


def test_process_documents_parses_html_and_downloads(mocker):
    """
    Tests that _process_documents correctly fetches an HTML page, parses it,
    finds a relevant PDF link, and processes it.
    """
    # Arrange
    mock_adapter = MagicMock()
    mock_adapter.bulk_load_batch.return_value = 1
    mock_storage = MagicMock(spec=IStorage)

    # Mock the response from requests.get
    mock_response = mocker.patch("requests.get")
    mock_response.return_value.status_code = 200
    mock_response.return_value.raise_for_status.return_value = None

    # This is the fake HTML page content we will return
    epar_page_url = "https://www.ema.europa.eu/en/medicines/human/EPAR/sample-medicine"
    pdf_relative_path = "/documents/assessment-report/sample-medicine_en.pdf"
    pdf_full_url = f"https://www.ema.europa.eu{pdf_relative_path}"
    mock_response.return_value.content = f"""
    <html>
        <body>
            <h1>Sample Medicine</h1>
            <a href="/some/other/link.html">Some other link</a>
            <p>Here is the assessment report.</p>
            <a href="{pdf_relative_path}">
                EPAR - Public assessment report
            </a>
            <a href="/another/doc.pdf">Another PDF</a>
        </body>
    </html>
    """.encode("utf-8")

    # Mock the actual download function so we don't download anything
    mock_download = mocker.patch(
        "py_load_epar.etl.orchestrator.download_document_and_hash"
    )
    fake_storage_uri = "file:///tmp/fake_storage/downloaded.pdf"
    fake_hash = "fake_sha256_hash"
    mock_download.return_value = (fake_storage_uri, fake_hash)

    # Create a sample EPAR record
    record = EparIndex(
        epar_id="test_epar_001",
        medicine_name="Sample Medicine",
        authorization_status="Authorised",
        last_update_date_source=datetime.date(2024, 1, 1),
        source_url=epar_page_url,
    )
    processed_records = [record]

    # Act
    result_count = _process_documents(
        adapter=mock_adapter, processed_records=processed_records, storage=mock_storage
    )

    # Assert
    # 1. Assert that the correct number of documents were processed
    assert result_count == 1

    # 2. Assert that the EPAR page was fetched
    mock_response.assert_called_once_with(epar_page_url, timeout=30)

    # 3. Assert that the document was "downloaded" with the correct full URL
    mock_download.assert_called_once_with(url=pdf_full_url, storage=mock_storage)

    # 4. Assert that the database load was prepared and finalized correctly
    mock_adapter.prepare_load.assert_called_once_with("DELTA", "epar_documents")
    mock_adapter.finalize.assert_called_once()

    # 5. Assert that the bulk load was called with the correct data
    mock_adapter.bulk_load_batch.assert_called_once()
    call_args, call_kwargs = mock_adapter.bulk_load_batch.call_args

    # Check the iterator and the columns passed
    loaded_data_iterator = call_args[0]
    columns = call_args[2]
    loaded_data = list(loaded_data_iterator)
    assert len(loaded_data) == 1

    # The data is now a tuple, not a pydantic model instance
    loaded_doc_tuple = loaded_data[0]
    assert isinstance(loaded_doc_tuple, tuple)

    # Create a dict from the tuple and columns to easily check values
    doc_dict = dict(zip(columns, loaded_doc_tuple))

    assert doc_dict["epar_id"] == "test_epar_001"
    assert doc_dict["source_url"] == pdf_full_url
    assert doc_dict["storage_location"] == fake_storage_uri
    assert doc_dict["file_hash"] == fake_hash
    assert "public assessment report" in doc_dict["document_type"]
