import uuid
from unittest.mock import MagicMock, call

import pytest
import requests_mock
from py_load_epar.etl.orchestrator import _process_documents
from py_load_epar.models import EparIndex


@pytest.fixture
def mock_db_adapter():
    """Fixture for a mocked database adapter."""
    return MagicMock()


@pytest.fixture
def mock_storage():
    """Fixture for a mocked storage adapter."""
    mock = MagicMock()
    mock.save.return_value = "mock://storage/document.pdf"
    return mock


def test_process_documents_resilient_to_link_text_variations(
    requests_mock, mock_db_adapter, mock_storage, mocker
):
    """
    Tests that the HTML parsing logic can find document links even with
    variations in capitalization and whitespace in the link text.
    """
    # Arrange
    mock_db_adapter.bulk_load_batch.return_value = 3
    download_mock = mocker.patch(
        "py_load_epar.etl.orchestrator.download_document_and_hash",
        return_value=("mock://storage/doc.pdf", "mock_hash"),
    )

    epar_record = EparIndex(
        epar_id="1",
        source_url="http://example.com/epar_summary",
        medicine_name="TestMed",
        authorization_status="Authorised",
        therapeutic_area="Test Area",
        last_update_date_source="2024-01-01",
        etl_execution_id=123,
    )

    html_content = """
    <html><body>
        <a href="doc1.pdf">Public Assessment Report</a>
        <a href="doc2.pdf">   EPAR   </a>
        <a href="doc3.pdf">smpc document</a>
    </body></html>
    """
    requests_mock.get("http://example.com/epar_summary", text=html_content)

    # Act
    count = _process_documents(
        adapter=mock_db_adapter,
        processed_records=[epar_record],
        storage=mock_storage,
    )

    # Assert
    assert count == 3
    download_mock.assert_has_calls(
        [
            call(url="http://example.com/doc1.pdf", storage=mock_storage),
            call(url="http://example.com/doc2.pdf", storage=mock_storage),
            call(url="http://example.com/doc3.pdf", storage=mock_storage),
        ],
        any_order=True,
    )
    mock_db_adapter.bulk_load_batch.assert_called_once()


def test_process_documents_handles_no_pdf_links(
    requests_mock, mock_db_adapter, mock_storage, caplog
):
    """
    Tests that the function handles cases where the HTML page contains no
    links to PDF documents and logs a warning.
    """
    # Arrange
    mock_db_adapter.bulk_load_batch.return_value = 0

    epar_record = EparIndex(
        epar_id="2",
        source_url="http://example.com/no_pdfs",
        medicine_name="TestMed 2",
        authorization_status="Authorised",
        therapeutic_area="Test Area",
        last_update_date_source="2024-01-01",
        etl_execution_id=123,
    )

    html_content = """
    <html><body>
        <a href="document.html">Some other document</a>
        <p>This page has no PDFs.</p>
    </body></html>
    """
    requests_mock.get("http://example.com/no_pdfs", text=html_content)

    # Act
    count = _process_documents(
        adapter=mock_db_adapter,
        processed_records=[epar_record],
        storage=mock_storage,
    )

    # Assert
    assert count == 0
    mock_db_adapter.bulk_load_batch.assert_not_called()
    assert "Could not find any downloadable PDF documents on page" in caplog.text


def test_process_documents_handles_download_error(
    requests_mock, mock_db_adapter, mock_storage, mocker, caplog
):
    """
    Tests that if one document download fails, the process logs the error
    and continues to process other documents.
    """
    # Arrange
    mock_db_adapter.bulk_load_batch.return_value = 1
    download_mock = mocker.patch(
        "py_load_epar.etl.orchestrator.download_document_and_hash",
    )
    download_mock.side_effect = [
        Exception("Download failed"),
        ("mock://storage/doc2.pdf", "mock_hash_2"),
    ]

    epar_record = EparIndex(
        epar_id="3",
        source_url="http://example.com/multi_docs",
        medicine_name="TestMed 3",
        authorization_status="Authorised",
        therapeutic_area="Test Area",
        last_update_date_source="2024-01-01",
        etl_execution_id=123,
    )

    html_content = """
    <html><body>
        <a href="failing_doc.pdf">Failing Document (EPAR)</a>
        <a href="working_doc.pdf">Working Document (EPAR)</a>
    </body></html>
    """
    requests_mock.get("http://example.com/multi_docs", text=html_content)

    # Act
    count = _process_documents(
        adapter=mock_db_adapter,
        processed_records=[epar_record],
        storage=mock_storage,
    )

    # Assert
    assert count == 1
    assert "Failed to process document link" in caplog.text
    assert "Download failed" in caplog.text
    assert download_mock.call_count == 2
    mock_db_adapter.bulk_load_batch.assert_called_once()


def test_process_documents_handles_multiple_documents(
    requests_mock, mock_db_adapter, mock_storage, mocker
):
    """
    Tests that if the HTML page for a single EPAR record contains multiple
    valid document links, all of them are found and processed.
    """
    # Arrange
    mock_db_adapter.bulk_load_batch.return_value = 4
    download_mock = mocker.patch(
        "py_load_epar.etl.orchestrator.download_document_and_hash",
        return_value=("mock://storage/doc.pdf", "mock_hash"),
    )

    epar_record = EparIndex(
        epar_id="4",
        source_url="http://example.com/many_docs",
        medicine_name="TestMed 4",
        authorization_status="Authorised",
        therapeutic_area="Test Area",
        last_update_date_source="2024-01-01",
        etl_execution_id=123,
    )

    html_content = """
    <html><body>
        <a href="doc1.pdf">Public Assessment Report</a>
        <a href="doc2.pdf">EPAR - Scientific Discussion</a>
        <a href="doc3.pdf">SMPC - Product Information</a>
        <a href="doc4.pdf">Package Leaflet</a>
        <a href="not_a_doc.html">An HTML link</a>
    </body></html>
    """
    requests_mock.get("http://example.com/many_docs", text=html_content)

    # Act
    count = _process_documents(
        adapter=mock_db_adapter,
        processed_records=[epar_record],
        storage=mock_storage,
    )

    # Assert
    assert count == 4
    assert download_mock.call_count == 4
    download_mock.assert_has_calls(
        [
            call(url="http://example.com/doc1.pdf", storage=mock_storage),
            call(url="http://example.com/doc2.pdf", storage=mock_storage),
            call(url="http://example.com/doc3.pdf", storage=mock_storage),
            call(url="http://example.com/doc4.pdf", storage=mock_storage),
        ],
        any_order=True,
    )
    mock_db_adapter.bulk_load_batch.assert_called_once()
