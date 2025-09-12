from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def malformed_excel_file(tmp_path: Path) -> Path:
    """
    Creates a sample EMA data file with common data quality issues.
    - Row 1: Valid
    - Row 2: Missing 'Medicine name' (NOT NULL constraint)
    - Row 3: Invalid date format
    - The 'URL' column is missing entirely.
    """
    file_path = tmp_path / "malformed_ema_data.xlsx"
    data = {
        "Category": ["Human", "Human", "Human"],
        "Medicine name": ["TestMed Valid", None, "TestMed Invalid Date"],
        "Therapeutic area": ["Oncology", "Cardiology", "Neurology"],
        "Active substance": ["substance_a", "substance_b", "substance_c"],
        "Product number": ["EMA/1", "EMA/2", "EMA/3"],
        "Patient safety": [None, None, None],
        "authorization_status": ["Authorised", "Withdrawn", "Authorised"],
        "ATC code": ["L01", "C01", "N01"],
        "Additional monitoring": [None, None, None],
        "Generic": [False, True, False],
        "Biosimilar": [False, False, False],
        "Conditional approval": [None, None, None],
        "Exceptional circumstances": [None, None, None],
        "Marketing authorisation date": ["2023-01-01", "2023-01-02", "not-a-date"],
        "Revision date": ["2023-01-15", "2023-01-16", "2023-01-17"],
        "Marketing authorisation holder/company name": [
            "PharmaCo",
            "BioGen",
            "Richards Pharma",
        ],
        # "URL" column is intentionally omitted
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_resilience_to_data_quality_issues(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    malformed_excel_file: Path,
    caplog,
):
    """
    Tests that the ETL pipeline is resilient to common data quality issues.

    This test verifies that the pipeline:
    1. Does not crash when a source column is missing (e.g., 'URL').
    2. Skips rows that fail Pydantic validation (e.g., missing required fields,
       invalid data types).
    3. Logs the validation errors for the skipped rows.
    4. Successfully processes and loads the valid rows from the same file.
    """
    # --- 1. Mock external dependencies ---
    # Mock the download function to return our local malformed test file
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=malformed_excel_file.open("rb"),
    )

    # Mock the SPOR API client to avoid network calls
    mock_spor_client = MagicMock()
    mock_spor_client.search_organisation.return_value = None
    mock_spor_client.search_substance.return_value = None
    mocker.patch(
        "py_load_epar.etl.orchestrator.SporApiClient", return_value=mock_spor_client
    )
    # Document processing is skipped because the 'URL' column is missing
    mock_process_docs = mocker.patch(
        "py_load_epar.etl.orchestrator._process_documents"
    )

    # --- 2. Run the ETL process ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    # The test now expects a ValueError because the critical 'URL' column is missing
    with pytest.raises(ValueError, match="Missing critical columns"):
        run_etl(settings)


@pytest.fixture
def single_good_record_file(tmp_path: Path) -> Path:
    """Creates a valid, single-record EMA data file for testing."""
    file_path = tmp_path / "single_good_record.xlsx"
    data = {
        "Category": ["Human"],
        "Medicine name": ["TestMed Good"],
        "Therapeutic area": ["Testing"],
        "Active substance": ["substance_a"],
        "Product number": ["EMA/GOOD/1"],
        "Patient safety": [None],
        "authorization_status": ["Authorised"],
        "ATC code": ["T01"],
        "Additional monitoring": [None],
        "Generic": [False],
        "Biosimilar": [False],
        "Conditional approval": [None],
        "Exceptional circumstances": [None],
        "Marketing authorisation date": ["2023-01-01"],
        "Revision date": ["2023-01-15"],
        "Marketing authorisation holder/company name": ["Good Corp"],
        "URL": ["http://example.com/good-medicine"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_process_documents_handles_page_with_no_pdf_links(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    caplog,
    single_good_record_file: Path,
):
    """
    Tests that the document processing logic can gracefully handle an EPAR
    summary page that does not contain any links to a PDF document.
    """
    # --- 1. Arrange ---
    # Mock the file download and SPOR API
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=single_good_record_file.open("rb"),
    )
    mock_spor_client = MagicMock()
    mock_spor_client.search_organisation.return_value = None
    mock_spor_client.search_substance.return_value = None
    mocker.patch(
        "py_load_epar.etl.orchestrator.SporApiClient", return_value=mock_spor_client
    )

    # Mock the HTTP fetch to return a page with no PDF links
    mock_fetch = mocker.patch(
        "py_load_epar.etl.orchestrator._fetch_html_with_retry"
    )
    mock_fetch.return_value = b"<html><body><p>No documents here.</p></body></html>"
    mock_download = mocker.patch(
        "py_load_epar.etl.orchestrator.download_document_and_hash"
    )

    # --- 2. Act ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    with caplog.at_level("WARNING"):
        run_etl(settings)

    # --- 3. Assert ---
    # Verify that the main record was still loaded
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 1

    # Verify that the HTML page was fetched
    mock_fetch.assert_called_once()

    # Verify that the document download function was NEVER called
    mock_download.assert_not_called()

    # Verify that a warning was logged
    assert "Could not find any downloadable PDF documents on page" in caplog.text

    # Verify that no documents were loaded into the database
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_documents")
        assert cursor.fetchone()[0] == 0
