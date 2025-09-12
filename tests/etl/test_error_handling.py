import pandas as pd
import pytest
from pathlib import Path
from pydantic import ValidationError

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

@pytest.fixture
def malformed_excel_file(tmp_path: Path) -> Path:
    """Creates a malformed EMA data file with a missing required column."""
    file_path = tmp_path / "malformed_ema_data.xlsx"
    data = {
        "Category": ["Human"],
        "Medicine name": ["TestMed A"],
        "Therapeutic area": ["Oncology"],
        # "Active substance" is missing
        "Product number": ["EMA/1"],
        "Patient safety": [None],
        "authorization_status": ["Authorised"],
        "ATC code": ["L01"],
        "Additional monitoring": [None],
        "Generic": [False],
        "Biosimilar": [False],
        "Conditional approval": [None],
        "Exceptional circumstances": [None],
        "Marketing authorisation date": ["2023-01-01"],
        "Revision date": ["2023-01-15"],
        "Marketing authorisation holder/company name": ["PharmaCo"],
        "URL": ["http://example.com/1"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path

def test_malformed_excel_file(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    malformed_excel_file: Path,
):
    """
    Tests that the ETL process fails gracefully when the Excel file is malformed.
    """
    # --- Mock dependencies ---
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)
    mock_download = mocker.patch("py_load_epar.etl.extract.download_file_to_memory")
    mock_download.return_value = malformed_excel_file.open("rb")

    # --- Run ETL and assert it fails ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    # This should not raise an exception, but log a warning and skip the record.
    run_etl(settings)

    # --- Assert that no data was loaded ---
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 0


def test_document_download_failure(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file_for_spor_test: Path, # Reusing a simple, valid file
    caplog,
):
    """
    Tests that if a document download fails, the ETL continues and logs the error,
    but does not fail the entire process.
    """
    # --- Mock dependencies ---
    # Mock the SPOR API to return nothing
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None

    # Mock the download of the source excel file
    mock_download = mocker.patch("py_load_epar.etl.extract.download_file_to_memory")
    mock_download.return_value = sample_excel_file_for_spor_test.open("rb")

    # Mock the HTML fetching to return a page with a PDF link
    pdf_url = "http://example.com/non_existent_document.pdf"
    mocker.patch(
        "py_load_epar.etl.orchestrator._fetch_html_with_retry",
        return_value=f'<html><body><a href="{pdf_url}">Public Assessment Report</a></body></html>'.encode(),
    )

    # Mock the document download itself to raise an exception
    mocker.patch(
        "py_load_epar.etl.orchestrator.download_document_and_hash",
        side_effect=Exception("Simulated download failure (e.g., 404)"),
    )

    # --- Run ETL ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assertions ---
    # Assert that the main record was still loaded
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT epar_id FROM epar_index")
        assert cursor.fetchone()[0] == "EMA/SPOR"

    # Assert that no document was loaded
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_documents")
        assert cursor.fetchone()[0] == 0

    # Assert that the error was logged
        assert "Failed to process document link" in caplog.text
    assert "Simulated download failure" in caplog.text


@pytest.fixture
def sample_excel_file_for_spor_test(tmp_path: Path) -> Path:
    """Creates a sample EMA data file for SPOR API error handling test."""
    file_path = tmp_path / "sample_ema_data_spor.xlsx"
    data = {
        "Category": ["Human"],
        "Medicine name": ["TestMed SPOR"],
        "Therapeutic area": ["Oncology"],
        "Active substance": ["substance_spor"],
        "Product number": ["EMA/SPOR"],
        "Patient safety": [None],
        "authorization_status": ["Authorised"],
        "ATC code": ["L01"],
        "Additional monitoring": [None],
        "Generic": [False],
        "Biosimilar": [False],
        "Conditional approval": [None],
        "Exceptional circumstances": [None],
        "Marketing authorisation date": ["2023-01-01"],
        "Revision date": ["2023-01-15"],
        "Marketing authorisation holder/company name": ["PharmaCo SPOR"],
        "URL": ["http://example.com/spor"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_spor_api_error_handling(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file_for_spor_test: Path,
):
    """
    Tests that the ETL process handles SPOR API errors gracefully.
    """
    # --- Mock dependencies ---
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.side_effect = Exception("SPOR API is down")
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)
    mock_download = mocker.patch("py_load_epar.etl.extract.download_file_to_memory")
    mock_download.return_value = sample_excel_file_for_spor_test.open("rb")

    # --- Run ETL ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assert that data was loaded without enrichment ---
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT mah_oms_id FROM epar_index WHERE epar_id = 'EMA/SPOR'")
        assert cursor.fetchone()[0] is None


@pytest.fixture
def invalid_data_type_excel_file(tmp_path: Path) -> Path:
    """Creates a file with an invalid data type in a date column."""
    file_path = tmp_path / "invalid_data_type.xlsx"
    data = {
        "Category": ["Human", "Human"],
        "Medicine name": ["TestMed Valid", "TestMed Invalid Date"],
        "Therapeutic area": ["Oncology", "Cardiology"],
        "Active substance": ["substance_a", "substance_b"],
        "Product number": ["EMA/1", "EMA/2"],
        "Patient safety": [None, None],
        "authorization_status": ["Authorised", "Authorised"],
        "ATC code": ["L01", "C01"],
        "Additional monitoring": [None, None],
        "Generic": [False, True],
        "Biosimilar": [False, False],
        "Conditional approval": [None, None],
        "Exceptional circumstances": [None, None],
        "Marketing authorisation date": ["2023-01-01", "not-a-date"],
        "Revision date": ["2023-01-15", "2023-01-16"],
        "Marketing authorisation holder/company name": ["PharmaCo", "BioGen"],
        "URL": ["http://example.com/1", "http://example.com/2"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_invalid_data_type_in_excel(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    invalid_data_type_excel_file: Path,
    caplog,
):
    """
    Tests that the ETL process handles invalid data types gracefully.
    It should skip the invalid record but continue to process valid ones.
    """
    # --- Mock dependencies ---
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)
    mock_download = mocker.patch("py_load_epar.etl.extract.download_file_to_memory")
    mock_download.return_value = invalid_data_type_excel_file.open("rb")

    # --- Run ETL ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assertions ---
    # Assert that the valid record was loaded
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 1
        cursor.execute("SELECT epar_id FROM epar_index")
        assert cursor.fetchone()[0] == "EMA/1"

    # Assert that a warning was logged for the invalid record
    assert "Could not parse marketing_authorisation_date" in caplog.text
    assert "'not-a-date'. Skipping record." in caplog.text


def test_database_transaction_integrity(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file_for_spor_test: Path, # Reusing a simple, valid file
    caplog,
):
    """
    Tests that if a database error occurs during the load, the transaction
    is rolled back and no data is left in the database.
    """
    # --- Mock dependencies ---
    # Mock the bulk_load_batch method to raise a DB error
    mocker.patch(
        "py_load_epar.db.postgres.PostgresAdapter.bulk_load_batch",
        side_effect=Exception("Simulated database error"),
    )
    # Mock other dependencies to isolate the database logic
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)
    mock_download = mocker.patch("py_load_epar.etl.extract.download_file_to_memory")
    mock_download.return_value = sample_excel_file_for_spor_test.open("rb")

    # --- Run ETL and assert it raises the expected exception ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    with pytest.raises(Exception, match="Simulated database error"):
        run_etl(settings)

    # --- Assertions ---
    # Assert that the error was logged
    assert "ETL run failed: Simulated database error" in caplog.text

    # Assert that the transaction was rolled back and no data was loaded
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 0
