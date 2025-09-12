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
