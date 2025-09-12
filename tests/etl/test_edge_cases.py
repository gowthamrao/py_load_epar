# tests/etl/test_edge_cases.py

import pandas as pd
import pytest
from pathlib import Path
import logging
from unittest.mock import MagicMock
from multiprocessing import Process
import time

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

@pytest.fixture
def malformed_excel_file(tmp_path: Path) -> Path:
    """Creates a malformed Excel file with missing required columns."""
    file_path = tmp_path / "malformed_ema_data.xlsx"
    data = {
        "Category": ["Human"],
        "Medicine name": ["TestMed Malformed"],
        # "Therapeutic area" is missing
        "Active substance": ["substance_malformed"],
        "Product number": ["EMA/MALFORMED"],
        "authorization_status": ["Authorised"],
        "URL": ["http://example.com/malformed"],
        "Revision date": ["2023-01-01"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path

def test_etl_with_malformed_data(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    malformed_excel_file: Path,
    caplog,
):
    """
    Tests that the ETL process handles malformed input data gracefully.
    It should log an error and not insert incomplete data into the database.
    """
    # Mock the download function to return our local test file
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=malformed_excel_file.open("rb"),
    )
    mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    settings = db_settings
    settings.etl.load_strategy = "FULL"

    with caplog.at_level(logging.ERROR):
        run_etl(settings)

    # Assert that a validation error was logged for the missing field
    assert "failed validation" in caplog.text
    assert "therapeutic_area" in caplog.text
    assert "Field required" in caplog.text

    # Assert that no data was loaded into the database
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 0

@pytest.fixture
def delta_excel_file_1(tmp_path: Path) -> Path:
    """Creates the initial version of an Excel file for delta load testing."""
    file_path = tmp_path / "delta_data_1.xlsx"
    data = {
        "Category": ["Human", "Human"],
        "Medicine name": ["DeltaMed v1", "StableMed"],
        "Therapeutic area": ["Gastroenterology", "Dermatology"],
        "Active substance": ["substance_delta_1", "substance_stable"],
        "Product number": ["EMA/DELTA/1", "EMA/STABLE/1"],
        "authorization_status": ["Authorised", "Authorised"],
        "Marketing authorisation date": ["2023-01-01", "2023-02-01"],
        "Revision date": ["2023-01-15", "2023-02-15"],
        "URL": ["http://example.com/delta1", "http://example.com/stable"],
    }
    pd.DataFrame(data).to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


@pytest.fixture
def delta_excel_file_2(tmp_path: Path) -> Path:
    """Creates the updated version of an Excel file for delta load testing."""
    file_path = tmp_path / "delta_data_2.xlsx"
    data = {
        "Category": ["Human", "Human", "Human"],
        "Medicine name": ["DeltaMed v2", "StableMed", "NewMed"],
        "Therapeutic area": ["Gastroenterology", "Dermatology", "Cardiology"],
        "Active substance": ["substance_delta_2", "substance_stable", "substance_new"],
        "Product number": ["EMA/DELTA/1", "EMA/STABLE/1", "EMA/NEW/1"],
        "authorization_status": ["Authorised", "Authorised", "Authorised"],
        "Marketing authorisation date": ["2023-01-01", "2023-02-01", "2023-03-01"],
        "Revision date": ["2023-01-20", "2023-02-15", "2023-03-15"], # Revision date updated for DeltaMed
        "URL": ["http://example.com/delta2", "http://example.com/stable", "http://example.com/new"],
    }
    pd.DataFrame(data).to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_delta_load_logic(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    delta_excel_file_1: Path,
    delta_excel_file_2: Path,
):
    """
    Tests the 'DELTA' load strategy.
    1. Runs a FULL load to populate the database.
    2. Runs a DELTA load with a modified file.
    3. Asserts that only the new/updated records are processed.
    """
    # Mock dependencies
    mock_download = mocker.patch("py_load_epar.etl.extract.download_file_to_memory")
    mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # --- 1. Initial FULL load ---
    mock_download.return_value = delta_excel_file_1.open("rb")
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 2
        cursor.execute("SELECT medicine_name FROM epar_index WHERE epar_id = 'EMA/DELTA/1'")
        assert cursor.fetchone()[0] == "DeltaMed v1"

    # --- 2. DELTA load with updated file ---
    mock_download.return_value = delta_excel_file_2.open("rb")
    settings.etl.load_strategy = "DELTA"
    run_etl(settings)

    # --- 3. Assertions ---
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 3 # One new record was added

        # Check that the updated record was modified
        cursor.execute("SELECT medicine_name, source_url FROM epar_index WHERE epar_id = 'EMA/DELTA/1'")
        name, url = cursor.fetchone()
        assert name == "DeltaMed v2"
        assert url == "http://example.com/delta2"

        # Check that the stable record was not modified (e.g., by checking a field that doesn't change)
        cursor.execute("SELECT therapeutic_area FROM epar_index WHERE epar_id = 'EMA/STABLE/1'")
        assert cursor.fetchone()[0] == "Dermatology"

        # Check that the new record was inserted
        cursor.execute("SELECT medicine_name FROM epar_index WHERE epar_id = 'EMA/NEW/1'")
        assert cursor.fetchone()[0] == "NewMed"

def etl_process_runner(settings: Settings, excel_file: Path):
    """A wrapper function to run the ETL process for concurrency testing."""
    # Each process needs its own mocks, especially for external clients.
    from unittest.mock import patch, MagicMock

    # Create a mock instance for the SPOR API client
    mock_spor_client = MagicMock()
    mock_spor_client.search_organisation.return_value = None
    mock_spor_client.search_substance.return_value = None

    with patch("py_load_epar.etl.extract.download_file_to_memory", return_value=excel_file.open("rb")), \
         patch("py_load_epar.etl.orchestrator.SporApiClient", return_value=mock_spor_client), \
         patch("py_load_epar.etl.orchestrator._process_documents", return_value=0):
        try:
            run_etl(settings)
        except Exception as e:
            # Log exceptions to stderr to make them visible in pytest output
            import sys
            print(f"ETL process failed in child process: {e}", file=sys.stderr)
            raise


def test_concurrent_full_loads(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    single_record_excel_file: Path, # Reusing this fixture from another test file
):
    """
    Tests that running two FULL loads concurrently does not corrupt the database.
    The final state should be as if only one process ran, thanks to transaction
    management and UPSERT logic.
    """
    settings = db_settings
    settings.etl.load_strategy = "FULL"

    # Create and start two processes running the ETL
    process1 = Process(target=etl_process_runner, args=(settings, single_record_excel_file))
    process2 = Process(target=etl_process_runner, args=(settings, single_record_excel_file))

    process1.start()
    process2.start()

    process1.join(timeout=60)
    process2.join(timeout=60)

    assert process1.exitcode == 0
    assert process2.exitcode == 0

    # --- Assert final database state ---
    # Give a moment for any final commits to settle
    time.sleep(1)

    with postgres_adapter.conn.cursor() as cursor:
        # Should only be one record in the table
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 1

        # Verify the data is correct
        cursor.execute("SELECT medicine_name FROM epar_index WHERE epar_id = 'EMA/IDEM'")
        assert cursor.fetchone()[0] == "TestMed Idempotent"
