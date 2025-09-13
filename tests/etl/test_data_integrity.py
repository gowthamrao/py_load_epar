# tests/etl/test_data_integrity.py
import logging
from pathlib import Path

import pandas as pd
import pytest
import psycopg2

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def sample_excel_file_with_invalid_date(tmp_path: Path) -> Path:
    """
    Creates a sample EMA data file where one record has an invalid date format.
    """
    file_path = tmp_path / "test_ema_data_invalid_date.xlsx"
    data = {
        "Category": ["Human", "Human", "Human"],
        "Medicine name": ["TestMed Valid 1", "TestMed Invalid Date", "TestMed Valid 2"],
        "Therapeutic area": ["Oncology", "Cardiology", "Neurology"],
        "Active substance": ["substance_a", "substance_b", "substance_c"],
        "Product number": ["EMA/VALID/1", "EMA/INVALID/1", "EMA/VALID/2"],
        "Patient safety": [None, None, None],
        "authorization_status": ["Authorised", "Authorised", "Authorised"],
        "ATC code": ["L01", "C01", "N01"],
        "Additional monitoring": [None, None, None],
        "Generic": [False, True, False],
        "Biosimilar": [False, False, False],
        "Conditional approval": [None, None, None],
        "Exceptional circumstances": [None, None, None],
        "Marketing authorisation date": [
            "2023-01-01",
            "NOT A DATE",  # Invalid date format
            "2023-01-03",
        ],
        "Revision date": ["2023-01-15", "2023-01-16", "2023-01-17"],
        "Marketing authorisation holder/company name": [
            "PharmaCo",
            "BioGen",
            "NeuroCorp",
        ],
        "URL": [
            "http://example.com/valid1",
            "http://example.com/invalid1",
            "http://example.com/valid2",
        ],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_with_invalid_data_type(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file_with_invalid_date: Path,
    caplog,
):
    """
    Tests that the ETL pipeline correctly handles records with data type errors
    at the extraction stage.
    It should reject the invalid record but continue to process valid ones.
    """
    # --- Mock dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=sample_excel_file_with_invalid_date.open("rb"),
    )
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # --- Run the ETL process ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    with caplog.at_level(logging.WARNING):
        run_etl(settings)

    # --- Assertions ---
    # Verify that the correct warning was logged during the extraction phase
    assert "Could not parse marketing_authorisation_date" in caplog.text
    assert "'NOT A DATE'" in caplog.text
    assert "Skipping record" in caplog.text

    # Verify that only the valid records were loaded
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 2

        # Verify that the invalid record is not in the database
        cursor.execute(
            "SELECT COUNT(*) FROM epar_index WHERE epar_id = 'EMA/INVALID/1'"
        )
        assert cursor.fetchone()[0] == 0

        # Verify that the valid records are in the database
        cursor.execute("SELECT epar_id FROM epar_index ORDER BY epar_id")
        loaded_ids = [row[0] for row in cursor.fetchall()]
        assert loaded_ids == ["EMA/VALID/1", "EMA/VALID/2"]


@pytest.fixture
def valid_excel_file(tmp_path: Path) -> Path:
    """Creates a sample EMA data file with only valid records."""
    file_path = tmp_path / "valid_ema_data.xlsx"
    data = {
        "Category": ["Human", "Human"],
        "Medicine name": ["TestMed A", "TestMed B"],
        "Therapeutic area": ["Oncology", "Cardiology"],
        "Active substance": ["substance_a", "substance_b"],
        "Product number": ["EMA/A/1", "EMA/B/1"],
        "authorization_status": ["Authorised", "Authorised"],
        "Marketing authorisation date": ["2023-01-01", "2023-01-02"],
        "Revision date": ["2023-01-15", "2023-01-16"],
        "URL": ["http://example.com/a", "http://example.com/b"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_transaction_rollback_on_failure(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    valid_excel_file: Path,
    caplog,
):
    """
    Tests that if the bulk load process fails, the entire transaction is
    rolled back, leaving the database in its initial state.
    """
    # --- Mock dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=valid_excel_file.open("rb"),
    )
    mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # --- Introduce a failure during the bulk_load_batch method ---
    # We patch the adapter instance that will be used by the ETL
    mocker.patch.object(
        postgres_adapter,
        "bulk_load_batch",
        side_effect=psycopg2.Error("Simulating a critical DB error during bulk load"),
    )
    # And we patch the factory to return our patched adapter
    mocker.patch(
        "py_load_epar.etl.orchestrator.get_db_adapter", return_value=postgres_adapter
    )


    # --- Run the ETL process ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"

    with caplog.at_level(logging.INFO):
        # The exception should be caught, logged, and re-raised by the orchestrator
        with pytest.raises(psycopg2.Error, match="Simulating a critical DB error"):
            run_etl(settings)

    # --- Assertions ---
    # Verify that the failure and rollback were logged
    assert "ETL run failed" in caplog.text
    assert "Simulating a critical DB error" in caplog.text
    assert "Rolling back transaction" in caplog.text
    assert "Failure logged for execution_id" in caplog.text

    # --- Verify that the database is empty ---
    # The transaction should have been rolled back.
    # The connection is closed by the orchestrator's finally block, so we
    # must re-establish it to check the state.
    postgres_adapter.connect()
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 0
