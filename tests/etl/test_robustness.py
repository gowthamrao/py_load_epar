# Tests for the robustness of the ETL pipeline.
# These tests cover scenarios like duplicate data, schema changes, and large data volumes.
from pathlib import Path

import pandas as pd
import pytest

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def sample_excel_file_with_duplicates(tmp_path: Path) -> Path:
    """Creates a sample EMA data file with duplicate records for testing."""
    file_path = tmp_path / "test_ema_data_with_duplicates.xlsx"
    data = {
        "Category": ["Human", "Human", "Human"],
        "Medicine name": ["TestMed A", "TestMed A", "TestMed B"],
        "Therapeutic area": ["Oncology", "Oncology", "Cardiology"],
        "Active substance": ["substance_a", "substance_a", "substance_b"],
        "Product number": ["EMA/1", "EMA/1", "EMA/2"],
        "Patient safety": [None, None, None],
        "authorization_status": ["Authorised", "Authorised", "Authorised"],
        "ATC code": ["L01", "L01", "C01"],
        "Additional monitoring": [None, None, None],
        "Generic": [False, False, True],
        "Biosimilar": [False, False, False],
        "Conditional approval": [None, None, None],
        "Exceptional circumstances": [None, None, None],
        "Marketing authorisation date": ["2023-01-01", "2023-01-01", "2023-01-02"],
        "Revision date": ["2023-01-15", "2023-01-15", "2023-01-16"],
        "Marketing authorisation holder/company name": ["PharmaCo", "PharmaCo", "BioGen"],
        "URL": ["http://example.com/1", "http://example.com/1", "http://example.com/2"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_with_duplicate_data(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file_with_duplicates: Path,
):
    """
    Tests that the ETL pipeline correctly handles duplicate records in the source file.
    It should process the file without errors and load only the unique records.
    """
    # --- Mock dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=sample_excel_file_with_duplicates.open("rb"),
    )
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # --- Run the ETL process ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assertions ---
    with postgres_adapter.conn.cursor() as cursor:
        # Verify that only the unique records were loaded
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 2

        # Verify that the correct records are in the database
        cursor.execute("SELECT epar_id FROM epar_index ORDER BY epar_id")
        epar_ids = [row[0] for row in cursor.fetchall()]
        assert epar_ids == ["EMA/1", "EMA/2"]


@pytest.fixture
def sample_excel_file_with_new_column(tmp_path: Path) -> Path:
    """Creates a sample EMA data file with an extra column for testing."""
    file_path = tmp_path / "test_ema_data_with_new_column.xlsx"
    data = {
        "Category": ["Human"],
        "Medicine name": ["TestMed C"],
        "Therapeutic area": ["Oncology"],
        "Active substance": ["substance_c"],
        "Product number": ["EMA/3"],
        "Patient safety": [None],
        "authorization_status": ["Authorised"],
        "ATC code": ["L01"],
        "Additional monitoring": [None],
        "Generic": [False],
        "Biosimilar": [False],
        "Conditional approval": [None],
        "Exceptional circumstances": [None],
        "Marketing authorisation date": ["2023-01-03"],
        "Revision date": ["2023-01-17"],
        "Marketing authorisation holder/company name": ["PharmaCo"],
        "URL": ["http://example.com/3"],
        "New Unexpected Column": ["some value"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_with_new_column(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file_with_new_column: Path,
):
    """
    Tests that the ETL pipeline can handle a new, unexpected column in the source file.
    It should process the file without errors, ignoring the extra column.
    """
    # --- Mock dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=sample_excel_file_with_new_column.open("rb"),
    )
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # --- Run the ETL process ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assertions ---
    with postgres_adapter.conn.cursor() as cursor:
        # Verify that the record was loaded
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 1

        # Verify that the correct record is in the database
        cursor.execute("SELECT epar_id FROM epar_index")
        assert cursor.fetchone()[0] == "EMA/3"


@pytest.fixture
def large_sample_excel_file(tmp_path: Path) -> Path:
    """Creates a large sample EMA data file for performance testing."""
    file_path = tmp_path / "large_ema_data.xlsx"
    num_records = 10000
    data = {
        "Category": ["Human"] * num_records,
        "Medicine name": [f"TestMed {i}" for i in range(num_records)],
        "Therapeutic area": ["Various"] * num_records,
        "Active substance": [f"substance_{i}" for i in range(num_records)],
        "Product number": [f"EMA/{i}" for i in range(num_records)],
        "Patient safety": [None] * num_records,
        "authorization_status": ["Authorised"] * num_records,
        "ATC code": ["A01"] * num_records,
        "Additional monitoring": [None] * num_records,
        "Generic": [False] * num_records,
        "Biosimilar": [False] * num_records,
        "Conditional approval": [None] * num_records,
        "Exceptional circumstances": [None] * num_records,
        "Marketing authorisation date": ["2023-01-01"] * num_records,
        "Revision date": ["2023-01-15"] * num_records,
        "Marketing authorisation holder/company name": ["Big Pharma"] * num_records,
        "URL": [f"http://example.com/{i}" for i in range(num_records)],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


@pytest.mark.timeout(120)  # 2-minute timeout for this test
def test_etl_with_large_data_volume(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    large_sample_excel_file: Path,
):
    """
    Tests the ETL pipeline's performance and stability with a large volume of data.
    It should process the file efficiently without errors.
    """
    # --- Mock dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=large_sample_excel_file.open("rb"),
    )
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # --- Run the ETL process ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assertions ---
    with postgres_adapter.conn.cursor() as cursor:
        # Verify that all records were loaded
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 10000
