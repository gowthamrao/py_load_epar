# Tests for the robustness of the ETL pipeline.
# These tests cover scenarios like duplicate data, schema changes, and large data volumes.
from pathlib import Path
import unicodedata

import pandas as pd
import pytest

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def sample_excel_file_with_pk_duplicates(tmp_path: Path) -> Path:
    """
    Creates a sample EMA data file with duplicate product numbers but different
    revision dates.
    """
    file_path = tmp_path / "test_ema_data_with_pk_duplicates.xlsx"
    data = {
        "Category": ["Human", "Human", "Human"],
        "Medicine name": ["TestMed A Old", "TestMed A New", "TestMed B"],
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
        "Revision date": ["2023-01-15", "2023-01-20", "2023-01-16"],
        "Marketing authorisation holder/company name": ["PharmaCo", "PharmaCo", "BioGen"],
        "URL": ["http://example.com/1", "http://example.com/1", "http://example.com/2"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_with_duplicate_product_numbers(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file_with_pk_duplicates: Path,
):
    """
    Tests that if the source file contains duplicate product numbers, only the one
    with the latest revision date is loaded.
    """
    # --- Mock dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=sample_excel_file_with_pk_duplicates.open("rb"),
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
        # Verify that only 2 records were loaded (the latest EMA/1 and EMA/2)
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 2

        # Verify that the correct version of EMA/1 is in the database
        cursor.execute("SELECT medicine_name FROM epar_index WHERE epar_id = 'EMA/1'")
        assert cursor.fetchone()[0] == "TestMed A New"


@pytest.fixture
def sample_excel_file_with_special_chars(tmp_path: Path) -> Path:
    """Creates a sample EMA data file with special (non-ASCII) characters."""
    file_path = tmp_path / "test_ema_data_with_special_chars.xlsx"
    data = {
        "Category": ["Human"],
        "Medicine name": ["Médicament Test Bø"],
        "Therapeutic area": ["Gastroentérologie"],
        "Active substance": ["substance_ß"],
        "Product number": ["EMA/SPECIAL"],
        "Patient safety": [None],
        "authorization_status": ["Authorised"],
        "ATC code": ["A02"],
        "Additional monitoring": [None],
        "Generic": [False],
        "Biosimilar": [False],
        "Conditional approval": [None],
        "Exceptional circumstances": [None],
        "Marketing authorisation date": ["2023-01-01"],
        "Revision date": ["2023-01-15"],
        "Marketing authorisation holder/company name": ["Crème Brûlée Pharma"],
        "URL": ["http://example.com/special"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_with_special_characters(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    sample_excel_file_with_special_chars: Path,
):
    """
    Tests that the ETL pipeline correctly handles non-ASCII characters.
    """
    # --- Mock dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=sample_excel_file_with_special_chars.open("rb"),
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
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 1

        cursor.execute(
            """
            SELECT medicine_name, therapeutic_area, active_substance_raw,
                   marketing_authorization_holder_raw
            FROM epar_index WHERE epar_id = 'EMA/SPECIAL'
            """
        )
        row = cursor.fetchone()
        assert unicodedata.normalize("NFC", row[0]) == unicodedata.normalize(
            "NFC", "Médicament Test Bø"
        )
        assert unicodedata.normalize("NFC", row[1]) == unicodedata.normalize(
            "NFC", "Gastroentérologie"
        )
        assert unicodedata.normalize("NFC", row[2]) == unicodedata.normalize(
            "NFC", "substance_ß"
        )
        assert unicodedata.normalize("NFC", row[3]) == unicodedata.normalize(
            "NFC", "Crème Brûlée Pharma"
        )


@pytest.fixture
def empty_excel_file(tmp_path: Path) -> Path:
    """Creates a completely empty Excel file."""
    file_path = tmp_path / "empty_ema_data.xlsx"
    df = pd.DataFrame()
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_with_empty_file(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    empty_excel_file: Path,
    caplog,
):
    """
    Tests that the ETL pipeline runs without error on an empty input file.
    """
    # --- Mock dependencies ---
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=empty_excel_file.open("rb"),
    )
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # --- Run the ETL process ---
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assertions ---
    # Verify that the ETL process completed successfully and logged the correct warning
    assert "Excel sheet is empty. No data to parse." in caplog.text

    # Verify no data was loaded
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 0

    # Verify that the SPOR client was not called
    mock_spor_client.return_value.search_organisation.assert_not_called()


@pytest.fixture
def delta_load_files(tmp_path: Path) -> tuple[Path, Path]:
    """
    Creates two Excel files to simulate a DELTA load scenario where one record
    is removed in the second run.
    """
    # File for the first run (initial load)
    file1_path = tmp_path / "delta_run1.xlsx"
    data1 = {
        "Category": ["Human", "Human"],
        "Medicine name": ["TestMed A", "TestMed B (to be removed)"],
        "Therapeutic area": ["Oncology", "Cardiology"],
        "Active substance": ["substance_a", "substance_b"],
        "Product number": ["EMA/DELTA/1", "EMA/DELTA/2"],
        "Patient safety": [None, None],
        "authorization_status": ["Authorised", "Authorised"],
        "ATC code": ["L01", "C01"],
        "Additional monitoring": [None, None],
        "Generic": [False, True],
        "Biosimilar": [False, False],
        "Conditional approval": [None, None],
        "Exceptional circumstances": [None, None],
        "Marketing authorisation date": ["2023-01-01", "2023-01-02"],
        "Revision date": ["2023-01-15", "2023-01-16"],
        "Marketing authorisation holder/company name": ["PharmaCo", "BioGen"],
        "URL": ["http://example.com/delta1", "http://example.com/delta2"],
    }
    df1 = pd.DataFrame(data1)
    df1.to_excel(file1_path, index=False, sheet_name="Medicines for human use")

    # File for the second run (record for EMA/DELTA/2 is removed)
    file2_path = tmp_path / "delta_run2.xlsx"
    data2 = {
        "Category": ["Human"],
        "Medicine name": ["TestMed A (updated)"],
        "Therapeutic area": ["Oncology"],
        "Active substance": ["substance_a"],
        "Product number": ["EMA/DELTA/1"],
        "Patient safety": [None],
        "authorization_status": ["Authorised"],
        "ATC code": ["L01"],
        "Additional monitoring": [None],
        "Generic": [False],
        "Biosimilar": [False],
        "Conditional approval": [None],
        "Exceptional circumstances": [None],
        "Marketing authorisation date": ["2023-01-01"],
        "Revision date": ["2023-01-18"], # Note the updated revision date
        "Marketing authorisation holder/company name": ["PharmaCo"],
        "URL": ["http://example.com/delta1"],
    }
    df2 = pd.DataFrame(data2)
    df2.to_excel(file2_path, index=False, sheet_name="Medicines for human use")

    return file1_path, file2_path


def test_delta_load_soft_delete(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    delta_load_files: tuple[Path, Path],
):
    """
    Tests that a DELTA load correctly performs a soft-delete on records that
    are no longer present in the source.
    """
    file1, file2 = delta_load_files

    # --- Mock dependencies ---
    mock_download = mocker.patch("py_load_epar.etl.extract.download_file_to_memory")
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)

    # --- 1. First Run (FULL load to establish baseline) ---
    mock_download.return_value = file1.open("rb")
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assert initial state ---
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index WHERE is_active = TRUE")
        assert cursor.fetchone()[0] == 2
        cursor.execute(
            "SELECT is_active FROM epar_index WHERE epar_id = 'EMA/DELTA/2'"
        )
        assert cursor.fetchone()[0] is True

    # --- 2. Second Run (DELTA load with one record removed) ---
    mock_download.return_value = file2.open("rb")
    settings.etl.load_strategy = "DELTA"
    run_etl(settings)

    # --- Assert final state ---
    with postgres_adapter.conn.cursor() as cursor:
        # Verify total records is still 2
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 2
        # Verify only 1 is active
        cursor.execute("SELECT COUNT(*) FROM epar_index WHERE is_active = TRUE")
        assert cursor.fetchone()[0] == 1
        # Verify the correct record was soft-deleted
        cursor.execute(
            "SELECT is_active FROM epar_index WHERE epar_id = 'EMA/DELTA/2'"
        )
        assert cursor.fetchone()[0] is False
        # Verify the other record is still active
        cursor.execute(
            "SELECT is_active FROM epar_index WHERE epar_id = 'EMA/DELTA/1'"
        )
        assert cursor.fetchone()[0] is True


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
