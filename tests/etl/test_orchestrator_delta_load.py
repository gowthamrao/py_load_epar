import pandas as pd
import pytest
from pathlib import Path

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

@pytest.fixture
def initial_excel_file(tmp_path: Path) -> Path:
    """Creates the initial EMA data file for the FULL load."""
    file_path = tmp_path / "initial_ema_data.xlsx"
    data = {
        "Category": ["Human", "Human"],
        "Medicine name": ["TestMed A", "TestMed B"],
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
        "Marketing authorisation date": ["2023-01-01", "2023-01-02"],
        "Revision date": ["2023-01-15", "2023-01-16"],
        "Marketing authorisation holder/company name": ["PharmaCo", "BioGen"],
        "URL": ["http://example.com/1", "http://example.com/2"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path

@pytest.fixture
def delta_excel_file(tmp_path: Path) -> Path:
    """Creates the delta EMA data file for the DELTA load."""
    file_path = tmp_path / "delta_ema_data.xlsx"
    data = {
        "Category": ["Human", "Human", "Human"],
        "Medicine name": ["TestMed B Updated", "TestMed C", "TestMed A"], # TestMed A is withdrawn
        "Therapeutic area": ["Cardiology", "Neurology", "Oncology"],
        "Active substance": ["substance_b_updated", "substance_c", "substance_a"],
        "Product number": ["EMA/2", "EMA/3", "EMA/1"],
        "Patient safety": [None, None, None],
        "authorization_status": ["Authorised", "Authorised", "Withdrawn"],
        "ATC code": ["C01", "N01", "L01"],
        "Additional monitoring": [None, None, None],
        "Generic": [False, False, False],
        "Biosimilar": [False, False, False],
        "Conditional approval": [None, None, None],
        "Exceptional circumstances": [None, None, None],
        "Marketing authorisation date": ["2023-01-02", "2023-01-03", "2023-01-01"],
        "Revision date": ["2023-01-18", "2023-01-17", "2023-01-15"],
        "Marketing authorisation holder/company name": ["BioGen Inc.", "NeuroCorp", "PharmaCo"],
        "URL": ["http://example.com/2/updated", "http://example.com/3", "http://example.com/1"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path

def test_delta_load_strategy(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    initial_excel_file: Path,
    delta_excel_file: Path,
):
    """
    Tests the DELTA load strategy.
    1. Runs a FULL load.
    2. Runs a DELTA load with new, updated, and withdrawn data.
    3. Verifies the database state is correct after each load.
    """
    # --- Mock dependencies ---
    mock_spor_client = mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mock_spor_client.return_value.search_organisation.return_value = None
    mock_spor_client.return_value.search_substance.return_value = None
    mocker.patch("py_load_epar.etl.orchestrator._process_documents", return_value=0)
    mock_download = mocker.patch("py_load_epar.etl.extract.download_file_to_memory")

    # --- 1. Run FULL load ---
    mock_download.return_value = initial_excel_file.open("rb")
    settings = db_settings
    settings.etl.load_strategy = "FULL"
    run_etl(settings)

    # --- Assert initial state ---
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 2
        cursor.execute("SELECT medicine_name FROM epar_index WHERE epar_id = 'EMA/2'")
        assert cursor.fetchone()[0] == "TestMed B"
        cursor.execute("SELECT is_active FROM epar_index WHERE epar_id = 'EMA/1'")
        assert cursor.fetchone()[0] is True

    # --- 2. Run DELTA load ---
    mock_download.return_value = delta_excel_file.open("rb")
    settings.etl.load_strategy = "DELTA"
    run_etl(settings)

    # --- 3. Assert final state ---
    with postgres_adapter.conn.cursor() as cursor:
        # Verify total count (1 new record)
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 3

        # Verify updated record (EMA/2)
        cursor.execute("SELECT medicine_name, source_url FROM epar_index WHERE epar_id = 'EMA/2'")
        updated_record = cursor.fetchone()
        assert updated_record[0] == "TestMed B Updated"
        assert updated_record[1] == "http://example.com/2/updated"

        # Verify new record (EMA/3)
        cursor.execute("SELECT medicine_name FROM epar_index WHERE epar_id = 'EMA/3'")
        assert cursor.fetchone()[0] == "TestMed C"

        # Verify withdrawn record (EMA/1)
        cursor.execute("SELECT is_active FROM epar_index WHERE epar_id = 'EMA/1'")
        assert cursor.fetchone()[0] is False
