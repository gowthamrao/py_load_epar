import pandas as pd
import pytest
from pathlib import Path

from py_load_epar.config import Settings
from py_load_epar.db.postgres import PostgresAdapter
from py_load_epar.etl.orchestrator import run_etl

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def file_with_missing_column(tmp_path: Path) -> Path:
    """Creates a sample EMA data file that is missing a critical column."""
    file_path = tmp_path / "missing_column_data.xlsx"
    data = {
        "Category": ["Human"],
        "Medicine name": ["TestMed Missing Column"],
        # "Product number" is missing
        "Therapeutic area": ["Oncology"],
        "Active substance": ["substance_a"],
        "authorization_status": ["Authorised"],
        "URL": ["http://example.com/1"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_fails_on_missing_critical_column(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    file_with_missing_column: Path,
):
    """
    Tests that the ETL process fails gracefully if a critical column is
    missing from the source file.
    """
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=file_with_missing_column.open("rb"),
    )
    # Mock other dependencies to isolate the failure
    mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mocker.patch("py_load_epar.etl.orchestrator._process_documents")

    settings = db_settings
    settings.etl.load_strategy = "FULL"

    with pytest.raises(ValueError, match="Missing critical columns"):
        run_etl(settings)


@pytest.fixture
def file_with_bad_data_types(tmp_path: Path) -> Path:
    """Creates a sample EMA data file with incorrect data types."""
    file_path = tmp_path / "bad_data_type.xlsx"
    data = {
        "Category": ["Human"],
        "Medicine name": ["TestMed Bad Date"],
        "Product number": ["EMA/BAD_DATE"],
        "Therapeutic area": ["Toxicology"],
        "Active substance": ["substance_x"],
        "authorization_status": ["Authorised"],
        "Marketing authorisation date": ["Not a date"],  # Invalid data type
        "Revision date": ["2023-01-01"],
        "URL": ["http://example.com/bad_date"],
    }
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False, sheet_name="Medicines for human use")
    return file_path


def test_etl_fails_on_bad_data_type(
    postgres_adapter: PostgresAdapter,
    db_settings: Settings,
    mocker,
    file_with_bad_data_types: Path,
):
    """
    Tests that the ETL process gracefully handles records with data that
    cannot be coerced into the expected type (e.g., a string in a date field).
    The current implementation should log a warning and skip the record.
    """
    mocker.patch(
        "py_load_epar.etl.extract.download_file_to_memory",
        return_value=file_with_bad_data_types.open("rb"),
    )
    mocker.patch("py_load_epar.etl.orchestrator.SporApiClient")
    mocker.patch("py_load_epar.etl.orchestrator._process_documents")

    settings = db_settings
    run_etl(settings)

    # The record with the bad date should be skipped, so the DB should be empty
    with postgres_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM epar_index")
        assert cursor.fetchone()[0] == 0
