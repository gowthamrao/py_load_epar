from pathlib import Path

import pytest

from py_load_epar.etl.parser import parse_ema_excel_file

# Define the path to the test data relative to this test file's location.
# This makes the test robust and runnable from any directory.
TEST_DATA_DIR = Path(__file__).parent.parent / "test_data"
SAMPLE_FILE_PATH = TEST_DATA_DIR / "sample_ema_data.xlsx"


@pytest.fixture
def sample_file(create_sample_excel_file: None) -> Path:
    """Fixture to provide the path to the sample Excel file and check for its existence."""
    if not SAMPLE_FILE_PATH.exists():
        pytest.fail(f"Test data file not found at: {SAMPLE_FILE_PATH}")
    return SAMPLE_FILE_PATH


def test_parse_ema_excel_file_returns_iterator(sample_file: Path):
    """Test that the parser returns an iterator."""
    from collections.abc import Iterator

    result = parse_ema_excel_file(sample_file)
    assert isinstance(result, Iterator)


def test_parse_ema_excel_file_yields_correct_data(sample_file: Path):
    """
    Tests that the parser successfully reads a valid Excel file and yields
    the correct data as dictionaries with snake_cased keys.
    """
    records = list(parse_ema_excel_file(sample_file))

    # Check that some records were parsed (the exact number might change)
    assert len(records) == 2

    # --- Check the first record ---
    first_record = records[0]
    assert isinstance(first_record, dict)

    # Check that headers were correctly converted to snake_case
    assert "medicine_name" in first_record
    assert "marketing_authorisation_holder_company_name" in first_record
    assert "revision_date" in first_record

    # Check specific values from the first data row of the sample file
    assert first_record["medicine_name"] == "TestMed1"
    assert first_record["marketing_authorisation_holder_company_name"] == "Test Pharma 1"
    assert first_record["authorisation_status"] == "Authorised"

    # --- Check a different record to ensure iteration is working ---
    second_record = records[1]
    assert second_record["medicine_name"] == "TestMed2"
    assert second_record["active_substance"] == "Testmed-B"


def test_parse_non_existent_file_raises_error():
    """
    Tests that the parser raises an exception when the file does not exist.
    We expect openpyxl to raise a FileNotFoundError.
    """
    non_existent_path = Path("this_file_does_not_exist.xlsx")
    with pytest.raises(FileNotFoundError):
        # We must consume the iterator to trigger the file open operation
        list(parse_ema_excel_file(non_existent_path))
