import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from py_load_epar.etl.extract import extract_data


@pytest.fixture(scope="module")
def sample_excel_file() -> Path:
    """Provides the path to the sample Excel file."""
    # This path is relative to the root of the project where pytest is run
    return Path("tests/test_data/sample_ema_data.xlsx")


@patch("py_load_epar.etl.extract.download_excel_file")
def test_extract_data_parses_excel_correctly(
    mock_download, sample_excel_file: Path
):
    """
    Test that extract_data correctly downloads and parses the sample Excel file.
    """
    # Arrange: Mock the downloader to return the path to our local sample file
    mock_download.return_value = sample_excel_file

    # Act: Call the extract_data function
    # The settings object is not used in the new implementation, so we can pass None
    records = list(extract_data(settings=None))

    # Assert: Check that the data was parsed as expected
    assert len(records) == 2

    # Check the first record
    record1 = records[0]
    assert record1["medicine_name"] == "TestMed1"
    assert record1["authorization_status"] == "Authorised"
    assert record1["marketing_authorization_holder_raw"] == "Test Pharma 1"
    assert record1["source_url"] == "http://example.com/doc1.pdf"
    # The value from Excel is a string, check if it's parsed correctly
    assert record1["last_update_date_source"] == "2024-01-25"

    # Check the second record
    record2 = records[1]
    assert record2["medicine_name"] == "TestMed2"
    assert record2["orphan_medicine"] == "Yes"
    assert record2["source_url"] == "http://example.com/doc2.pdf"

    # Assert that the downloader was called
    mock_download.assert_called_once()
