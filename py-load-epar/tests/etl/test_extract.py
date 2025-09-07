import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from py_load_epar.etl.extract import extract_data


@pytest.fixture(scope="module")
def sample_excel_file() -> Path:
    """Provides the path to the sample Excel file."""
    # Construct path relative to this test file, which is more robust
    return Path(__file__).parent.parent / "test_data/sample_ema_data.xlsx"


from py_load_epar.config import Settings


@patch("py_load_epar.etl.extract.download_excel_file")
def test_extract_data_parses_excel_correctly(
    mock_download, sample_excel_file: Path
):
    """
    Test that extract_data correctly downloads and parses the sample Excel file.
    """
    # Arrange: Mock the downloader to return the path to our local sample file
    mock_download.return_value = sample_excel_file
    settings = Settings()  # Create a dummy settings object

    # Act: Call the extract_data function and unpack the results
    records_iterator, new_hwm = extract_data(settings=settings)
    records = list(records_iterator)

    # Assert: Check that the data was parsed as expected
    assert len(records) == 2
    assert new_hwm is not None
    assert new_hwm.day == 15  # The latest date in the sample file is Feb 15

    # Check the first record
    record1 = records[0]
    assert record1["medicine_name"] == "TestMed1"
    assert record1["authorization_status"] == "Authorised"
    assert record1["marketing_authorization_holder_raw"] == "Test Pharma 1"
    assert record1["source_url"] == "http://example.com/doc1.pdf"
    assert record1["last_update_date_source"] == datetime.datetime(2024, 1, 25, 0, 0)

    # Check the second record
    record2 = records[1]
    assert record2["medicine_name"] == "TestMed2"
    assert record2["orphan_medicine"] == "Yes"
    assert record2["source_url"] == "http://example.com/doc2.pdf"

    # Assert that the downloader was called
    mock_download.assert_called_once()
