import datetime
from unittest.mock import MagicMock

import pytest
from py_load_epar.config import Settings
from py_load_epar.etl.extract import extract_data, CDC_COLUMN_NAME


@pytest.fixture
def mock_settings() -> Settings:
    """Fixture to provide a default Settings object for tests."""
    return Settings()


@pytest.fixture
def mock_parsed_data() -> list[dict]:
    """Fixture to provide a sample of parsed records from the Excel file."""
    return [
        {
            "Medicine name": "MedA",
            CDC_COLUMN_NAME: datetime.datetime(2023, 5, 20),
        },
        {
            "Medicine name": "MedB",
            CDC_COLUMN_NAME: datetime.datetime(2023, 6, 1),
        },
        {
            "Medicine name": "MedC",
            CDC_COLUMN_NAME: datetime.datetime(2023, 7, 15),
        },
        {
            "Medicine name": "MedD_bad_date",
            CDC_COLUMN_NAME: "invalid-date-format",
        },
        {
            "Medicine name": "MedE_no_date",
            "Some other column": "some value",
        },
    ]


def test_extract_data_no_high_water_mark(monkeypatch, mock_settings, mock_parsed_data):
    """
    Tests that all valid records are extracted when no high_water_mark is provided.
    """
    # Mock the downloader functions
    mock_download = MagicMock(return_value="dummy_bytes")
    mock_parse = MagicMock(return_value=iter(mock_parsed_data))
    monkeypatch.setattr("py_load_epar.etl.extract.download_file", mock_download)
    monkeypatch.setattr("py_load_epar.etl.extract.parse_excel_data", mock_parse)

    result = list(extract_data(settings=mock_settings, high_water_mark=None))

    # Should return the 3 valid records
    assert len(result) == 3
    assert result[0]["Medicine name"] == "MedA"
    assert result[1]["Medicine name"] == "MedB"
    assert result[2]["Medicine name"] == "MedC"
    mock_download.assert_called_once()
    mock_parse.assert_called_once()


def test_extract_data_with_high_water_mark(monkeypatch, mock_settings, mock_parsed_data):
    """
    Tests that only records with a last_update_date > high_water_mark are extracted.
    """
    mock_download = MagicMock(return_value="dummy_bytes")
    mock_parse = MagicMock(return_value=iter(mock_parsed_data))
    monkeypatch.setattr("py_load_epar.etl.extract.download_file", mock_download)
    monkeypatch.setattr("py_load_epar.etl.extract.parse_excel_data", mock_parse)

    # Set the high_water_mark to the date of the second record
    high_water_mark = datetime.date(2023, 6, 1)

    result = list(extract_data(settings=mock_settings, high_water_mark=high_water_mark))

    # Should only return the third record ("MedC")
    assert len(result) == 1
    assert result[0]["Medicine name"] == "MedC"


def test_extract_data_no_new_records(monkeypatch, mock_settings, mock_parsed_data):
    """
    Tests that no records are returned if the high_water_mark is up to date.
    """
    mock_download = MagicMock(return_value="dummy_bytes")
    mock_parse = MagicMock(return_value=iter(mock_parsed_data))
    monkeypatch.setattr("py_load_epar.etl.extract.download_file", mock_download)
    monkeypatch.setattr("py_load_epar.etl.extract.parse_excel_data", mock_parse)

    # Set the high_water_mark to the date of the last valid record
    high_water_mark = datetime.date(2023, 7, 15)

    result = list(extract_data(settings=mock_settings, high_water_mark=high_water_mark))

    # Should return no records
    assert len(result) == 0
