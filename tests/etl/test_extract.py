import datetime
import io
from unittest.mock import patch

from py_load_epar.config import Settings
from py_load_epar.etl.extract import extract_data


def test_extract_data_uses_downloader_and_parser():
    """
    Tests that extract_data correctly orchestrates the downloader and parser modules.
    """
    settings = Settings()
    # We use mock context managers to patch the dependencies of the extract_data function
    with patch("py_load_epar.etl.extract.download_file_to_memory") as mock_download, \
         patch("py_load_epar.etl.extract.parse_ema_excel_file") as mock_parse:

        # --- Arrange ---
        # Configure the mocks to return dummy values
        fake_file_stream = io.BytesIO(b"fake excel data")
        mock_download.return_value = fake_file_stream
        # The parser mock yields an empty iterator to prevent processing loops
        mock_parse.return_value = iter([])

        # --- Act ---
        # Consume the iterator from extract_data to ensure the code runs
        list(extract_data(settings=settings))

        # --- Assert ---
        # Check that the downloader was called with the correct URL
        mock_download.assert_called_once_with(url=settings.etl.epar_data_url)

        # Check that the parser was called with the stream returned by the downloader
        mock_parse.assert_called_once_with(fake_file_stream)


def test_extract_data_renames_fields_correctly():
    """
    Tests that extract_data correctly renames raw fields from the parser
    to the field names expected by the Pydantic models.
    """
    settings = Settings()
    with patch("py_load_epar.etl.extract.download_file_to_memory"), \
         patch("py_load_epar.etl.extract.parse_ema_excel_file") as mock_parse:

        # Arrange: Mock the parser to return a record with the "raw" field names
        mock_parse.return_value = iter([
            {
                "product_number": "EMA/1",
                "revision_date": datetime.date(2024, 1, 15),
                "marketing_authorisation_holder_company_name": "Test MAH",
                "active_substance": "Test Substance",
                "u_r_l": "http://example.com"
            }
        ])

        # Act
        records = list(extract_data(settings=settings))

        # Assert
        assert len(records) == 1
        record = records[0]
        assert "last_update_date_source" in record
        assert "marketing_authorization_holder_raw" in record
        assert "active_substance_raw" in record
        assert "source_url" in record
        assert record["last_update_date_source"] == datetime.date(2024, 1, 15)
        assert record["marketing_authorization_holder_raw"] == "Test MAH"
        assert record["active_substance_raw"] == "Test Substance"
        assert record["source_url"] == "http://example.com"


def test_extract_data_filters_by_high_water_mark():
    """
    Tests the CDC (Change Data Capture) logic of extract_data, ensuring it
    correctly filters out records that are not newer than the high_water_mark.
    """
    settings = Settings()
    # Records with a date on or before the HWM should be filtered out
    high_water_mark = datetime.datetime(2024, 2, 15)

    with patch("py_load_epar.etl.extract.download_file_to_memory"), \
         patch("py_load_epar.etl.extract.parse_ema_excel_file") as mock_parse:

        # Arrange: Mock the parser to return a list of records with various dates
        mock_parse.return_value = iter([
                {"product_number": "EMA/1", "medicine_name": "OldMed", "revision_date": datetime.date(2024, 1, 15), "active_substance": "a"},
                {"product_number": "EMA/2", "medicine_name": "SameDayMed", "revision_date": datetime.date(2024, 2, 15), "active_substance": "b"},
                {"product_number": "EMA/3", "medicine_name": "NewMed", "revision_date": datetime.date(2024, 2, 16), "active_substance": "c"},
        ])

        # Act: Call the function and get the list of processed records
        records = list(extract_data(settings=settings, high_water_mark=high_water_mark))

        # Assert: Only the record with a date after the HWM should be yielded
        assert len(records) == 1
        assert records[0]["medicine_name"] == "NewMed"
        assert records[0]["last_update_date_source"] == datetime.date(2024, 2, 16)
