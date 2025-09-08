import datetime
from pathlib import Path
from unittest.mock import patch

from py_load_epar.config import Settings
from py_load_epar.etl.extract import extract_data


def test_extract_data_uses_downloader_and_parser():
    """
    Tests that extract_data correctly orchestrates the downloader and parser modules.
    """
    settings = Settings()
    # We use mock context managers to patch the dependencies of the extract_data function
    with patch("py_load_epar.etl.extract.download_excel_file") as mock_download, \
         patch("py_load_epar.etl.extract.parse_ema_excel_file") as mock_parse, \
         patch("py_load_epar.etl.extract.shutil.rmtree") as mock_rmtree:

        # --- Arrange ---
        # Configure the mocks to return dummy values
        fake_file_path = Path("/tmp/fake_dir/fake_file.xlsx")
        mock_download.return_value = fake_file_path
        # The parser mock yields an empty iterator to prevent processing loops
        mock_parse.return_value = iter([])

        # --- Act ---
        # Consume the iterator from extract_data to ensure the code runs
        list(extract_data(settings=settings))

        # --- Assert ---
        # Check that the downloader was called with the correct URL
        mock_download.assert_called_once()
        assert mock_download.call_args[1]['url'] == settings.api.ema_file_url

        # Check that the parser was called with the path returned by the downloader
        mock_parse.assert_called_once_with(fake_file_path)

        # Check that the temporary directory is cleaned up
        mock_rmtree.assert_called_once()


def test_extract_data_filters_by_high_water_mark():
    """
    Tests the CDC (Change Data Capture) logic of extract_data, ensuring it
    correctly filters out records that are not newer than the high_water_mark.
    """
    settings = Settings()
    # Records with a date on or before the HWM should be filtered out
    high_water_mark = datetime.datetime(2024, 2, 15)

    with patch("py_load_epar.etl.extract.download_excel_file"), \
         patch("py_load_epar.etl.extract.parse_ema_excel_file") as mock_parse:

        # Arrange: Mock the parser to return a list of records with various dates
        mock_parse.return_value = iter([
            {"medicine_name": "OldMed", "revision_date": datetime.date(2024, 1, 15)},
            {"medicine_name": "SameDayMed", "revision_date": datetime.date(2024, 2, 15)},
            {"medicine_name": "NewMed", "revision_date": datetime.date(2024, 2, 16)},
        ])

        # Act: Call the function and get the list of processed records
        records = list(extract_data(settings=settings, high_water_mark=high_water_mark))

        # Assert: Only the record with a date after the HWM should be yielded
        assert len(records) == 1
        assert records[0]["medicine_name"] == "NewMed"
        assert records[0]["last_update_date_source"] == datetime.date(2024, 2, 16)
