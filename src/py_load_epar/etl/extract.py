import datetime
import logging
from typing import Iterator, Dict, Any

from py_load_epar.config import Settings
from py_load_epar.etl.downloader import download_file, parse_excel_data

logger = logging.getLogger(__name__)

# The date format used in the EMA Excel file
EMA_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
# The name of the column in the Excel file that we use for CDC
CDC_COLUMN_NAME = "Last update date"


def extract_data(settings: Settings, high_water_mark: datetime.date | None = None) -> Iterator[Dict[str, Any]]:
    """
    Extracts EPAR data from the source by downloading and parsing the EMA file.

    It orchestrates the download and parsing, then applies Change Data Capture (CDC)
    logic to filter for new or updated records based on the high_water_mark.

    Args:
        settings: The application settings, containing the URL for the file.
        high_water_mark: The last update date from the previous successful run.

    Yields:
        A dictionary representing a single raw EPAR record that is new or updated.
    """
    logger.info("Starting data extraction process.")

    # 1. Download the file
    file_url = settings.api.ema_file_url
    file_content = download_file(file_url)

    # 2. Parse the Excel data
    raw_records = parse_excel_data(file_content)

    # 3. Apply CDC logic
    logger.info(f"Applying CDC filter with high_water_mark: {high_water_mark}")
    extracted_count = 0
    for record in raw_records:
        # The date in the Excel file can be a datetime object or a string
        last_update_val = record.get(CDC_COLUMN_NAME)

        if not last_update_val:
            logger.warning(f"Record missing CDC column '{CDC_COLUMN_NAME}'. Skipping. Record: {record.get('Medicine name')}")
            continue

        try:
            # Handle both datetime objects and string representations
            if isinstance(last_update_val, datetime.datetime):
                record_date = last_update_val.date()
            else:
                # Assuming the string is in a consistent format.
                # The .date() call extracts just the date part.
                record_date = datetime.datetime.strptime(str(last_update_val), EMA_DATE_FORMAT).date()
        except (ValueError, TypeError) as e:
            logger.error(
                f"Could not parse date '{last_update_val}' for record. Skipping. "
                f"Record: {record.get('Medicine name')}. Error: {e}"
            )
            continue

        # The actual CDC filter
        if high_water_mark is None or record_date > high_water_mark:
            logger.debug(f"Extracting record updated on {record_date}: {record.get('Medicine name')}")
            # Add the parsed date to the record for downstream use
            record['last_update_date_source'] = record_date
            yield record
            extracted_count += 1

    logger.info(f"Finished data extraction. Extracted {extracted_count} new/updated records.")
