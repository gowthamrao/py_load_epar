import datetime
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterator

from py_load_epar.config import Settings
from py_load_epar.etl.downloader import download_excel_file
from py_load_epar.etl.parser import parse_ema_excel_file

logger = logging.getLogger(__name__)


def extract_data(
    settings: Settings, high_water_mark: datetime.datetime | None = None
) -> Iterator[Dict[str, Any]]:
    """
    Orchestrates the extraction of EPAR data from the source file.

    1. Downloads the main EMA data file to a temporary location.
    2. Parses the Excel file into a stream of dictionaries with snake_cased keys.
    3. Remaps and cleans raw dictionary keys to match Pydantic model fields.
    4. Filters the records based on the high_water_mark for Change Data Capture (CDC).
    5. Cleans up the temporary file after completion.

    Args:
        settings: The application settings, containing the URL for the data file.
        high_water_mark: The timestamp of the last successful run. Only records
                         newer than this will be processed.

    Yields:
        An iterator of dictionaries, where each dictionary represents a single
        new or updated record, cleaned and ready for Pydantic validation.
    """
    logger.info("Starting data extraction process.")
    if high_water_mark:
        logger.info(f"Using high water mark for CDC: {high_water_mark.isoformat()}")

    temp_dir = tempfile.mkdtemp(prefix="py_load_epar_")
    try:
        file_path = download_excel_file(
            url=settings.api.ema_file_url, destination_folder=Path(temp_dir)
        )
        raw_records_iterator = parse_ema_excel_file(file_path)

        processed_count = 0
        for record in raw_records_iterator:
            # --- Field renaming and type conversion ---
            update_date_val = record.get("revision_date")
            if not update_date_val:
                continue

            if isinstance(update_date_val, datetime.datetime):
                record_date = update_date_val.date()
            elif isinstance(update_date_val, datetime.date):
                record_date = update_date_val
            else:
                try:
                    record_date = datetime.datetime.fromisoformat(str(update_date_val)).date()
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse date '{update_date_val}' for record. Skipping.")
                    continue

            # The Pydantic model expects 'last_update_date_source'
            record["last_update_date_source"] = record_date

            # Rename keys from parser output to match Pydantic model fields
            if "marketing_authorisation_holder_company_name" in record:
                record["marketing_authorization_holder_raw"] = record.pop("marketing_authorisation_holder_company_name")
            if "active_substance" in record:
                record["active_substance_raw"] = record.pop("active_substance")

            # --- CDC Filter ---
            if high_water_mark and record_date <= high_water_mark.date():
                continue

            yield record
            processed_count += 1
        logger.info(f"Finished data extraction. Yielded {processed_count} new/updated records.")

    finally:
        shutil.rmtree(temp_dir)
        logger.debug(f"Cleaned up temporary directory: {temp_dir}")
