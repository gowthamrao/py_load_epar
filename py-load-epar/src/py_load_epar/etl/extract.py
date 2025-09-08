import datetime
import logging
from typing import Any, Dict, Iterator

from py_load_epar.config import Settings
from py_load_epar.etl.downloader import download_file_to_memory
from py_load_epar.etl.parser import parse_ema_excel_file

logger = logging.getLogger(__name__)


def extract_data(
    settings: Settings, high_water_mark: datetime.datetime | None = None
) -> Iterator[Dict[str, Any]]:
    """
    Orchestrates the extraction of EPAR data from the source file.

    1. Downloads the main EMA data file into memory.
    2. Parses the in-memory Excel file into a stream of dictionaries.
    3. Remaps and cleans raw dictionary keys to match Pydantic model fields.
    4. Filters records based on the high_water_mark for Change Data Capture (CDC).

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

    # 1. Download the file into an in-memory stream
    excel_file_stream = download_file_to_memory(url=settings.api.ema_file_url)

    # 2. Parse the stream directly
    raw_records_iterator = parse_ema_excel_file(excel_file_stream)

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
                record_date = datetime.datetime.fromisoformat(
                    str(update_date_val)
                ).date()
            except (ValueError, TypeError):
                logger.warning(
                    f"Could not parse date '{update_date_val}' for record. Skipping."
                )
                continue

        # The Pydantic model expects 'last_update_date_source'
        record["last_update_date_source"] = record_date

        # Rename keys from parser output to match Pydantic model fields
        if "marketing_authorisation_holder_company_name" in record:
            record["marketing_authorization_holder_raw"] = record.pop(
                "marketing_authorisation_holder_company_name"
            )
        if "active_substance" in record:
            record["active_substance_raw"] = record.pop("active_substance")

        # The 'URL' column from the sheet is snake_cased to 'url' by the parser.
        # We map it to the 'source_url' field in our Pydantic model.
        if "url" in record:
            record["source_url"] = record.pop("url")

        # --- CDC Filter ---
        if high_water_mark and record_date <= high_water_mark.date():
            continue

        yield record
        processed_count += 1
    logger.info(
        f"Finished data extraction. Yielded {processed_count} new/updated records."
    )
