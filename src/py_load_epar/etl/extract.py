import datetime
import datetime
import logging
from typing import Any, Dict, Iterator

import pandas as pd
from py_load_epar.config import Settings
from py_load_epar.etl.downloader import download_file_to_memory
from py_load_epar.etl.parser import parse_ema_excel_file

logger = logging.getLogger(__name__)


def extract_data(  # noqa: C901
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
    excel_file_stream = download_file_to_memory(url=settings.etl.epar_data_url)

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

        # Coerce 'marketing_authorisation_date' to a date object, skipping
        # the record on failure. This ensures that malformed dates do not
        # silently become None during Pydantic validation.
        auth_date_val = record.get("marketing_authorisation_date")
        if auth_date_val:
            if isinstance(auth_date_val, datetime.datetime):
                record["marketing_authorisation_date"] = auth_date_val.date()
            elif isinstance(auth_date_val, datetime.date):
                # It's already a date, no action needed
                pass
            else:
                try:
                    # Pandas sometimes reads dates as timestamps, handle that
                    if isinstance(auth_date_val, pd.Timestamp):
                        record["marketing_authorisation_date"] = auth_date_val.date()
                    else:
                        # Otherwise, attempt to parse from string
                        record["marketing_authorisation_date"] = datetime.datetime.fromisoformat(
                            str(auth_date_val)
                        ).date()
                except (ValueError, TypeError):
                    logger.warning(
                        f"Could not parse marketing_authorisation_date "
                        f"'{auth_date_val}'. Skipping record."
                    )
                    continue

        # Rename keys from parser output to match Pydantic model fields
        # Handle inconsistent spelling of 'authorization'
        if "authorisation_status" in record and "authorization_status" not in record:
            record["authorization_status"] = record.pop("authorisation_status")

        if "marketing_authorisation_holder_company_name" in record:
            record["marketing_authorization_holder_raw"] = record.pop(
                "marketing_authorisation_holder_company_name"
            )
        record["active_substance_raw"] = record.pop("active_substance", None)
        if record["active_substance_raw"] is None:
            logger.warning(
                f"Record missing 'active_substance'. Skipping. Record: {record}"
            )
            continue

        # The 'URL' column from the sheet is snake_cased to 'u_r_l' by the parser.
        # We map it to the 'source_url' field in our Pydantic model.
        if "u_r_l" in record:
            record["source_url"] = record.pop("u_r_l")

        # --- CDC Filter ---
        if high_water_mark and record_date <= high_water_mark.date():
            continue

        yield record
        processed_count += 1
    logger.info(
        f"Finished data extraction. Yielded {processed_count} new/updated records."
    )
