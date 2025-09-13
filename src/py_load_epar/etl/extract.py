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

    # 2. Parse the stream into a DataFrame for deduplication
    raw_records_iterator = parse_ema_excel_file(excel_file_stream)
    try:
        df = pd.DataFrame(raw_records_iterator)
        if df.empty:
            logger.info("Source file is empty. No records to process.")
            return
    except (IOError, TypeError, AttributeError):
        logger.info("Could not create DataFrame, likely empty file. No records to process.")
        return

    # --- Handle duplicates: keep the record with the latest revision date ---
    # Convert revision_date to datetime, coercing errors to NaT
    df["revision_date"] = pd.to_datetime(df["revision_date"], errors="coerce")
    # Drop rows where revision_date could not be parsed
    df.dropna(subset=["revision_date"], inplace=True)
    # Sort by revision date to ensure the latest is last
    df.sort_values(by="revision_date", ascending=True, inplace=True)
    # Drop duplicates on product number, keeping the last (most recent)
    df.drop_duplicates(subset=["product_number"], keep="last", inplace=True)

    processed_count = 0
    for record in df.to_dict("records"):
        # --- Field renaming and type conversion ---
        record_date = record["revision_date"].date()

        # The Pydantic model expects 'last_update_date_source'
        record["last_update_date_source"] = record_date

        # Coerce 'marketing_authorisation_date' to a date object, skipping
        # the record on failure.
        auth_date_val = record.get("marketing_authorisation_date")
        if pd.notna(auth_date_val):
            # pd.to_datetime can handle various formats including existing datetimes
            coerced_date = pd.to_datetime(auth_date_val, errors="coerce")
            if pd.notna(coerced_date):
                record["marketing_authorisation_date"] = coerced_date.date()
            else:
                logger.warning(
                    f"Could not parse marketing_authorisation_date "
                    f"'{auth_date_val}'. Skipping record."
                )
                continue
        else:
            record["marketing_authorisation_date"] = None

        # Rename keys from parser output to match Pydantic model fields
        if "authorisation_status" in record:
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

        # The 'URL' column is snake_cased to 'u_r_l' by the parser.
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
