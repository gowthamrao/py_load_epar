import datetime
import logging
from typing import Any, Dict, Iterator

import openpyxl
from py_load_epar.config import Settings
from py_load_epar.etl.downloader import EMA_EXCEL_URL, download_file_to_memory

logger = logging.getLogger(__name__)


def _clean_header(header: str) -> str:
    """Converts an Excel header to a snake_case identifier."""
    if not header:
        return ""
    return "".join(filter(str.isalnum, header.lower()))


def extract_data(
    settings: Settings, high_water_mark: datetime.datetime | None = None
) -> Iterator[Dict[str, Any]]:
    """
    Extracts EPAR data from the source, implementing CDC filtering.

    Downloads the EMA Excel file, parses it, and yields records row by row that
    are newer than the provided high_water_mark. The caller is responsible for
    determining the new high water mark from the yielded records.

    Args:
        settings: The application settings.
        high_water_mark: The timestamp of the last successful run. Only records
                         newer than this will be processed.

    Yields:
        An iterator of dictionaries for each new or updated record.
    """
    logger.info("Starting data extraction.")
    if high_water_mark:
        logger.info(f"Using high water mark for CDC: {high_water_mark}")

    # Download the Excel file into an in-memory buffer
    excel_file_stream = download_file_to_memory(url=EMA_EXCEL_URL)

    # Process the in-memory file
    workbook = openpyxl.load_workbook(excel_file_stream, read_only=True)
    sheet = workbook.active
    if sheet is None:
        raise ValueError("No active worksheet found in the Excel file.")

    header_row = [cell.value for cell in sheet[1]]
    header_to_field_map = {
        "Category": "category",
        "Medicine name": "medicine_name",
        "Therapeutic area": "therapeutic_area",
        "INN / common name": "active_substance_raw",
        "Authorisation status": "authorization_status",
        "Orphan medicine": "orphan_medicine",
        "Marketing authorisation holder/company name": "marketing_authorization_holder_raw",
        "Date of opinion": "date_of_opinion",
        "First published": "first_published",
        "Revision date": "last_update_date_source",
        "URL": "source_url",
    }
    index_to_field_name = {
        i: header_to_field_map[h]
        for i, h in enumerate(header_row)
        if h in header_to_field_map
    }

    if not index_to_field_name:
        raise ValueError("Could not map any headers from Excel file.")

    processed_count = 0
    for row in sheet.iter_rows(min_row=2, values_only=True):
        record = {
            field_name: row[index]
            for index, field_name in index_to_field_name.items()
            if index < len(row)
        }

        if not record.get("medicine_name"):
            continue

        update_date_val = record.get("last_update_date_source")
        if isinstance(update_date_val, str):
            try:
                # Handle ISO format with or without time component
                if " " in update_date_val:
                    update_date_val = datetime.datetime.strptime(
                        update_date_val, "%Y-%m-%d %H:%M:%S"
                    )
                else:
                    update_date_val = datetime.datetime.fromisoformat(update_date_val)
            except (ValueError, TypeError):
                logger.warning(
                    f"Could not parse date '{update_date_val}' for "
                    f"record {record.get('medicine_name')}. Skipping."
                )
                continue
        elif not isinstance(update_date_val, datetime.datetime):
            continue  # Skip rows without a valid date for CDC

        record["last_update_date_source"] = update_date_val

        # Apply CDC filter
        if high_water_mark and update_date_val <= high_water_mark:
            continue

        yield record
        processed_count += 1
    logger.info(f"Finished data extraction. Yielded {processed_count} records.")
