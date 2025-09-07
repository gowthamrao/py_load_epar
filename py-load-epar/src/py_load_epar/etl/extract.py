import datetime
import logging
from typing import Any, Dict, Iterator

import openpyxl
from py_load_epar.config import Settings
from py_load_epar.etl.downloader import EMA_EXCEL_URL, download_excel_file

logger = logging.getLogger(__name__)


def _clean_header(header: str) -> str:
    """Converts an Excel header to a snake_case identifier."""
    if not header:
        return ""
    return "".join(filter(str.isalnum, header.lower()))


def extract_data(
    settings: Settings, high_water_mark: datetime.date | None = None
) -> Iterator[Dict[str, Any]]:
    """
    Extracts EPAR data from the source.

    Downloads the EMA Excel file, parses it, and yields records row by row in a
    format that can be validated by the Pydantic models.
    """
    logger.info("Starting data extraction.")
    download_path = None
    try:
        download_path = download_excel_file(url=EMA_EXCEL_URL)
        workbook = openpyxl.load_workbook(download_path, read_only=True)
        sheet = workbook.active
        if sheet is None:
            raise ValueError("No active worksheet found in the Excel file.")

        # 1. Read header and create a mapping to model field names
        header_row = [cell.value for cell in sheet[1]]

        # This mapping defines which Excel columns we care about and what their
        # corresponding Pydantic model field name is.
        # It's based on inspecting the actual Excel file from EMA.
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

        # Create a reverse map from cell index to our desired field name
        index_to_field_name = {
            i: header_to_field_map[h]
            for i, h in enumerate(header_row)
            if h in header_to_field_map
        }

        if not index_to_field_name:
            raise ValueError(
                "Could not map any headers from the Excel file to the expected fields."
            )

        logger.info(f"Mapped {len(index_to_field_name)} columns from Excel file.")

        # 2. Iterate over data rows
        for row in sheet.iter_rows(min_row=2, values_only=True):
            record = {
                field_name: row[index]
                for index, field_name in index_to_field_name.items()
                if index < len(row)
            }

            # TODO: Implement proper CDC check using 'last_update_date_source'
            if record.get("medicine_name"):  # Basic check for non-empty rows
                yield record

    finally:
        # 3. Clean up the downloaded file
        if download_path and download_path.exists():
            logger.debug(f"Cleaning up downloaded file: {download_path}")
            download_path.unlink()
            try:
                download_path.parent.rmdir()
            except OSError:
                pass  # Ignore error if directory is not empty

    logger.info("Finished data extraction.")
