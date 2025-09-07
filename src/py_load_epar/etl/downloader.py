import io
import logging
from typing import Any, Dict, Iterator

import openpyxl
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
def download_file(url: str) -> io.BytesIO:
    """
    Downloads a file from a URL into an in-memory buffer.

    Includes retry logic with exponential backoff for robustness.

    Args:
        url: The URL of the file to download.

    Returns:
        An in-memory BytesIO buffer containing the file content.

    Raises:
        requests.exceptions.RequestException: If the download fails after all retries.
    """
    logger.info(f"Downloading file from {url}...")
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        logger.info(f"Successfully downloaded file from {url} ({len(response.content)} bytes).")
        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        logger.error(f"Attempt to download {url} failed: {e}")
        raise


def parse_excel_data(file_content: io.BytesIO) -> Iterator[Dict[str, Any]]:
    """
    Parses an Excel file from an in-memory buffer and yields rows as dictionaries.

    Assumes the first row is the header.

    Args:
        file_content: A BytesIO buffer containing the Excel file content.

    Yields:
        A dictionary for each row, with column headers as keys.
    """
    logger.info("Parsing Excel data from in-memory buffer.")
    workbook = openpyxl.load_workbook(file_content)
    sheet = workbook.active

    # Read the header row and clean up names (e.g., convert to string, strip whitespace)
    header = [str(cell.value).strip() for cell in sheet[1]]
    logger.debug(f"Parsed header row: {header}")

    # Iterate over the rest of the rows
    for row in sheet.iter_rows(min_row=2):
        row_data = {header[i]: cell.value for i, cell in enumerate(row)}
        yield row_data

    logger.info("Finished parsing Excel data.")
