import hashlib
import logging
import tempfile
from pathlib import Path
from typing import IO, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# The official URL for the EMA medicines data Excel file
EMA_EXCEL_URL = "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _download_file_to_stream(url: str, file_stream: IO[bytes]) -> None:
    """
    Downloads a file from a URL into a byte stream with retry logic.

    Args:
        url: The URL to download the file from.
        file_stream: A file-like object opened in binary write mode.
    """
    logger.info(f"Attempting to download file from: {url}")
    try:
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                file_stream.write(chunk)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download file from {url}: {e}")
        raise


def download_excel_file(
    url: str, destination_folder: Path | str | None = None
) -> Path:
    """
    Downloads the main EMA Excel file and saves it to a specified location.

    Args:
        url: The URL of the Excel file.
        destination_folder: The folder to save the file in. If None, a temporary
                            folder is created.

    Returns:
        The path to the downloaded file.
    """
    if destination_folder:
        Path(destination_folder).mkdir(parents=True, exist_ok=True)
        destination_path = Path(destination_folder) / "ema_data.xlsx"
    else:
        temp_dir = tempfile.mkdtemp(prefix="py_load_epar_")
        destination_path = Path(temp_dir) / "ema_data.xlsx"

    with open(destination_path, "wb") as f:
        _download_file_to_stream(url, f)

    logger.info(f"Successfully downloaded Excel file to: {destination_path}")
    return destination_path


def download_document_and_hash(
    url: str, destination_folder: Path
) -> Tuple[Path, str]:
    """
    Downloads a document, saves it, and calculates its SHA-256 hash.

    The file is saved using its URL's filename.

    Args:
        url: The URL of the document to download.
        destination_folder: The root folder for storing documents.

    Returns:
        A tuple containing the path to the saved file and its SHA-256 hash.
    """
    hasher = hashlib.sha256()
    filename = url.split("/")[-1] or "downloaded_document"
    destination_path = destination_folder / filename

    destination_folder.mkdir(parents=True, exist_ok=True)

    with open(destination_path, "wb") as f:
        _download_file_to_stream(url, f)

    # Re-open the file to calculate the hash
    with open(destination_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)

    file_hash = hasher.hexdigest()
    logger.info(
        f"Successfully downloaded document to {destination_path} with hash {file_hash}"
    )
    return destination_path, file_hash
