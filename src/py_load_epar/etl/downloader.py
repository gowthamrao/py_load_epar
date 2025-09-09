import hashlib
import io
import logging
from typing import IO, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from py_load_epar.storage.interfaces import IStorage

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


def download_file_to_memory(url: str) -> io.BytesIO:
    """
    Downloads a file from a URL into an in-memory BytesIO stream.

    Args:
        url: The URL to download the file from.

    Returns:
        An io.BytesIO object containing the file content.
    """
    memory_file = io.BytesIO()
    _download_file_to_stream(url, memory_file)
    memory_file.seek(0)  # Rewind the stream to the beginning for reading
    logger.info(f"Successfully downloaded file from {url} into memory.")
    return memory_file


def download_document_and_hash(
    url: str, storage: IStorage, object_name_prefix: str = "documents"
) -> Tuple[str, str]:
    """
    Downloads a document to memory, calculates its hash, and saves it via a
    storage adapter.

    The object name is derived from the URL's filename.

    Args:
        url: The URL of the document to download.
        storage: An instance of a storage adapter (e.g., LocalStorage, S3Storage).
        object_name_prefix: A prefix for the object name in the storage backend.

    Returns:
        A tuple containing the storage URI of the saved file and its SHA-256 hash.
    """
    hasher = hashlib.sha256()
    filename = url.split("/")[-1] or "downloaded_document"
    object_name = f"{object_name_prefix}/{filename}"

    # 1. Download the file into an in-memory stream
    memory_file = download_file_to_memory(url)

    # 2. Calculate the hash from the in-memory stream
    # Iterate over the stream in chunks to avoid loading the whole file into
    # memory again
    memory_file.seek(0)
    for chunk in iter(lambda: memory_file.read(4096), b""):
        hasher.update(chunk)
    file_hash = hasher.hexdigest()

    # Rewind the stream again before passing it to the storage adapter
    memory_file.seek(0)

    # 3. Use the storage adapter to save the file
    storage_uri = storage.save(data_stream=memory_file, object_name=object_name)

    logger.info(
        f"Processed document from {url}, stored at {storage_uri} with hash {file_hash}"
    )
    return storage_uri, file_hash
