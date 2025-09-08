import logging
import os
from pathlib import Path
from typing import IO

from py_load_epar.storage.interfaces import IStorage

logger = logging.getLogger(__name__)


class LocalStorage(IStorage):
    """
    An adapter for storing files on the local filesystem.
    """

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)
        self._create_base_directory()

    def _create_base_directory(self) -> None:
        """Ensures the base storage directory exists."""
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured local storage directory exists at: {self.base_path}")
        except OSError as e:
            logger.error(f"Failed to create local storage directory at {self.base_path}: {e}")
            raise

    def save(self, data_stream: IO[bytes], object_name: str) -> str:
        """
        Saves a byte stream to a file on the local filesystem.

        Args:
            data_stream: A file-like object in binary mode containing the data.
            object_name: The relative path/name for the file.

        Returns:
            The full file URI of the saved object.
        """
        destination_path = self.base_path / object_name
        logger.info(f"Attempting to save file to local path: {destination_path}")

        # Ensure the parent directory of the destination file exists
        try:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create parent directory for {destination_path}: {e}")
            raise

        try:
            with open(destination_path, "wb") as f:
                # Reset stream position just in case
                data_stream.seek(0)
                f.write(data_stream.read())

            file_uri = destination_path.as_uri()
            logger.info(f"Successfully saved file to {file_uri}")
            return file_uri
        except IOError as e:
            logger.error(f"Failed to write to file {destination_path}: {e}")
            raise
