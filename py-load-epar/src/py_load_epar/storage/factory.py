import logging
from typing import cast

from py_load_epar.config import StorageSettings
from py_load_epar.storage.interfaces import IStorage
from py_load_epar.storage.local import LocalStorage
from py_load_epar.storage.s3 import S3Storage

logger = logging.getLogger(__name__)


class StorageFactory:
    """
    Factory for creating storage adapter instances based on configuration.
    """

    def __init__(self, settings: StorageSettings):
        self.settings = settings

    def get_storage(self) -> IStorage:
        """
        Instantiates and returns the appropriate storage adapter.

        Returns:
            An instance of a class that implements the IStorage interface.

        Raises:
            ValueError: If the configured backend is not supported.
        """
        backend = self.settings.backend.lower()
        logger.info(f"Creating storage adapter for backend: '{backend}'")

        if backend == "local":
            return LocalStorage(base_path=self.settings.local_storage_path)

        if backend == "s3":
            # The cast is safe because we expect s3_bucket to be set
            # if the backend is 's3'. This should be validated at a
            # higher level or within the settings model itself.
            bucket = cast(str, self.settings.s3_bucket)
            return S3Storage(bucket_name=bucket, region_name=self.settings.s3_region)

        raise ValueError(f"Unsupported storage backend: '{self.settings.backend}'")
