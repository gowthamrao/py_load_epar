import pytest

from py_load_epar.config import StorageSettings
from py_load_epar.storage.factory import StorageFactory
from py_load_epar.storage.local import LocalStorage
from pathlib import Path
from py_load_epar.storage.s3 import S3Storage


def test_storage_factory_creates_local_storage():
    """
    Tests that the StorageFactory correctly creates a LocalStorage instance.
    """
    settings = StorageSettings(backend="local", local_storage_path="/tmp/test")
    factory = StorageFactory(settings=settings)
    storage = factory.get_storage()

    assert isinstance(storage, LocalStorage)
    assert storage.base_path == Path("/tmp/test")


def test_storage_factory_creates_s3_storage():
    """
    Tests that the StorageFactory correctly creates an S3Storage instance.
    """
    settings = StorageSettings(
        backend="s3", s3_bucket="test-bucket", s3_region="us-west-2"
    )
    factory = StorageFactory(settings=settings)
    storage = factory.get_storage()

    assert isinstance(storage, S3Storage)
    assert storage.bucket_name == "test-bucket"
    assert storage.s3_client.meta.region_name == "us-west-2"


def test_storage_factory_raises_error_for_unsupported_backend():
    """
    Tests that the StorageFactory raises a ValueError for an unsupported backend.
    """
    settings = StorageSettings(backend="ftp")
    factory = StorageFactory(settings=settings)

    with pytest.raises(ValueError, match="Unsupported storage backend: 'ftp'"):
        factory.get_storage()
