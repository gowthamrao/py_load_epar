import io
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from py_load_epar.storage.local import LocalStorage
from py_load_epar.storage.s3 import S3Storage

# --- Tests for LocalStorage ---

def test_local_storage_save(tmp_path: Path):
    """
    Tests that LocalStorage correctly saves a file to the local filesystem.
    """
    storage = LocalStorage(base_path=tmp_path)
    test_content = b"this is a local test"
    data_stream = io.BytesIO(test_content)
    object_name = "docs/test_document.txt"

    storage_uri = storage.save(data_stream, object_name)

    expected_path = tmp_path / object_name
    assert expected_path.exists()
    assert expected_path.read_bytes() == test_content
    assert storage_uri == expected_path.as_uri()

def test_local_storage_creates_basedir():
    """
    Tests that LocalStorage creates the base directory if it doesn't exist.
    """
    # Use a non-existent path
    base_path = Path("./non_existent_dir_for_testing")
    # Clean up before and after just in case
    if base_path.exists():
        base_path.rmdir()

    assert not base_path.exists()
    storage = LocalStorage(base_path=base_path)
    assert base_path.exists()

    # Clean up
    base_path.rmdir()


def test_local_storage_create_basedir_permission_error(mocker):
    """
    Tests that LocalStorage raises an OSError if it fails to create the base directory.
    """
    mock_mkdir = mocker.patch("pathlib.Path.mkdir", side_effect=OSError("Permission denied"))

    with pytest.raises(OSError, match="Permission denied"):
        LocalStorage(base_path="./permission_denied_dir")

    mock_mkdir.assert_called_once()


def test_local_storage_save_write_error(tmp_path: Path, mocker):
    """
    Tests that LocalStorage raises an IOError if a file write fails.
    """
    storage = LocalStorage(base_path=tmp_path)
    mock_open = mocker.patch("builtins.open", side_effect=IOError("Disk full"))

    with pytest.raises(IOError, match="Disk full"):
        storage.save(io.BytesIO(b"test"), "some_file.txt")

    # Check that open was called with the correct path and mode
    expected_path = tmp_path / "some_file.txt"
    mock_open.assert_called_once_with(expected_path, "wb")


# --- Tests for S3Storage ---

@mock_aws
def test_s3_storage_save():
    """
    Tests that S3Storage correctly uploads a file to a mock S3 bucket.
    """
    bucket_name = "test-epar-bucket"
    region = "us-east-1"

    # Set up mock S3 environment
    s3_client = boto3.client("s3", region_name=region)
    s3_client.create_bucket(Bucket=bucket_name)

    storage = S3Storage(bucket_name=bucket_name, region_name=region)
    test_content = b"this is an s3 test"
    data_stream = io.BytesIO(test_content)
    object_name = "docs/s3_document.pdf"

    storage_uri = storage.save(data_stream, object_name)

    # Verify the file was uploaded
    response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
    assert response["Body"].read() == test_content
    assert storage_uri == f"s3://{bucket_name}/{object_name}"

from botocore.exceptions import ClientError


@mock_aws
def test_s3_storage_raises_error_on_failure(mocker):
    """
    Tests that S3Storage raises an IOError if the boto3 client fails.
    """
    storage = S3Storage(bucket_name="any-bucket")

    # Mock the upload_fileobj method to raise a ClientError
    # This is the actual exception class that boto3 raises for API errors.
    mock_error = ClientError(
        error_response={"Error": {"Code": "InternalError", "Message": "S3 is down"}},
        operation_name="UploadFile",
    )
    mocker.patch.object(
        storage.s3_client,
        "upload_fileobj",
        side_effect=mock_error,
    )

    with pytest.raises(IOError, match="S3 upload failed"):
        storage.save(io.BytesIO(b"test"), "any_object")

def test_s3_storage_requires_bucket():
    """
    Tests that S3Storage raises ValueError if no bucket name is provided.
    """
    with pytest.raises(ValueError, match="S3 bucket name must be provided"):
        S3Storage(bucket_name="")
