import logging
from typing import IO, Optional

import boto3
from botocore.exceptions import ClientError

from py_load_epar.storage.interfaces import IStorage

logger = logging.getLogger(__name__)


class S3Storage(IStorage):
    """
    An adapter for storing files in an AWS S3 bucket.
    """

    def __init__(self, bucket_name: str, region_name: Optional[str] = None):
        if not bucket_name:
            raise ValueError("S3 bucket name must be provided.")

        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3", region_name=region_name)
        logger.info(f"Initialized S3Storage for bucket '{self.bucket_name}' in region '{region_name or 'default'}'.")

    def save(self, data_stream: IO[bytes], object_name: str) -> str:
        """
        Uploads a byte stream to the S3 bucket.

        Args:
            data_stream: A file-like object in binary mode containing the data.
            object_name: The key for the object in the S3 bucket.

        Returns:
            The full S3 URI of the uploaded object.

        Raises:
            IOError: If the upload fails due to a client error.
        """
        logger.info(f"Attempting to upload to S3: s3://{self.bucket_name}/{object_name}")
        try:
            # Reset stream position to the beginning
            data_stream.seek(0)
            self.s3_client.upload_fileobj(data_stream, self.bucket_name, object_name)

            s3_uri = f"s3://{self.bucket_name}/{object_name}"
            logger.info(f"Successfully uploaded to {s3_uri}")
            return s3_uri
        except ClientError as e:
            logger.error(f"Failed to upload to S3 bucket '{self.bucket_name}': {e}")
            # Raise an IOError to provide a more generic exception type
            # at this layer of abstraction.
            raise IOError(f"S3 upload failed: {e}") from e
