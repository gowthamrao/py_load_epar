from abc import ABC, abstractmethod
from typing import IO


class IStorage(ABC):
    """
    Interface (Port) for a storage backend.
    Defines the contract for saving files to a persistent storage.
    """

    @abstractmethod
    def save(self, data_stream: IO[bytes], object_name: str) -> str:
        """
        Saves a byte stream to the storage backend.

        Args:
            data_stream: A file-like object in binary mode containing the data.
            object_name: The name/key for the object in the storage backend
                         (e.g., 'documents/document1.pdf').

        Returns:
            The full URI of the saved object (e.g.,
            's3://my-bucket/documents/document1.pdf' or
            'file:///path/to/storage/documents/document1.pdf').
        """
        pass
