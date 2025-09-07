from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Type

if TYPE_CHECKING:
    from pydantic import BaseModel


class IDatabaseAdapter(ABC):
    """
    Abstract interface (Port) for database loading operations.
    Defines the contract that all database adapters must implement.
    """

    @abstractmethod
    def connect(self, connection_params: Optional[Dict[str, Any]]) -> None:
        """Establish connection to the target database."""
        pass

    @abstractmethod
    def prepare_load(self, load_strategy: str, target_table: str) -> str:
        """
        Prepare the database for loading.

        This can include creating staging tables, truncating tables for a FULL load,
        or other setup tasks.

        Args:
            load_strategy: The loading strategy ('FULL' or 'DELTA').
            target_table: The final target table for the data.

        Returns:
            The name of the table to load data into (e.g., a staging table).
        """
        pass

    @abstractmethod
    def bulk_load_batch(
        self,
        data_iterator: Iterator["BaseModel"],
        target_table: str,
        pydantic_model: Type["BaseModel"],
    ) -> int:
        """
        Execute the native bulk load operation for a batch of data.

        Args:
            data_iterator: An iterator yielding Pydantic model instances to load.
            target_table: The table to load the data into (e.g., a staging table).
            pydantic_model: The Pydantic model class corresponding to the data.

        Returns:
            The number of rows loaded in the batch.
        """
        pass

    @abstractmethod
    def finalize(
        self,
        load_strategy: str,
        target_table: str,
        staging_table: str | None = None,
        pydantic_model: Type["BaseModel"] | None = None,
    ) -> None:
        """
        Finalize the load process (e.g., merge staging to target, analyze, commit).

        Args:
            load_strategy: The loading strategy ('FULL' or 'DELTA').
            target_table: The final target table.
            staging_table: The staging table used for the load (if any).
            pydantic_model: The Pydantic model, required for 'DELTA' loads to build
                the merge statement.
        """
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Roll back the transaction in case of failure."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass
