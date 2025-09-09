from abc import ABC, abstractmethod
import datetime
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
        data_iterator: Iterator[tuple],
        target_table: str,
        columns: list[str],
    ) -> int:
        """
        Execute the native bulk load operation for a batch of data.

        Args:
            data_iterator: An iterator yielding tuples of data to load.
            target_table: The table to load the data into (e.g., a staging table).
            columns: A list of column names in the order they appear in the tuples.

        Returns:
            The number of rows loaded in the batch.
        """
        pass

    @abstractmethod
    def finalize(
        self,
        load_strategy: str,
        target_table: str,
        staging_table: Optional[str] = None,
        pydantic_model: Optional[Type["BaseModel"]] = None,
        primary_key_columns: Optional[list[str]] = None,
        soft_delete_settings: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Finalize the load process (e.g., merge staging to target, analyze, commit).
        Args:
            load_strategy: The loading strategy ('FULL' or 'DELTA').
            target_table: The final target table.
            staging_table: The staging table used for the load (if any).
            pydantic_model: The Pydantic model, required for 'DELTA' loads.
            primary_key_columns: A list of primary key columns, required for 'DELTA'
                loads to build the merge/conflict statement.
            soft_delete_settings: Optional dictionary with settings for soft
                deletes. Expected keys: 'column' (e.g., 'is_active'),
                'inactive_value' (e.g., False), 'active_value' (e.g., True).
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

    @abstractmethod
    def get_latest_high_water_mark(self) -> Optional[datetime.datetime]:
        """
        Retrieves the latest high water mark from the pipeline execution log
        for successful DELTA runs.
        """
        pass

    @abstractmethod
    def log_pipeline_start(self, load_strategy: str, source_file_version: Optional[str] = None) -> int:
        """
        Logs the start of a new pipeline execution and returns the execution ID.
        """
        pass

    @abstractmethod
    def log_pipeline_success(
        self,
        execution_id: int,
        records_processed: int,
        new_high_water_mark: Optional[datetime.datetime] = None,
    ) -> None:
        """Updates the pipeline execution log to mark a run as successful."""
        pass

    @abstractmethod
    def log_pipeline_failure(self, execution_id: int) -> None:
        """Updates the pipeline execution log to mark a run as failed."""
        pass
