import datetime
import io
import logging
from typing import Any, Dict, Iterator, Optional, Type

import psycopg2
from psycopg2.extensions import connection as PgConnection
from pydantic import BaseModel

from py_load_epar.config import DatabaseSettings
from py_load_epar.db.interfaces import IDatabaseAdapter

logger = logging.getLogger(__name__)


class PostgresAdapter(IDatabaseAdapter):
    """
    PostgreSQL specific implementation of the database adapter (Adapter).
    Uses the native COPY command for high-performance bulk loading.
    """

    def __init__(self, settings: DatabaseSettings):
        self.settings = settings
        self.conn: PgConnection | None = None

    def connect(self, connection_params: Dict[str, Any] | None = None) -> None:
        """Establish connection to the PostgreSQL database."""
        if self.conn and self.conn.closed == 0:
            logger.debug("Connection already established.")
            return

        conn_details = self.settings.model_dump()
        if connection_params:
            conn_details.update(connection_params)

        conn_details.pop("type", None)

        try:
            logger.info(
                (
                    f"Connecting to PostgreSQL database '{self.settings.dbname}' on "
                    f"'{self.settings.host}:{self.settings.port}'."
                )
            )
            self.conn = psycopg2.connect(**conn_details)
            self.conn.autocommit = False
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def prepare_load(self, load_strategy: str, target_table: str) -> str:
        """
        Prepare the database for loading. For 'FULL', truncates the table.
        For 'DELTA', creates an unlogged staging table.
        """
        if not self.conn:
            raise ConnectionError("Database connection is not established.")

        with self.conn.cursor() as cursor:
            if load_strategy.upper() == "FULL":
                logger.info(f"FULL load strategy: Truncating table {target_table}.")
                cursor.execute(
                    f"TRUNCATE TABLE {target_table} RESTART IDENTITY CASCADE;"
                )
                return target_table
            elif load_strategy.upper() == "DELTA":
                staging_table = f"staging_{target_table}"
                logger.info(
                    "DELTA load strategy: Creating UNLOGGED staging table "
                    f"{staging_table}."
                )
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
                cursor.execute(
                    (
                        f"CREATE UNLOGGED TABLE {staging_table} "
                        f"(LIKE {target_table} INCLUDING DEFAULTS);"
                    )
                )
                return staging_table
            else:
                raise ValueError(f"Unknown load strategy: {load_strategy}")

    def bulk_load_batch(
        self,
        data_iterator: Iterator[tuple],
        target_table: str,
        columns: list[str],
    ) -> int:
        """
        Execute the native bulk load operation for a batch of data using
        COPY FROM STDIN in a streaming fashion.
        """
        if not self.conn:
            raise ConnectionError("Database connection is not established.")

        streaming_iterator = StreamingIteratorIO(
            iterator=(
                ("\t".join(map(self._format_value, row)) + "\n").encode("utf-8")
                for row in data_iterator
            )
        )

        with self.conn.cursor() as cursor:
            try:
                copy_sql = (
                    f"COPY {target_table} ({','.join(columns)}) FROM STDIN "
                    "WITH (FORMAT text, NULL '\\N')"
                )
                cursor.copy_expert(copy_sql, streaming_iterator)
                logger.info(
                    f"Successfully loaded {cursor.rowcount} records into "
                    f"{target_table}."
                )
                return cursor.rowcount if cursor.rowcount != -1 else 0
            except psycopg2.Error as e:
                logger.error(f"Bulk load failed: {e}")
                self.rollback()
                raise

    def finalize(
        self,
        load_strategy: str,
        target_table: str,
        staging_table: Optional[str] = None,
        pydantic_model: Optional[Type[BaseModel]] = None,
        primary_key_columns: Optional[list[str]] = None,
        soft_delete_settings: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Finalize the load process. For 'DELTA', merges from staging to target.
        Commits the transaction.
        """
        if not self.conn:
            raise ConnectionError("Database connection is not established.")

        with self.conn.cursor() as cursor:
            if load_strategy.upper() == "DELTA":
                if not all([staging_table, pydantic_model, primary_key_columns]):
                    raise ValueError(
                        "For 'DELTA' strategy, 'staging_table', 'pydantic_model', "
                        "and 'primary_key_columns' must be provided."
                    )
                logger.info(f"Merging data from {staging_table} to {target_table}.")

                columns = list(pydantic_model.model_fields.keys())
                pk_cols_str = ", ".join(primary_key_columns)

                update_cols = [
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col not in primary_key_columns
                ]

                merge_sql = f"""
                INSERT INTO {target_table} ({', '.join(columns)})
                SELECT {', '.join(columns)} FROM {staging_table}
                ON CONFLICT ({pk_cols_str}) DO UPDATE SET
                    {', '.join(update_cols)};
                """
                if not update_cols:
                    merge_sql = f"""
                    INSERT INTO {target_table} ({', '.join(columns)})
                    SELECT {', '.join(columns)} FROM {staging_table}
                    ON CONFLICT ({pk_cols_str}) DO NOTHING;
                    """

                cursor.execute(merge_sql)
                logger.info(f"Merged {cursor.rowcount} records into {target_table}.")

                if soft_delete_settings:
                    self._perform_soft_delete(
                        cursor,
                        target_table,
                        staging_table,
                        primary_key_columns,
                        soft_delete_settings,
                    )

                logger.info(f"Dropping staging table {staging_table}.")
                cursor.execute(f"DROP TABLE {staging_table};")

            logger.info("Committing transaction.")
            self.conn.commit()

    def _perform_soft_delete(
        self,
        cursor: Any,
        target_table: str,
        staging_table: str,
        primary_key_columns: list[str],
        settings: Dict[str, Any],
    ) -> None:
        """Helper method to perform a soft-delete operation."""
        delete_col = settings.get("column")
        inactive_val = settings.get("inactive_value")
        active_val = settings.get("active_value")

        if not all([delete_col, inactive_val is not None, active_val is not None]):
            logger.warning("Soft delete settings are incomplete. Skipping.")
            return

        logger.info(f"Performing soft-delete on {target_table} for withdrawn records.")
        pk_match_clause = " AND ".join(
            [f"t.{pk} = s.{pk}" for pk in primary_key_columns]
        )

        soft_delete_sql = f"""
            UPDATE {target_table} AS t
            SET {delete_col} = %s
            WHERE
                {delete_col} = %s
                AND NOT EXISTS (
                    SELECT 1 FROM {staging_table} AS s WHERE {pk_match_clause}
                );
        """
        cursor.execute(soft_delete_sql, (inactive_val, active_val))
        logger.info(f"Soft-deleted {cursor.rowcount} records from {target_table}.")

    def rollback(self) -> None:
        """Roll back the transaction in case of failure."""
        if self.conn:
            logger.warning("Rolling back transaction.")
            self.conn.rollback()

    def close(self) -> None:
        """Closes the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Database connection closed.")

    def _format_value(self, value: Any) -> str:
        """Formats Python values for text-based COPY."""
        if value is None:
            return "\\N"
        value_str = str(value)
        return (
            value_str.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )

    def get_latest_high_water_mark(self) -> Optional[datetime.datetime]:
        """
        Retrieves the latest high water mark from the pipeline execution log
        for successful DELTA runs.
        """
        if not self.conn:
            raise ConnectionError("Database connection is not established.")

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(high_water_mark)
                FROM pipeline_execution
                WHERE status = 'SUCCESS' AND load_strategy = 'DELTA'
                """
            )
            result = cursor.fetchone()[0]
            if result:
                logger.info(f"Found latest high water mark: {result}")
                return result
            logger.info("No previous high water mark found.")
            return None

    def log_pipeline_start(
        self, load_strategy: str, source_file_version: Optional[str] = None
    ) -> int:
        """
        Logs the start of a new pipeline execution and returns the execution ID.
        """
        if not self.conn:
            raise ConnectionError("Database connection is not established.")

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_execution
                    (start_timestamp_utc, status, load_strategy, source_file_version)
                VALUES
                    (%s, %s, %s, %s)
                RETURNING execution_id
                """,
                (
                    datetime.datetime.now(datetime.timezone.utc),
                    "RUNNING",
                    load_strategy,
                    source_file_version,
                ),
            )
            execution_id = cursor.fetchone()[0]
            self.conn.commit()
            logger.info(
                f"Logged pipeline start for execution_id {execution_id} with strategy {load_strategy}."
            )
            return execution_id

    def log_pipeline_success(
        self,
        execution_id: int,
        records_processed: int,
        new_high_water_mark: Optional[datetime.datetime] = None,
    ) -> None:
        """Updates the pipeline execution log to mark a run as successful."""
        if not self.conn:
            raise ConnectionError("Database connection is not established.")

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_execution
                SET
                    end_timestamp_utc = %s,
                    status = 'SUCCESS',
                    records_processed = %s,
                    high_water_mark = %s
                WHERE execution_id = %s
                """,
                (
                    datetime.datetime.now(datetime.timezone.utc),
                    records_processed,
                    new_high_water_mark,
                    execution_id,
                ),
            )
            self.conn.commit()
            logger.info(f"Successfully logged success for execution_id {execution_id}.")

    def log_pipeline_failure(self, execution_id: int) -> None:
        """Updates the pipeline execution log to mark a run as failed."""
        if not self.conn:
            raise ConnectionError("Database connection is not established.")

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_execution
                SET
                    end_timestamp_utc = %s,
                    status = 'FAILED'
                WHERE execution_id = %s
                """,
                (datetime.datetime.now(datetime.timezone.utc), execution_id),
            )
            self.conn.commit()
            logger.error(f"Successfully logged failure for execution_id {execution_id}.")


class StreamingIteratorIO(io.IOBase):
    """
    A file-like object that wraps an iterator of bytes.
    `psycopg2.copy_expert` can read from this object, allowing for true
    streaming of data from a Python iterator to the database without
    buffering the entire dataset in memory.
    """
    def __init__(self, iterator: Iterator[bytes]):
        self._iterator = iterator
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        """Read bytes from the iterator."""
        if size == -1:
            self._buffer += b"".join(self._iterator)
            data = self._buffer
            self._buffer = b""
            return data

        while len(self._buffer) < size:
            try:
                self._buffer += next(self._iterator)
            except StopIteration:
                break

        data = self._buffer[:size]
        self._buffer = self._buffer[size:]
        return data
