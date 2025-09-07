import io
import logging
from typing import Any, Dict, Iterator, Type

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

        # The 'type' key is not a valid psycopg2 parameter
        conn_details.pop("type", None)

        try:
            logger.info(
                (
                    f"Connecting to PostgreSQL database '{self.settings.dbname}' on "
                    f"'{self.settings.host}:{self.settings.port}'."
                )
            )
            self.conn = psycopg2.connect(**conn_details)
            self.conn.autocommit = False  # Ensure transactions are managed manually
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
        data_iterator: Iterator[BaseModel],
        target_table: str,
        pydantic_model: Type[BaseModel],
    ) -> int:
        """
        Execute the native bulk load operation for a batch of data using
        COPY FROM STDIN.
        """
        if not self.conn:
            raise ConnectionError("Database connection is not established.")

        string_buffer = io.StringIO()
        count = 0
        columns = list(pydantic_model.model_fields.keys())

        for record in data_iterator:
            row_values = [self._format_value(getattr(record, col)) for col in columns]
            string_buffer.write("\t".join(row_values) + "\n")
            count += 1

        string_buffer.seek(0)

        with self.conn.cursor() as cursor:
            try:
                copy_sql = (
                    f"COPY {target_table} ({','.join(columns)}) FROM STDIN "
                    "WITH (FORMAT text, NULL '\\N')"
                )
                cursor.copy_expert(copy_sql, string_buffer)
                logger.info(
                    f"Successfully loaded {cursor.rowcount} records into "
                    f"{target_table}."
                )
                return int(cursor.rowcount)
            except psycopg2.Error as e:
                logger.error(f"Bulk load failed: {e}")
                self.rollback()
                raise

    def finalize(
        self,
        load_strategy: str,
        target_table: str,
        staging_table: str | None = None,
        pydantic_model: Type[BaseModel] | None = None,
        primary_key_columns: list[str] | None = None,
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
                cursor.execute(merge_sql)
                logger.info(f"Merged {cursor.rowcount} records into {target_table}.")

                logger.info(f"Dropping staging table {staging_table}.")
                cursor.execute(f"DROP TABLE {staging_table};")

            logger.info("Committing transaction.")
            self.conn.commit()

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
        # Escape characters that have special meaning in text format
        return (
            value_str.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )
