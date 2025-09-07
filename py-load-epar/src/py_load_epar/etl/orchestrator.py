import logging
from typing import Iterator, List, TypeVar

from py_load_epar.config import Settings
from py_load_epar.db.factory import get_db_adapter
from py_load_epar.etl.extract import extract_data
from py_load_epar.etl.transform import transform_and_validate
from py_load_epar.models import EparIndex

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _batch_iterator(iterator: Iterator[T], batch_size: int) -> Iterator[List[T]]:
    """
    Yields batches of a given size from an iterator.
    """
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def run_etl(settings: Settings) -> None:
    """
    Runs the main ETL pipeline.

    1. Gets a database adapter using the factory.
    2. Connects to the database.
    3. Extracts raw data (mocked).
    4. Transforms and validates the data.
    5. Loads the data into the database in batches using the adapter.

    Manages the database transaction, committing on success or rolling back on failure.
    """
    logger.info(f"Starting ETL run with strategy: {settings.etl.load_strategy}")
    adapter = get_db_adapter(settings)

    try:
        adapter.connect(connection_params=None)

        # In a real scenario, you'd fetch the high_water_mark from the db
        high_water_mark = None

        raw_records_iterator = extract_data(settings, high_water_mark)
        validated_models_iterator = transform_and_validate(raw_records_iterator)

        target_model = EparIndex
        target_table = "epar_index"  # This could be derived from the model name

        staging_table = adapter.prepare_load(
            load_strategy=settings.etl.load_strategy, target_table=target_table
        )

        total_loaded_count = 0
        batches = _batch_iterator(validated_models_iterator, settings.etl.batch_size)

        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i+1} with {len(batch)} records.")
            loaded_count = adapter.bulk_load_batch(
                data_iterator=iter(batch),
                target_table=staging_table,
                pydantic_model=target_model,
            )
            total_loaded_count += loaded_count

        adapter.finalize(
            load_strategy=settings.etl.load_strategy,
            target_table=target_table,
            staging_table=staging_table,
            pydantic_model=target_model,
        )

        logger.info(f"ETL run successful. Total records loaded: {total_loaded_count}")

    except Exception as e:
        logger.error(f"ETL run failed: {e}", exc_info=True)
        if adapter:
            adapter.rollback()
        # Re-raise the exception to indicate failure to the caller
        raise
    finally:
        if adapter and getattr(adapter, "close", None):
            adapter.close()
