import datetime
import logging
import uuid
from pathlib import Path
from typing import Iterator, List, TypeVar

from py_load_epar.config import Settings
from py_load_epar.db.factory import get_db_adapter
from py_load_epar.db.interfaces import IDatabaseAdapter
from py_load_epar.etl.downloader import download_document_and_hash
from py_load_epar.etl.extract import extract_data
from py_load_epar.etl.transform import transform_and_validate
from py_load_epar.models import EparDocument, EparIndex

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _batch_iterator(iterator: Iterator[T], batch_size: int) -> Iterator[List[T]]:
    """Yields batches of a given size from an iterator."""
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _process_documents(
    adapter: IDatabaseAdapter,
    processed_records: List[EparIndex],
    document_storage_path: Path,
) -> int:
    """Download, hash, and load metadata for associated documents."""
    logger.info("Starting document processing.")
    document_records = []
    for record in processed_records:
        if not record.source_url or not record.source_url.startswith("http"):
            continue

        try:
            storage_path, file_hash = download_document_and_hash(
                record.source_url, document_storage_path
            )
            doc = EparDocument(
                document_id=uuid.uuid4(),
                epar_id=record.epar_id,
                document_type="EPAR",  # Assuming a default type
                language_code="en",  # Assuming a default
                source_url=record.source_url,
                storage_location=str(storage_path),
                file_hash=file_hash,
                download_timestamp=datetime.datetime.now(datetime.timezone.utc),
            )
            document_records.append(doc)
        except Exception as e:
            logger.error(
                f"Failed to download or process document for {record.epar_id} "
                f"from {record.source_url}: {e}"
            )
            continue

    if not document_records:
        logger.info("No documents to process.")
        return 0

    # Load the document metadata into the database
    # For documents, we'll use a FULL load strategy into a staging table
    # and then merge, to handle potential re-downloads gracefully.
    target_table = "epar_documents"
    staging_table = adapter.prepare_load("DELTA", target_table)
    loaded_count = adapter.bulk_load_batch(
        iter(document_records), staging_table, EparDocument
    )
    adapter.finalize("DELTA", target_table, staging_table, EparDocument)

    logger.info(f"Successfully processed and loaded {loaded_count} documents.")
    return loaded_count


def run_etl(settings: Settings) -> None:
    """Runs the main ETL pipeline, including document processing."""
    logger.info(f"Starting ETL run with strategy: {settings.etl.load_strategy}")
    adapter = get_db_adapter(settings)
    all_validated_records = []

    try:
        adapter.connect(connection_params=None)

        high_water_mark = None  # TODO: Fetch from database
        raw_records_iterator = extract_data(settings, high_water_mark)
        validated_models_iterator = transform_and_validate(raw_records_iterator)

        target_model = EparIndex
        target_table = "epar_index"
        staging_table = adapter.prepare_load(
            load_strategy=settings.etl.load_strategy, target_table=target_table
        )

        total_loaded_count = 0
        batches = _batch_iterator(validated_models_iterator, settings.etl.batch_size)

        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i+1} with {len(batch)} records.")
            # Keep track of all validated records for document processing later
            all_validated_records.extend(batch)
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
        logger.info(
            f"ETL run for epar_index successful. Total records loaded: {total_loaded_count}"
        )

        # After the main ETL is complete, process the documents
        if all_validated_records:
            doc_path = Path(settings.etl.document_storage_path)
            _process_documents(adapter, all_validated_records, doc_path)

    except Exception as e:
        logger.error(f"ETL run failed: {e}", exc_info=True)
        if adapter:
            adapter.rollback()
        raise
    finally:
        if adapter and getattr(adapter, "close", None):
            adapter.close()
