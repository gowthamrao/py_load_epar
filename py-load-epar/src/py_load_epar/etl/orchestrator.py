import datetime
import logging
import uuid
from pathlib import Path
from typing import Iterator, List, Tuple, TypeVar
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from py_load_epar.config import Settings
from py_load_epar.db.factory import get_db_adapter
from py_load_epar.db.interfaces import IDatabaseAdapter
from py_load_epar.etl.downloader import download_document_and_hash
from py_load_epar.etl.extract import extract_data
from py_load_epar.storage.factory import StorageFactory
from py_load_epar.storage.interfaces import IStorage
from py_load_epar.etl.transform import transform_and_validate
from py_load_epar.models import (
    EparDocument,
    EparIndex,
    EparSubstanceLink,
    Organization,
    Substance,
)
from py_load_epar.spor_api.client import SporApiClient

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


def _process_substance_links(
    adapter: IDatabaseAdapter, substance_links: List[EparSubstanceLink]
) -> int:
    """Bulk loads substance link records into the database."""
    if not substance_links:
        return 0

    logger.info(f"Processing {len(substance_links)} substance link records.")
    target_table = "epar_substance_link"
    model = EparSubstanceLink
    columns = list(model.model_fields.keys())
    data_iterator = (
        tuple(record.model_dump(include=columns).values()) for record in substance_links
    )

    # Use DELTA strategy to avoid inserting duplicate links on reruns
    staging_table = adapter.prepare_load("DELTA", target_table)
    loaded_count = adapter.bulk_load_batch(data_iterator, staging_table, columns)
    adapter.finalize(
        "DELTA",
        target_table,
        staging_table,
        model,
        primary_key_columns=["epar_id", "spor_substance_id"],
    )
    logger.info(f"Successfully loaded {loaded_count} substance links.")
    return loaded_count


def _process_organizations(
    adapter: IDatabaseAdapter, organizations: List[Organization]
) -> int:
    """Bulk loads organization master data into the database."""
    if not organizations:
        return 0

    logger.info(f"Processing {len(organizations)} organization master records.")
    target_table = "organizations"
    model = Organization
    columns = list(model.model_fields.keys())
    # Dedup
    unique_organizations = {org.oms_id: org for org in organizations}.values()
    data_iterator = (
        tuple(record.model_dump(include=columns).values())
        for record in unique_organizations
    )

    staging_table = adapter.prepare_load("DELTA", target_table)
    loaded_count = adapter.bulk_load_batch(data_iterator, staging_table, columns)
    adapter.finalize(
        "DELTA",
        target_table,
        staging_table,
        model,
        primary_key_columns=["oms_id"],
    )
    logger.info(f"Successfully loaded {loaded_count} organization records.")
    return loaded_count


def _process_substances(adapter: IDatabaseAdapter, substances: List[Substance]) -> int:
    """Bulk loads substance master data into the database."""
    if not substances:
        return 0

    logger.info(f"Processing {len(substances)} substance master records.")
    target_table = "substances"
    model = Substance
    columns = list(model.model_fields.keys())
    # Dedup
    unique_substances = {
        sub.spor_substance_id: sub for sub in substances
    }.values()
    data_iterator = (
        tuple(record.model_dump(include=columns).values())
        for record in unique_substances
    )

    staging_table = adapter.prepare_load("DELTA", target_table)
    loaded_count = adapter.bulk_load_batch(data_iterator, staging_table, columns)
    adapter.finalize(
        "DELTA",
        target_table,
        staging_table,
        model,
        primary_key_columns=["spor_substance_id"],
    )
    logger.info(f"Successfully loaded {loaded_count} substance records.")
    return loaded_count


@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _fetch_html_with_retry(url: str) -> bytes:
    """
    Fetches HTML content from a URL with a robust retry mechanism.
    """
    logger.debug(f"Fetching EPAR page: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch HTML from {url}: {e}")
        raise


def _process_documents(
    adapter: IDatabaseAdapter,
    processed_records: List[EparIndex],
    storage: IStorage,
) -> int:
    """
    Downloads, hashes, and loads metadata for associated documents.
    It fetches the EPAR summary page, parses the HTML to find links to
    relevant documents (e.g., Public Assessment Report), and then downloads them.
    """
    logger.info("Starting document processing and HTML parsing.")
    document_records = []
    # Define keywords to identify relevant documents
    DOCUMENT_KEYWORDS = [
        "public assessment report",
        "smpc",
        "product information",
        "package leaflet",
        "epar",
    ]

    for record in processed_records:
        if not record.source_url or not record.source_url.startswith("http"):
            continue

        try:
            # 1. Fetch the HTML of the EPAR summary page with retry
            html_content = _fetch_html_with_retry(record.source_url)

            # 2. Parse the HTML
            soup = BeautifulSoup(html_content, "html.parser")

            # 3. Find and process all relevant document links
            links = soup.find_all("a", href=True)
            found_docs_for_record = False
            for link in links:
                link_text = link.get_text(strip=True).lower()
                href = link["href"]

                # Check if link text contains keywords and points to a PDF
                if any(
                    keyword in link_text for keyword in DOCUMENT_KEYWORDS
                ) and href.lower().endswith(".pdf"):
                    # 4. Construct the full URL for the document
                    doc_url = urljoin(record.source_url, href)

                    logger.info(
                        f"Found document '{link_text}' at {doc_url} for EPAR {record.epar_id}"
                    )

                    # 5. Download the document
                    storage_uri, file_hash = download_document_and_hash(
                        url=doc_url, storage=storage
                    )

                    # 6. Create the EparDocument record
                    doc = EparDocument(
                        document_id=uuid.uuid4(),
                        epar_id=record.epar_id,
                        document_type=link_text,  # Use the link text as the doc type
                        language_code="en",  # Assuming 'en', might need refinement
                        source_url=doc_url,
                        storage_location=storage_uri,
                        file_hash=file_hash,
                        download_timestamp=datetime.datetime.now(
                            datetime.timezone.utc
                        ),
                    )
                    document_records.append(doc)
                    found_docs_for_record = True

            if not found_docs_for_record:
                logger.warning(
                    "Could not find any downloadable PDF documents on page: "
                    f"{record.source_url}"
                )

        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to fetch HTML for EPAR page {record.source_url}: {e}"
            )
            continue
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while processing documents for EPAR {record.epar_id} "
                f"from {record.source_url}: {e}"
            )
            continue

    if not document_records:
        logger.info("No new documents were found or processed.")
        return 0

    # Load the document metadata into the database
    # Use DELTA strategy to handle potential re-downloads gracefully.
    target_table = "epar_documents"
    model = EparDocument
    columns = list(model.model_fields.keys())
    data_iterator = (
        tuple(record.model_dump(include=columns).values())
        for record in document_records
    )

    staging_table = adapter.prepare_load("DELTA", target_table)
    loaded_count = adapter.bulk_load_batch(data_iterator, staging_table, columns)
    adapter.finalize(
        "DELTA",
        target_table,
        staging_table,
        model,
        primary_key_columns=["document_id"],
    )

    logger.info(f"Successfully processed and loaded {loaded_count} documents.")
    return loaded_count


def run_etl(settings: Settings) -> None:
    """
    Runs the main ETL pipeline, including document processing, in a memory-efficient
    batch-oriented way.
    """
    logger.info(f"Starting ETL run with strategy: {settings.etl.load_strategy}")
    adapter = get_db_adapter(settings)
    spor_client = SporApiClient(settings.spor_api)
    storage = StorageFactory(settings.storage).get_storage()
    execution_id = None

    try:
        adapter.connect(connection_params=None)

        # 1. Log pipeline start and get execution ID
        execution_id = adapter.log_pipeline_start(
            load_strategy=settings.etl.load_strategy
        )

        # 2. Prepare main table for loading
        target_model = EparIndex
        target_table = "epar_index"
        main_staging_table = adapter.prepare_load(
            load_strategy=settings.etl.load_strategy, target_table=target_table
        )

        # 3. Set up iterators for streaming data
        high_water_mark = None
        if settings.etl.load_strategy.upper() == "DELTA":
            high_water_mark = adapter.get_latest_high_water_mark()

        raw_records_iterator = extract_data(settings, high_water_mark)
        enriched_models_iterator = transform_and_validate(
            raw_records_iterator, spor_client, execution_id
        )
        batches = _batch_iterator(enriched_models_iterator, settings.etl.batch_size)

        # 4. Process data in batches
        total_loaded_count = 0
        new_high_water_mark = high_water_mark
        all_substance_links: List[EparSubstanceLink] = []

        for i, batch in enumerate(batches):
            if not batch:
                continue

            (
                epar_records,
                substance_links,
                organizations,
                substances,
            ) = zip(*batch)
            logger.info(f"Processing batch {i+1} with {len(epar_records)} records.")

            # Flatten lists from the batch
            flat_organizations = [org for sublist in organizations for org in sublist]
            flat_substances = [sub for sublist in substances for sub in sublist]
            all_substance_links.extend(
                [link for sublist in substance_links for link in sublist]
            )

            # --- Load Master Data and Documents ---
            # This can happen in-flight as these tables don't depend on epar_index.
            _process_organizations(adapter, flat_organizations)
            _process_substances(adapter, flat_substances)
            _process_documents(
                adapter=adapter, processed_records=list(epar_records), storage=storage
            )

            # --- Find the latest date in the current batch to update the HWM ---
            for record in epar_records:
                if new_high_water_mark is None or record.last_update_date_source > (
                    (
                        new_high_water_mark.date()
                        if isinstance(new_high_water_mark, datetime.datetime)
                        else new_high_water_mark
                    )
                ):
                    new_high_water_mark = record.last_update_date_source

            # --- Load batch into the main epar_index staging table ---
            columns = list(target_model.model_fields.keys())
            data_iterator = (
                tuple(record.model_dump(include=columns).values())
                for record in epar_records
            )
            loaded_count = adapter.bulk_load_batch(
                data_iterator=data_iterator,
                target_table=main_staging_table,
                columns=columns,
            )
            total_loaded_count += loaded_count

        # 5. Finalize the main table load
        logger.info("Finalizing load for epar_index table.")
        adapter.finalize(
            load_strategy=settings.etl.load_strategy,
            target_table=target_table,
            staging_table=main_staging_table,
            pydantic_model=target_model,
            primary_key_columns=["epar_id"],
        )

        # 6. Load substance links now that epar_index is populated
        logger.info(f"Processing {len(all_substance_links)} total substance links.")
        _process_substance_links(adapter, all_substance_links)

        # 6. Log pipeline success
        adapter.log_pipeline_success(
            execution_id=execution_id,
            records_processed=total_loaded_count,
            new_high_water_mark=new_high_water_mark,
        )
        logger.info(
            f"ETL run successful. Execution ID: {execution_id}. "
            f"Total records loaded: {total_loaded_count}"
        )

    except Exception as e:
        logger.error(f"ETL run failed: {e}", exc_info=True)
        if adapter and execution_id is not None:
            adapter.log_pipeline_failure(execution_id)
        # We still want to rollback the data transaction
        if adapter:
            adapter.rollback()
        raise
    finally:
        if adapter and getattr(adapter, "close", None):
            adapter.close()
