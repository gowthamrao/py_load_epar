import logging
from typing import Any, Dict, Iterator

from pydantic import ValidationError

from py_load_epar.models import EparIndex

logger = logging.getLogger(__name__)


def transform_and_validate(
    raw_records: Iterator[Dict[str, Any]]
) -> Iterator[EparIndex]:
    """
    Transforms raw data dictionaries into Pydantic models and validates them.

    Records that fail validation are logged and skipped (quarantined).

    Args:
        raw_records: An iterator yielding dictionaries of raw EPAR data.

    Yields:
        Validated EparIndex Pydantic model instances.
    """
    logger.info("Starting data transformation and validation.")
    validated_count = 0
    failed_count = 0

    for i, raw_record in enumerate(raw_records):
        try:
            # Pydantic will automatically match dict keys to model fields
            # and ignore extra fields not defined in the model.
            validated_model = EparIndex.model_validate(raw_record)
            yield validated_model
            validated_count += 1
        except ValidationError as e:
            logger.warning(
                f"Record {i+1} failed validation and will be quarantined. "
                f"Record ID: {raw_record.get('epar_id', 'N/A')}. Error: {e}"
            )
            failed_count += 1
            # In a real system, this failed record would be sent to a
            # dead-letter queue or error table.
            continue

    logger.info(
        f"Finished transformation. "
        f"Successfully validated: {validated_count}. Failed: {failed_count}."
    )
