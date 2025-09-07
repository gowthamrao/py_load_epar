import datetime
import logging
from typing import Iterator, Dict, Any

from py_load_epar.config import Settings

logger = logging.getLogger(__name__)


def extract_data(settings: Settings, high_water_mark: datetime.date | None = None) -> Iterator[Dict[str, Any]]:
    """
    Extracts EPAR data from the source.

    This is currently a mock implementation that yields fake data.
    In a real implementation, this function would:
    1. Download the EMA Excel/CSV file.
    2. Connect to the SPOR API.
    3. Use the high_water_mark to filter for new/updated records (CDC).

    Args:
        settings: The application settings.
        high_water_mark: The last update date from the previous successful run.

    Yields:
        A dictionary representing a single raw EPAR record.
    """
    logger.info("Starting data extraction (mock implementation).")

    # Mock data representing rows from the EMA source file
    mock_epar_records = [
        {
            "epar_id": "EMA/123456",
            "medicine_name": "Testmed",
            "authorization_status": "Authorised",
            "first_authorization_date": "2022-01-15",
            "last_update_date_source": "2023-05-20",
            "active_substance_raw": "Testsubstance A",
            "marketing_authorization_holder_raw": "Pharma Corp",
            "therapeutic_area": "Testing",
            "source_url": "http://ema.europa.eu/ema/123456"
        },
        {
            "epar_id": "EMA/789012",
            "medicine_name": "Anothertest",
            "authorization_status": "Authorised",
            "first_authorization_date": "2021-11-10",
            "last_update_date_source": "2023-06-01",
            "active_substance_raw": "Testsubstance B",
            "marketing_authorization_holder_raw": "Bio Inc",
            "therapeutic_area": "Testing",
            "source_url": "http://ema.europa.eu/ema/789012"
        },
        {
            "epar_id": "EMA/345678",
            "medicine_name": "Failmed",
            "authorization_status": "Authorised",
            # Missing first_authorization_date to test validation
            "last_update_date_source": "2023-06-02",
            "active_substance_raw": "Failing substance",
            "marketing_authorization_holder_raw": "Bad Data Ltd",
            "therapeutic_area": "Error Handling",
            "source_url": "http://ema.europa.eu/ema/345678",
            "some_unexpected_column": "should be ignored"
        },
    ]

    for record in mock_epar_records:
        # Simulate CDC by checking the high_water_mark
        record_date = datetime.date.fromisoformat(record["last_update_date_source"])
        if high_water_mark is None or record_date > high_water_mark:
            logger.debug(f"Extracting record: {record['epar_id']}")
            yield record

    logger.info("Finished data extraction.")
